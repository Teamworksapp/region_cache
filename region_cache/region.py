import pickle
from collections import MutableMapping
from datetime import datetime
from functools import wraps

import blinker


class Region(MutableMapping):
    """
    A bound cache region. Do not instantiate these directly. Instead use the RegionCache.region() function.

    This will make for proper nesting of cache structures.
    """
    def __init__(self, region_cache, name, timeout=None, update_resets_timeout=True, serializer=pickle):
        self._conn = region_cache._conn
        self.name = name
        self._conn.hset(name, '__cache_region_created_at__', datetime.utcnow().isoformat())
        self._timeout = None
        self._region_cache = region_cache
        self._serializer = serializer
        self._pipe = None
        self._children_key = self.name + "::child_caches"
        self._local_storage = {}
        self._update_resets_timeout = update_resets_timeout

        if timeout:
            self._timeout = timeout
            self._conn.expire(name, timeout)

        if '.' in name:
            parent = name.rsplit('.', 1)[0]
            parent = self._region_cache.region(parent)
            parent.add_child(self)

    def __repr__(self):
        return "Region({})".format(self.name)

    def region(self, name=None, timeout=None, update_resets_timeout=None, serializer=None):
        """
        Get a sub-region from this region. When this region is invalidated, the subregion will be too.

        :param name: The name of the subregion. If dots are included, then the dotted regions are treated as subregions
            of this subregion.
        :param timeout: The timeout in seconds for this region. Defaults to the parent region's timeout
        :param update_resets_timeout: Whether updating the region resets the timeout. Defaults to the parent region's
            setting.
        :param serializer: The serializer to use. Must define loads and dumps(). Defaults to the parent region's setting
        :return: Region
        """
        return self._region_cache.region(
            name=self.name + '.' + name,
            timeout=timeout or self._timeout,
            update_resets_timeout=(
                update_resets_timeout if self._update_resets_timeout is not None else self._update_resets_timeout),
            serializer=serializer or self._serializer
        )

    def invalidate(self, pipeline=None):
        """
        Delete this region's cache data and all its subregions.

        :param pipeline: Used internally.

        :return: None
        """
        if pipeline is None:
            pipeline = self._conn.pipeline()
            is_root_call = True
        else:
            is_root_call = False

        self._local_storage = {}

        for child in self.children():
            child.invalidate(pipeline)

        pipeline.delete(self.name)

        if is_root_call:
            pipeline.execute()

    def invalidate_on(self, *signals):
        """
        Bind this cache region to blinker signals. When any of the signals have been triggered, invalidate the cache.

        :param signals: blinker signal objects or string names for named signals.
        :return: None
        """

        def handler(sender, **kwargs):
            self.invalidate()

        for sig in signals:
            if isinstance(sig, str):
                sig = blinker.signal(sig)
            sig.connect(handler, weak=False)

    def cached(self, f):
        """
        Decorator that uses a serialized form of the input args as a key and caches the result of calling the method.
        Subsequent calls to the method with the same arguments will return the cached result.
        """
        @wraps(f)
        def wrapper(*args, **kwargs):
            key = self._serializer.dumps((args, kwargs))
            try:
                ret = self[key]
            except KeyError:
                ret = f(*args, **kwargs)
                self[key] = ret

            return ret

        return wrapper

    def __getitem__(self, item):
        raw_item = self._conn.hget(self.name, item)

        if raw_item is not None:
            if raw_item not in self._local_storage:
                self._local_storage[item] = self._serializer.loads(raw_item)
        else:
            raise KeyError(raw_item)

        return self._local_storage[raw_item]

    def __setitem__(self, key, value):
        raw_value = self._serializer.dumps(value)

        if self._update_resets_timeout and self._timeout:
            if self._pipe:
                self._pipe.hset(self.name, key, raw_value)
                self._pipe.expire(self.name, self._timeout)
            else:
                with self._conn.pipeline() as pipe:
                    pipe.hset(self.name, key, raw_value)
                    pipe.expire(self.name, self._timeout)
        else:
            if self._pipe:
                self._pipe.hset(self.name, key, raw_value)
            else:
                self._conn.hset(self.name, key, self._serializer.dumps(value))

        self._local_storage[raw_value] = value

    def __delitem__(self, key):
        raw_item = self._conn.hget(self.name, key)
        if self._update_resets_timeout and self._timeout:
            if self._pipe:
                self._pipe.hdel(self.name, key)
                self._pipe.expire(self.name, self._timeout)
            else:
                with self._conn.pipeline() as pipe:
                    pipe.hdel(self.name, key)
                    pipe.expire(self.name, self._timeout)
        else:
            if self._pipe:
                self._pipe.hdel(self.name, key)
            else:
                self._conn.hdel(self.name, key)

        del self._local_storage[raw_item]

    def __iter__(self):
        for k, v in self._conn.hgetall(self.name):
            if not k.startswith('__'):
                yield k, self._serializer.loads(v)

    def __len__(self):
        return self._conn.hlen(self.name)

    def __enter__(self):
        if not self._pipe:
            self._pipe = self._conn.pipeline()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self._pipe.execute()
            retval = True
        else:
            self._pipe.reset()
            retval = False

        self._pipe = None
        return retval

    def __eq__(self, other):
        return other.name == self.name

    def children(self):
        return (self._region_cache.region(name.decode('utf-8')) for name in self._conn.smembers(self._children_key))

    def add_child(self, child):
        self._conn.sadd(self._children_key, child.name)

    def reset_timeout(self):
        self._conn.expire(self.name, self._timeout)
