import pickle
import redis
from collections import MutableMapping
from datetime import datetime
from functools import wraps

import blinker
import logging

from logging import getLogger

_logger = getLogger('region_cache')

class Region(MutableMapping):
    """
    A bound cache region. Do not instantiate these directly. Instead use the RegionCache.region() function.

    This will make for proper nesting of cache structures.
    """
    def __init__(self, region_cache, name, timeout=None, update_resets_timeout=True, serializer=pickle):
        self._region_cache = region_cache
        self.name = name
        self._region_cache.conn.hset(name, '__cache_region_created_at__', datetime.utcnow().isoformat())
        self._timeout = None
        self._region_cache = region_cache
        self._serializer = serializer
        self._pipe = None
        self._children_key = self.name + "::child_caches"
        self._update_resets_timeout = update_resets_timeout

        if timeout:
            self._timeout = timeout
            self._region_cache.conn.expire(name, timeout)

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
        _logger.debug("Invalidating region %s", self.name)

        if pipeline is None:
            pipeline = self._region_cache.conn.pipeline()
            is_root_call = True
        else:
            is_root_call = False

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
            try:
                self.invalidate()
            except redis.TimeoutError:
                logging.getLogger('region_cache').exception(
                    "Invalidation of {self.name} in signal handler timed out. Flush it manually".format(self=self))

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

    def get_or_compute(self, item, alt):
        """
        Get the value, or if the value is not in the cache, compute it from `alt`. Alt can be a callable or a scalar.

        :param item: The key to get
        :param alt: Callable or scalar. The value to return. Will be stored in the cache on computation.
        :return: The value in the cache.
        """
        try:
            return self[item]
        except KeyError:
            value = alt() if callable(alt) else alt
            self[item] = value
            return value
        except redis.TimeoutError:
            _logger.warning("Cannot reach cache. Using alternative")
            if callable(alt):
                return alt()
            else:
                return alt

    def __getitem__(self, item):
        timed_out = False

        if self._region_cache.is_disconnected():
            raise KeyError(item)

        # pylint: disable=W0212
        if self._region_cache._raise_on_timeout:  # raise the redis timeout error instead of a key error
            raw_value = self._region_cache.read_conn.hget(self.name, item)
        else:
            try:
                raw_value = self._region_cache.read_conn.hget(self.name, item)
            except redis.TimeoutError:
                raw_value = None
                timed_out = True

        if timed_out:
            # pylint: disable=W0212
            if self._region_cache._reconnect_on_timeout:
                self._region_cache.invalidate_connections()
            raise KeyError(item)

        if raw_value is not None:
            return self._serializer.loads(raw_value)
        else:
            raise KeyError(item)

    def __setitem__(self, key, value):
        raw_value = self._serializer.dumps(value)

        if not self._region_cache.is_disconnected():
            should_reset_timeout = (not self._pipe and
                                    self._timeout and
                                    (self._update_resets_timeout or not len(self)))


            if self._pipe:
                self._pipe.hset(self.name, key, raw_value)
            else:
                self._region_cache.conn.hset(self.name, key, raw_value)

            if should_reset_timeout:
                self._region_cache.conn.expire(self.name, self._timeout)

    def __delitem__(self, key):
        if not self._region_cache.is_disconnected():
            should_reset_timeout = (not self._pipe and
                                    self._timeout and
                                    (self._update_resets_timeout or (len(self) == 1 and key in self)))

            if self._pipe:
                self._pipe.hdel(self.name, key)
            else:
                self._region_cache.conn.hdel(self.name, key)

            if should_reset_timeout:
                self._region_cache.conn.expire(self.name, self._timeout)

        else:
            raise redis.TimeoutError(f"Cannot delete item {key} from {self.name} because we are disconnected.")

    def __iter__(self):
        for k in self._region_cache.read_conn.hkeys(self.name):
            if not k.decode('utf-8').startswith('__'):
                yield k

    def __len__(self):
        return self._region_cache.conn.hlen(self.name)

    def __enter__(self):
        if not self._pipe:
            self._pipe = self._region_cache.conn.pipeline()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:

            # if we started with nothing in the cache, reset it
            should_reset_timeout = (self._timeout and (self._update_resets_timeout or len(self) == 0))

            self._pipe.execute()

            if should_reset_timeout:
                self._region_cache.conn.expire(self.name, self._timeout)
            retval = True
        else:
            self._pipe.reset()
            retval = False

        self._pipe = None
        return retval

    def __eq__(self, other):
        return other.name == self.name

    def children(self):
        return (self._region_cache.region(name.decode('utf-8'))
                for name in self._region_cache.read_conn.smembers(self._children_key))

    def add_child(self, child):
        self._region_cache.conn.sadd(self._children_key, child.name)

    def reset_timeout(self):
        self._region_cache.conn.expire(self.name, self._timeout)
