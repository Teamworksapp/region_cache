# -*- coding: utf-8 -*-

from urllib.parse import urlparse

import redis
import pickle

from .region import Region

class RegionCache(object):
    """
    This is the flask extension itself. Initialize this when you initialize all of your other extensions.
    """
    def __init__(self, root='root', serializer=pickle):
        self._serializer = serializer
        self._regions = {}
        self._conn = None
        self._root_name = root

    def init_app(self, app):
        """
        Configure this object as a flask or celery extension through the flask config.

        :param app:
        :return:
        """
        redis_url_parsed = urlparse(app.config['CACHE_REDIS_URL'])

        self._conn = redis.StrictRedis(
            host=redis_url_parsed.hostname,
            port=redis_url_parsed.port or 6379,
            db=int(redis_url_parsed.path[1:]),
            password=redis_url_parsed.password,
        )
        self._root = self.region()


    def region(self, name=None, timeout=None, update_resets_timeout=True, serializer=None):
        """
        Return a (possibly existing) cache region
        :param name:
        :param timeout:
        :param update_resets_timeout:
        :param serializer:
        :return:
        """
        if name is None:
            name = self._root_name

        if name in self._regions:
            return self._regions[name]

        names = name.split('.') if '.' in name else [name]
        names.reverse()
        parent = None
        if name != self._root_name and not name.startswith('root.'):
            names.append(self._root_name)
        parts = []
        fqname = ''
        while names:
            parts.append(names.pop())
            fqname = '.'.join(parts)
            if fqname not in self._regions:
                self._regions[fqname] = Region(
                    self, fqname,
                    timeout=timeout,
                    update_resets_timeout=update_resets_timeout,
                    serializer=serializer or self._serializer
                )
            parent = self._regions[fqname]

        return self._regions[fqname]

    def clear(self):
        self.region().invalidate()  # invalidate the root cache region will cascade down.


