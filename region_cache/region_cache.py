# -*- coding: utf-8 -*-
import datetime
from urllib.parse import urlparse

import redis
import pickle

from .region import Region
from logging import getLogger

_logger = getLogger('region_cache')


class RegionCache(object):
    """
    This is the flask extension itself. Initialize this when you initialize all of your other extensions.
    """
    def __init__(
        self,
        root='root',
        serializer=pickle,
        host='localhost',
        port=6379,
        db=0,
        password=None,
        op_timeout=None,
        reconnect_on_timeout=True,
        timeout_backoff=None,
        raise_on_timeout=False,
        rr_host=None,
        rr_port=0,
        rr_password=None,
        *args,
        **kwargs
    ):
        """
        Construct a new RegionCache object.

        Pass in params, or if you are using a with Flask or Celery, you can control with config vars.

        :param root (optional str): Default 'root' The key to use for the base region.
        :param serializer (optional pickle-like object): Default = pickle. Flask/Celery config is
            REGION_CACHE_SERIALIZER.
        :param host (optional str): Default localhost The hostname of the redis master instance. Flask/Celery config is
            REGION_CACHE_HOST.
        :param port (int): Default 6379. The port of the redis master instance. Flask/Celery config is
            REGION_CACHE_PORT.
        :param db (int): Default 0. The db number to use on the redis master instance. Flask/Celery config is
            REGION_CACHE_DB.
        :param password (optional int): The password to use for the redis master instance. Flask/Celery config is
            REGION_CACHE_PASSWORD.
        :param op_timeout (optional number): Default = no timeout. A timeout in seconds after which an operation will
            fail. Flask/Celery config is REGION_CACHE_OP_TIMEOUT.
        :param reconnect_on_timeout (optional bool): Default = False. Whether to close the connection and reconnect on
            timeout. Flask/Celery config is REGION_CACHE_OP_TIMEOUT_RECONNECT.
        :param reconnect_backoff (optional int): Seconds that we should wait before trying to reconnect to the cache.
        :param raise_on_timeout (optional bool): Default = False. If false, we catch the exception and return None for
            readonly operations.Otherwise raise redis.TimeoutError. Flask/Celery config is
            REGION_CACHE_OP_TIMEOUT_RAISE.
        :param rr_host (optional str): Default None. The host for a redis read-replica, if it exists. Flask/Celery
            config is REGION_CACHE_RR_HOST.
        :param rr_port (int): The port for a redis read-replica. Flask/Celery config is REGION_CACHE_RR_PORT.
        :param rr_password (str): The password for the redis read replica. Flask/Celery config is
            REGION_CACHE_RR_PASSWORD.
        :param args: Arguments to pass to StrictRedis. Flask/Celery config is REGION_CACHE_REDIS_ARGS.
        :param kwargs: Extra options to pass to StrictRedis. Flask/Celery config is REGION_CACHE_REDIS_OPTIONS.
        """
        self._serializer = serializer
        self._regions = {}
        self._w_conn = None
        self._r_conn = None
        self._root_name = root
        self._op_timeout = op_timeout
        self._reconnect_on_timeout = reconnect_on_timeout
        self._raise_on_timeout = raise_on_timeout

        self._reconnect_backoff = timeout_backoff
        self._last_timeout = None
        self._reconnect_after = None

        self._host = host
        self._port = port
        self._db = db
        self._password = password

        self._rr_host = rr_host
        self._rr_port = rr_port or 6379
        self._rr_password = rr_password

        self._args = args
        self._kwargs = kwargs

        self._root = None

    def init_app(self, app):
        """
        Configure this object as a flask or celery extension through the flask config.

        :param app: A flask or celery app.
        :return: None
        """
        if app.config.get('REGION_CACHE_OP_TIMEOUT', None):
            self._reconnect_on_timeout = app.config.get('REGION_CACHE_OP_TIMEOUT_RECONNECT', self._reconnect_on_timeout)
            self._reconnect_backoff = app.config.get('REGION_CACHE_RECONNECT_BACKOFF', self._reconnect_backoff)
            self._op_timeout = app.config.get('REGION_CACHE_OP_TIMEOUT', self._op_timeout)
            self._raise_on_timeout = app.config.get('REGION_CACHE_OP_TIMEOUT_RAISE', self._raise_on_timeout)

            if self._reconnect_backoff:
                self._reconnect_backoff = float(self._reconnect_backoff)
            if self._op_timeout:
                self._op_timeout = float(self._op_timeout)

        if 'REGION_CACHE_URL' in app.config:
            redis_url_parsed = urlparse(app.config['REGION_CACHE_URL'])

            self._host = redis_url_parsed.hostname
            self._port = redis_url_parsed.port or 6379
            self._db = int(redis_url_parsed.path[1:])
            self._password = redis_url_parsed.password
        else:
            self._host = app.config.get('REGION_CACHE_HOST', 'localhost')
            self._port = app.config.get('REGION_CACHE_PORT', 6379)
            self._password = app.config.get('REGION_CACHE_PASSWORD', None)

        # if there's a read replica to connect to.
        if 'REGION_CACHE_RR_URL' in app.config:
            redis_url_parsed = urlparse(app.config['REGION_CACHE_RR_URL'])

            self._rr_host = redis_url_parsed.hostname
            self._rr_port = redis_url_parsed.port or 6379
            self._rr_password = redis_url_parsed.password
        else:
            self._rr_host = app.config.get('REGION_CACHE_RR_HOST', None)
            self._rr_port = app.config.get('REGION_CACHE_RR_PORT', None)
            self._rr_password = app.config.get('REGION_CACHE_RR_PASSWORD', None)

        self._args += tuple(app.config.get('REGION_CACHE_REDIS_ARGS', ()))
        self._kwargs.update(app.config.get('REGION_CACHE_REDIS_OPTIONS', {}))

        self._root = self.region()

    def invalidate_connections(self):
        _logger.debug("Invalidating connections")

        if self._r_conn and self._r_conn is not self._w_conn:
            self._r_conn.connection_pool.disconnect()
        if self._w_conn:
            self._w_conn.connection_pool.disconnect()

        self._r_conn = None
        self._w_conn = None
        self._last_timeout = datetime.datetime.utcnow()
        if self._reconnect_backoff:
            self._reconnect_after = self._last_timeout + datetime.timedelta(self._reconnect_backoff)

    def is_disconnected(self):
        if not (self._w_conn and self._r_conn) and self._reconnect_after:
            if datetime.datetime.utcnow() < self._reconnect_after:
                return True

        return False

    @property
    def conn(self):
        """
        The master connection to redis.
        """
        if not self._w_conn:
            _logger.debug("Attempting connection to redis on %s", self._host)

            self._reconnect_after = None
            kwargs = dict(**self._kwargs)
            if self._op_timeout:
                kwargs['socket_timeout'] = self._op_timeout

            try:
                self._w_conn = redis.StrictRedis(
                    host=self._host,
                    port=self._port,
                    db=self._db,
                    password=self._password,
                    *self._args,
                    **kwargs
                )
            except Exception:
                _logger.exception("Failed to (re)connect to redis on %s.", self._host)
                self.invalidate_connections()

        return self._w_conn

    @property
    def read_conn(self):
        """
        A connection suitable for doing readonly operations against redis. Uses a read-replica if configured.
        """
        if not self._r_conn:
            self._reconnect_after = None
            if self._rr_host:
                _logger.debug('Attempting to connect to read replica redis on %s', self._rr_host)

                try:
                    self._r_conn = redis.StrictRedis(
                       host=self._rr_host,
                       port=self._rr_port,
                       db=self._db,
                       password=self._rr_password,
                       *self._args,
                       **self._kwargs
                    )
                    return self._r_conn
                except Exception:
                    _logger.exception("Failed to (re)connect to redis on %s", self._rr_host)
                    self.invalidate_connections()
            else:
                return self.conn
        else:
            return self._r_conn

    def region(self, name=None, timeout=None, update_resets_timeout=True, serializer=None):
        """
        Return a (possibly existing) cache region.

        :param name: (str) The name of the region. Should be a dot-separated string.
        :param timeout: (int) Default=None. The TTL (secs) that the region should live before invalidating.
        :param update_resets_timeout: Default=True. Updating the cache should start the timeout over again for the
            whole region.
        :param serializer: (serializer) Default=None. An alternative serializer to the default for the region.

        :return: Region
        """
        if name is None:
            name = self._root_name

        if name in self._regions:
            return self._regions[name]

        names = name.split('.') if '.' in name else [name]
        names.reverse()
        if name != self._root_name and not name.startswith('root.'):
            names.append(self._root_name)
        parts = []
        fqname = ''
        while names:
            parts.append(names.pop())
            fqname = '.'.join(parts)
            if fqname not in self._regions:
                _logger.info("Initializing region %s", fqname)
                self._regions[fqname] = Region(
                    self, fqname,
                    timeout=timeout,
                    update_resets_timeout=update_resets_timeout,
                    serializer=serializer or self._serializer
                )

        return self._regions[fqname]

    def clear(self):
        """
        Invalidate and empty this cache region and all its sub-regions.

        :return: None
        """
        _logger.info("Clearing entire cache")
        self.region().invalidate()  # invalidate the root cache region will cascade down.


