#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `region_cache` package."""

import pytest

from collections import namedtuple
from region_cache import RegionCache


@pytest.fixture(params=[
    {'REGION_CACHE_URL': 'redis://localhost:6379/5'},
    {
        'REGION_CACHE_URL': 'redis://localhost:6379/5',
        'REGION_CACHE_RR_URL': 'redis://localhost:6379/5'
    },
    {
        'REGION_CACHE_HOST': 'localhost',
        'REGION_CACHE_PORT': 6379,
        'REGION_CACHE_DB': 5,
        'REGION_CACHE_RR_HOST': 'localhost',
        'REGION_CACHE_RR_PORT': 6379
    },
    {
        'REGION_CACHE_HOST': 'localhost',
        'REGION_CACHE_PORT': 6379,
        'REGION_CACHE_DB': 5,
        'REGION_CACHE_OP_TIMEOUT': 0.5
    },
    {
        'REGION_CACHE_HOST': 'localhost',
        'REGION_CACHE_PORT': 6379,
        'REGION_CACHE_DB': 5,
        'REGION_CACHE_OP_TIMEOUT': 0.5,
        'REGION_CACHE_OP_TIMEOUT_RAISE': False,
        'REGION_CACHE_OP_TIMEOUT_RECONNECT': True,
        'REGION_CACHE_REDIS_OPTIONS': {
            'max_connections': 3
        }

    }
])
def app(request):
    return namedtuple('app', ['config'])(config=request.param)


@pytest.fixture()
def region_cache(app):
    c = RegionCache()
    c.init_app(app)
    c.conn.flushall()
    yield c
    c.conn.flushall()


@pytest.fixture()
def region(region_cache):
    r = region_cache.region('example_region')
    yield r
    r.invalidate()


@pytest.fixture()
def region_with_timeout(region_cache):
    r = region_cache.region('timed_region', timeout=2)
    yield r
    r.invalidate()


def test_init_app(app):
    c = RegionCache()
    c.init_app(app)
    assert c.conn
    assert c.conn.ping()
    assert c._root
    assert c._root_name in c._regions
    assert c._regions[c._root_name] is c._root
    assert len(c._regions) == 1


def test_subregions(region_cache):
    r = region_cache.region('abc.xyz')
    assert '{region_cache._root_name}.abc'.format(region_cache=region_cache) in region_cache._regions
    assert '{region_cache._root_name}.abc.xyz'.format(region_cache=region_cache)  in region_cache._regions
    assert 'abc.xyz' not in region_cache._regions
    assert 'xyz' not in region_cache._regions

    r1 = region_cache.region('xml', timeout=60)
    assert r1._timeout == 60
    r2 = r1.region('json')
    assert r2._timeout == 60


def test_region_context_manager(region):
    with region as r:
        r['key1'] = 0
        r['key2'] = 1

    assert 'key1' in region
    assert 'key2' in region
    assert region._region_cache.conn.hget(region.name, 'key1') is not None
    assert region._region_cache.conn.hget(region.name, 'key2') is not None


def test_invalidate(region):
    region['key'] = 'value'
    region.invalidate()
    assert 'key' not in region
    assert region._region_cache.conn.hget(region.name, 'key') is None

    sb = region.region('sub')
    sb['key2'] = 'value'
    region.invalidate()

    assert region._region_cache.conn.hget(sb.name, 'key2') is None
    assert 'key2' not in sb


def test_invalidate_region(region_cache, region):
    region['key'] = 'value'
    region_cache.region('root').invalidate()
    assert 'key' not in region
    assert region._region_cache.conn.hget(region.name, 'key') is None

    sb = region.region('sub')
    sb['key2'] = 'value'
    region.invalidate()

    assert region._region_cache.conn.hget(sb.name, 'key2') is None
    assert 'key2' not in sb


def test_items(region):
    region['foo'] = 'bar'
    assert region['foo'] == 'bar'
    assert region._region_cache.conn.hget(region.name, 'foo') is not None
    del region['foo']
    assert pytest.raises(KeyError, lambda: region['foo'])


def test_children(region):
    sb = region.region('sub')
    assert sb in list(region.children())


def test_iter(region, region_cache):
    region['foo'] = 'bar'
    assert [x for x in region]
    region.invalidate()


def test_invalidate_on(region):
    import blinker
    s = blinker.signal('named_signal')
    t = blinker.signal('other_signal')

    region['key'] = 'value'
    region.invalidate_on(s, t)

    s.send('nothing',in_='particular')
    assert 'key' not in region
    assert region._region_cache.conn.hget(region.name, 'key') is None

    region['key'] = 'value'

    t.send('nothing', in_='particular')
    assert 'key' not in region
    assert region._region_cache.conn.hget(region.name, 'key') is None


def test_cached(region):
    called = [0]

    @region.cached
    def foobar(k, x=None):
        called[0] += 1
        return k

    foobar(1)
    assert called[0] == 1
    foobar(1)
    assert called[0] == 1


def test_get_or_compute(region):
    x = region.get_or_compute('computed_key', 0)
    assert 'computed_key' in region
    assert region['computed_key'] == 0
    assert x == 0

    y = region.get_or_compute('computed_key2', lambda: 200)
    assert y == 200
    assert 'computed_key2' in region
    assert region['computed_key2'] == 200


def test_invalidate_connections(region_cache):
    region_cache.invalidate_connections()
    assert region_cache._w_conn is None
    assert region_cache._r_conn is None


def test_reconnect_backoff(region, region_cache):
    region['key1'] = 0
    region['key2'] = 1
    region_cache._reconnect_backoff = 5  # 5 second backoff before trying to reconnect
    region_cache.invalidate_connections()
    assert region_cache.is_disconnected()
    with pytest.raises(KeyError):
        region['key1']
    assert region_cache._w_conn is None
    assert region_cache._r_conn is None


def test_timeout_with_context(region_with_timeout):
    with region_with_timeout as r:
        r['key1'] = 0
        r['key2'] = 1

    assert region_with_timeout._region_cache.conn.hget(region_with_timeout.name, 'key1') is not None
    assert region_with_timeout._region_cache.conn.hget(region_with_timeout.name, 'key2') is not None

    assert region_with_timeout._region_cache.conn.ttl(region_with_timeout.name) > 0

    assert 'key1' in region_with_timeout
    assert 'key2' in region_with_timeout

    import time
    time.sleep(1)

    assert region_with_timeout._region_cache.conn.ttl(region_with_timeout.name) > 0

    assert region_with_timeout._region_cache.conn.hget(region_with_timeout.name, 'key1') is not None
    assert region_with_timeout._region_cache.conn.hget(region_with_timeout.name, 'key2') is not None

    assert 'key1' in region_with_timeout
    assert 'key2' in region_with_timeout

    time.sleep(1.5)

    assert region_with_timeout._region_cache.conn.ttl(region_with_timeout.name) == -2

    assert 'key1' not in region_with_timeout
    assert 'key2' not in region_with_timeout



def test_timeout(region_with_timeout):
    region_with_timeout['key1'] = 0
    region_with_timeout['key2'] = 1

    assert region_with_timeout._region_cache.conn.hget(region_with_timeout.name, 'key1') is not None
    assert region_with_timeout._region_cache.conn.hget(region_with_timeout.name, 'key2') is not None

    assert region_with_timeout._region_cache.conn.ttl(region_with_timeout.name) > 0

    assert 'key1' in region_with_timeout
    assert 'key2' in region_with_timeout

    import time
    time.sleep(1)

    assert region_with_timeout._region_cache.conn.ttl(region_with_timeout.name) > 0

    assert region_with_timeout._region_cache.conn.hget(region_with_timeout.name, 'key1') is not None
    assert region_with_timeout._region_cache.conn.hget(region_with_timeout.name, 'key2') is not None

    assert 'key1' in region_with_timeout
    assert 'key2' in region_with_timeout

    time.sleep(1.5)

    assert region_with_timeout._region_cache.conn.ttl(region_with_timeout.name) == -2

    assert 'key1' not in region_with_timeout
    assert 'key2' not in region_with_timeout

    # make sure we can recreate the region.

    region_with_timeout['key1'] = 0
    region_with_timeout['key2'] = 1

    assert region_with_timeout._region_cache.conn.hget(region_with_timeout.name, 'key1') is not None
    assert region_with_timeout._region_cache.conn.hget(region_with_timeout.name, 'key2') is not None

    assert region_with_timeout._region_cache.conn.ttl(region_with_timeout.name) > 0

    assert 'key1' in region_with_timeout
    assert 'key2' in region_with_timeout

    subregion = region_with_timeout.region("subregion")

    subregion['key1'] = 0
    subregion['key2'] = 1

    assert subregion._region_cache.conn.hget(subregion.name, 'key1') is not None
    assert subregion._region_cache.conn.hget(subregion.name, 'key2') is not None

    assert subregion._region_cache.conn.ttl(subregion.name) > 0

    assert 'key1' in subregion
    assert 'key2' in subregion

    import time
    time.sleep(1)

    assert subregion._region_cache.conn.ttl(subregion.name) > 0

    assert subregion._region_cache.conn.hget(subregion.name, 'key1') is not None
    assert subregion._region_cache.conn.hget(subregion.name, 'key2') is not None

    assert 'key1' in subregion
    assert 'key2' in subregion

    time.sleep(1.5)

    assert subregion._region_cache.conn.ttl(subregion.name) == -2

    assert 'key1' not in subregion
    assert 'key2' not in subregion

    # make sure we can recreate the region.

    subregion['key1'] = 0
    subregion['key2'] = 1

    assert subregion._region_cache.conn.hget(subregion.name, 'key1') is not None
    assert subregion._region_cache.conn.hget(subregion.name, 'key2') is not None

    assert subregion._region_cache.conn.ttl(subregion.name) > 0

    assert 'key1' in subregion
    assert 'key2' in subregion
