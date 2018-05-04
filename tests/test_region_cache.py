#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `region_cache` package."""

import pytest

from collections import namedtuple
from region_cache import Region, RegionCache

@pytest.fixture()
def app():
    return namedtuple('app',['config'])(config={'CACHE_REDIS_URL': 'redis://localhost:6379/5'})

@pytest.fixture()
def region_cache(app):
    c = RegionCache()
    c.init_app(app)
    yield c
    c._conn.flushall()


@pytest.fixture()
def region(region_cache):
    r = region_cache.region('example_region')
    yield r
    r.invalidate()


def test_init_app(app):
    c = RegionCache()
    c.init_app(app)
    assert c._conn
    assert c._conn.ping()
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
    assert region._conn.hget(region.name, 'key1') is not None
    assert region._conn.hget(region.name, 'key2') is not None


def test_invalidate(region):
    region['key'] = 'value'
    region.invalidate()
    assert 'key' not in region
    assert region._conn.hget(region.name, 'key') is None

    sb = region.region('sub')
    sb['key2'] = 'value'
    region.invalidate()

    assert region._conn.hget(sb.name, 'key2') is None
    assert 'key2' not in sb


def test_items(region):
    region['foo'] = 'bar'
    assert region['foo'] == 'bar'
    assert region._conn.hget(region.name, 'foo') is not None
    del region._local_storage['foo']
    assert region['foo'] == 'bar'
    assert 'foo' in region._local_storage

    del region['foo']
    assert pytest.raises(KeyError, lambda: region['foo'])


def test_children(region):
    sb = region.region('sub')
    assert sb in list(region.children())


def test_invalidate_on(region):
    import blinker
    s = blinker.signal('named_signal')
    t = blinker.signal('other_signal')

    region['key'] = 'value'
    region.invalidate_on(s, t)

    s.send('nothing',in_='particular')
    assert 'key' not in region
    assert region._conn.hget(region.name, 'key') is None

    region['key'] = 'value'

    t.send('nothing', in_='particular')
    assert 'key' not in region
    assert region._conn.hget(region.name, 'key') is None

called = 0
def test_cached(region):
    @region.cached
    def foobar(k, x=None):
        global called
        called += 1
        return k

    foobar(1)
    assert called == 1
    foobar(1)
    assert called == 1
