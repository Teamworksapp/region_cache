"""
Region based Flask caching with Redis.

This module provides high-level nested, region-based caching with Redis. A region is a dot-separated
namespace where cache keys and values live. It is implemented as hashes in redis. Parent-child relationships
are implemented as sets. Timeouts use the EXPIRE command. All work is persisted in redis, and two regions with
the same name on two different processes or even different servers will share the same storage.

Invalidation of caches is "active", not "lazy", so caches are purged immediately upon invalidation, to solve
the problem of one process knowing about a cache invalidation and the other not. Cache writes are aggressively
pipelined and transactional, so if two processes write to the same key at the same time, results will not be
inconsistent.

The cache is written as a Flask extension, so you can use init_app with a valid flask app to initialize it.
Simply set the CACHE_REDIS_URL setting in the config.

Examples:
---------

Using the region as a context manager treats everything in the context manager as a single transaction:

```
with region('abc.xyz') as r:
    x in r  # test for presence
    r[x] = 100  # get or None
    x = r[x]  # set
    del r[x]  # remove
```

Bind to blinker signals, so the cache is purged declaratively:

```
region('abc.xyz').invalidate_on(
    blinker.signal('a'),
    blinker.signal('b'),
    blinker.signal('c'),
    blinker.signal('d'),
)
```

Nest regions. If you invalidate the parent region, all the children will also be invalidated. This is recursive, so
sub-sub-sub regions will be correctly invalidated as well.

```
region('abc').region('xyz')  # subregion
region('abc').invalidate()  # invalidate abc AND xyz
```

The default serializers is "pickle", but you can supply any serializer that exposes a loads and dumps, and individual
regions can be configured differently. Children inherit the settings of their parents.

Finally, timeouts are supported, and by default the timeout refreshes itself every time you write to the cache
See the region() function for more detail on how to configure it.

"""

from .region_cache import RegionCache, Region
__version__ = '0.1.0'
