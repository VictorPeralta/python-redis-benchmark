import pytest
import redis
import io
import asyncio
try:
    import uvloop
except ImportError:
    has_uvloop = False
else:
    has_uvloop = True

import aioredis
import asyncio_redis
import redis.asyncio as redis_async
from coredis import Redis as CoredisRedis
from glide import GlideClient, GlideClientConfiguration, NodeAddress

from aioredis import parser as aioredis_parser
from asyncio_redis.protocol import RedisProtocol, HiRedisProtocol


def pytest_addoption(parser):
    parser.addoption('--redis-host', default='localhost',
                     help="Redis server host")
    parser.addoption('--redis-port', default=6379, type=int,
                     help="Redis server port")


@pytest.fixture(scope='session')
def redispy(request):
    host = request.config.getoption('--redis-host')
    port = request.config.getoption('--redis-port')
    pool = redis.ConnectionPool(host=host, port=port,
                                parser_class=request.param)
    r = redis.Redis(host=host, port=port, connection_pool=pool)
    return r


class FakeSocket(io.BytesIO):

    def recv(self, size):
        return self.read(size)

    def recv_into(self, buf):
        return self.readinto(buf)



@pytest.fixture(params=[
    pytest.param(None, id='bytes'),
    pytest.param('utf-8', id='utf-8'),
])
def reader_encoding(request):
    return request.param


@pytest.fixture()
def reader(request, reader_encoding):
    if reader_encoding:
        return request.param(encoding=reader_encoding)
    return request.param()




async def aioredis_start(host, port):
    client = await aioredis.create_redis_pool(
        (host, port),
        maxsize=2)
    await client.ping()
    return client


async def aioredis_py_start(host, port):
    client = await aioredis.create_redis_pool(
        (host, port),
        maxsize=2, parser=aioredis_parser.PyReader)
    await client.ping()
    return client


async def aioredis_stop(client):
    client.close()
    await client.wait_closed()


async def asyncio_redis_start(host, port):
    pool = await asyncio_redis.Pool.create(
        host, port, poolsize=2,
        protocol_class=HiRedisProtocol)
    await pool.ping()
    return pool


async def asyncio_redis_py_start(host, port):
    pool = await asyncio_redis.Pool.create(
        host, port, poolsize=2,
        protocol_class=RedisProtocol)
    await pool.ping()
    return pool


async def asyncio_redis_stop(pool):
    pool.close()


async def redis_asyncio_start(host, port):
    client = redis_async.Redis(
        host=host,
        port=port,
        max_connections=2000,
        decode_responses=False,
    )
    await client.ping()
    return client


async def redis_asyncio_stop(client):
    # Gracefully close if available
    try:
        await client.aclose()
    except Exception:
        pass


# Optional coredis client support
async def coredis_start(host, port):
    client = CoredisRedis(
        host=host,
        port=port,
        db=0,
        max_connections=2,
        decode_responses=False,
        # Use RESP2 for compatibility with older redis versions
        protocol_version=2,
    )
    await client.ping()
    return client


async def coredis_stop(client):
    try:
        await client.quit()
    except Exception:
        pass


# Optional valkey-glide client support
async def valkey_glide_start(host, port):
    config = GlideClientConfiguration(
        [NodeAddress(host, port)],
        request_timeout=500,
        inflight_requests_limit=1500
    )
    client = await GlideClient.create(config)
    await client.ping()
    return client


async def valkey_glide_stop(client):
    try:
        await client.close()
    except Exception:
        pass


@pytest.fixture(params=[
    pytest.param((aioredis_start, aioredis_stop),
                 marks=[pytest.mark.hiredis, pytest.mark.aioredis],
                 id='aioredis[hi]-----'),
    pytest.param((aioredis_py_start, aioredis_stop),
                 marks=[pytest.mark.pyreader, pytest.mark.aioredis],
                 id='aioredis[py]-----'),
    pytest.param((asyncio_redis_start, asyncio_redis_stop),
                 marks=[pytest.mark.hiredis, pytest.mark.asyncio_redis],
                 id='asyncio_redis[hi]'),
    pytest.param((asyncio_redis_py_start, asyncio_redis_stop),
                 marks=[pytest.mark.pyreader, pytest.mark.asyncio_redis],
                 id='asyncio_redis[py]'),
    pytest.param((redis_asyncio_start, redis_asyncio_stop),
                 marks=[pytest.mark.redis_asyncio],
                 id='redis.asyncio'),
    pytest.param((coredis_start, coredis_stop),
                 marks=[pytest.mark.coredis],
                 id='coredis'),
    pytest.param((valkey_glide_start, valkey_glide_stop),
                 marks=[pytest.mark.valkey_glide],
                 id='valkey-glide'),
])
def async_redis(loop, request):
    start, stop = request.param
    host = request.config.getoption('--redis-host')
    port = request.config.getoption('--redis-port')
    client = loop.run_until_complete(start(host, port))
    yield client
    if stop:
        loop.run_until_complete(stop(client))


if has_uvloop:
    kw = dict(params=[
        pytest.param(uvloop.new_event_loop, marks=pytest.mark.uvloop,
                     id='uvloop-'),
        pytest.param(asyncio.new_event_loop, marks=pytest.mark.asyncio,
                     id='asyncio'),
    ])
else:
    kw = dict(params=[
        pytest.param(asyncio.new_event_loop, marks=pytest.mark.asyncio,
                     id='asyncio'),
    ])


@pytest.fixture(**kw)
def loop(request):
    """Asyncio event loop, either uvloop or asyncio."""
    loop = request.param()
    asyncio.set_event_loop(None)
    yield loop
    loop.stop()
    loop.run_forever()
    loop.close()


@pytest.fixture
def _aioredis(loop):
    r = loop.run_until_complete(aioredis.create_redis(('localhost', 6379)))
    try:
        yield r
    finally:
        r.close()
        loop.run_until_complete(r.wait_closed())


MIN_SIZE = 10

MAX_SIZE = 2**15


@pytest.fixture(scope='session')
def r(request):
    host = request.config.getoption('--redis-host')
    port = request.config.getoption('--redis-port')
    return redis.Redis(host=host, port=port)


@pytest.fixture(
    scope='session',
    params=[MIN_SIZE, 2**8, 2**10, 2**12, 2**14, MAX_SIZE],
    ids=lambda n: str(n).rjust(5, '-')
    )
def data_size(request):
    return request.param


def data_value(size):
    return ''.join(chr(i) for i in range(size))


@pytest.fixture(scope='session')
def key_get(data_size, r):
    key = 'get:size:{:05}'.format(data_size)
    value = data_value(data_size)
    assert r.set(key, value) is True
    return key


@pytest.fixture(scope='session')
def key_hgetall(data_size, r):
    items = data_size
    size = MAX_SIZE // items
    key = 'dict:size:{:05}x{:05}'.format(items, size)
    val = data_value(size)
    p = r.pipeline()
    for i in range(items):
        p.hset(key, 'f:{:05}'.format(i), val)
    p.execute()
    return key


@pytest.fixture(scope='session')
def key_zrange(data_size, r):
    key = 'zset:size:{:05}'.format(data_size)
    p = r.pipeline()
    for i in range(data_size):
        val = 'val:{:05}'.format(i)
        p.zadd(key, {val: i / 2})
    p.execute()
    return key


@pytest.fixture(scope='session')
def key_lrange(data_size, r):
    size = MAX_SIZE // data_size
    key = 'list:size:{:05}x{:05}'.format(data_size, size)
    val = data_value(size)
    p = r.pipeline()
    for i in range(data_size):
        p.lpush(key, val)
    p.execute()
    return key


@pytest.fixture(scope='session')
def key_set(data_size):
    return 'set:size:{:05}'.format(data_size)


@pytest.fixture(scope='session')
def value_set(key_set, data_size, r):
    val = data_value(data_size)
    r.set(key_set, val)
    return val


@pytest.fixture(scope='session')
def parse_bulk_str(data_size):
    val = data_value(data_size).encode('utf-8')
    return b'$%d\r\n%s\r\n' % (len(val), val)


@pytest.fixture(scope='session')
def parse_multi_bulk(data_size):
    items = data_size
    item = MAX_SIZE // items

    val = data_value(item).encode('utf-8')
    val = b'$%d\r\n%s\r\n' % (len(val), val)
    return (b'*%d\r\n' % items) + (val * items)
