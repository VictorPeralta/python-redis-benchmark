import asyncio
import os
import time
import aioredis
from redis.asyncio import BlockingConnectionPool, Redis as AsyncioRedis
from coredis import Redis as CoredisRedis
from glide import GlideClient, GlideClientConfiguration, NodeAddress


url = os.environ.get('REDIS_URL', 'redis://redis:6379/13')
max_conn = os.environ.get('MAX_CONNECTIONS', 64)

# ----Single----

# aioredis: 1500 tasks with with single connections: 0.031246582977473736s
# redispy: 1500 tasks with single connections: 0.13096699997549877s
# coredis: 1500 tasks with single connection: 0.16489741700934246s
# valkey-glide: 1500 tasks with single connection: 0.12460599996848032s

# ----Pool----

# aioredis pool: 1500 tasks with blocking pool with 64 connections: 0.0338731249794364s
# redispy pool: 1500 tasks with blocking pool with 64 connections: 0.14161749999038875s
# coredis pool: 1500 tasks with blocking pool with 64 connections: 0.1515817089821212s

async def run_aioredis_pool(n=1500):
    async def task(i, redis):
        key = f'key:{i}'
        v = await redis.get(key)
        new_v = 1 if v is None else int(v) + 1
        await redis.set(key, new_v, expire=600)

    redis = await aioredis.create_redis_pool(
        url, encoding='utf-8', maxsize=max_conn
    )

    tasks = [asyncio.create_task(task(i, redis)) for i in range(n)]
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    t = time.perf_counter() - start
    print(f'aioredis pool: {n} tasks with blocking pool with {max_conn} connections: {t}s')




async def run_redispy_pool(n=1500):
    async def task(i, redis):
        key = f'key:{i}'
        v = await redis.get(key)
        new_v = 1 if v is None else int(v.decode()) + 1
        await redis.set(key, new_v, ex=600)
    pool = BlockingConnectionPool.from_url(
        url=url, max_connections=max_conn
    )
    redis = AsyncioRedis(connection_pool=pool)

    tasks = [asyncio.create_task(task(i, redis)) for i in range(n)]
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    t = time.perf_counter() - start
    print(f'redispy pool: {n} tasks with blocking pool with {max_conn} connections: {t}s')





async def run_aioredis(n=1500):
    async def task(i, redis):
        key = f'key:{i}'
        v = await redis.get(key)
        new_v = 1 if v is None else int(v) + 1
        await redis.set(key, new_v, expire=600)

    redis = await aioredis.create_redis(
        url, encoding='utf-8'
    )

    tasks = [asyncio.create_task(task(i, redis)) for i in range(n)]
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    t = time.perf_counter() - start
    print(f'aioredis: {n} tasks with with single connections: {t}s')

async def run_redispy(n=1500):
    async def task(i, redis):
        key = f'key:{i}'
        v = await redis.get(key)
        new_v = 1 if v is None else int(v.decode()) + 1
        await redis.set(key, new_v, ex=600)
    redis = await AsyncioRedis.from_url(
        url=url
    )

    tasks = [asyncio.create_task(task(i, redis)) for i in range(n)]
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    t = time.perf_counter() - start
    print(f'redispy: {n} tasks with single connections: {t}s')


async def run_coredis_pool(n=1500):
    async def task(i, redis):
        key = f'key:{i}'
        v = await redis.get(key)
        new_v = 1 if v is None else int(v) + 1
        await redis.set(key, new_v, ex=600)
    
    redis = CoredisRedis(
        host='redis',
        port=6379,
        db=13,
        max_connections=max_conn,
        decode_responses=False,
        protocol_version=2,
    )

    tasks = [asyncio.create_task(task(i, redis)) for i in range(n)]
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    t = time.perf_counter() - start
    print(f'coredis pool: {n} tasks with blocking pool with {max_conn} connections: {t}s')


async def run_coredis(n=1500):
    async def task(i, redis):
        key = f'key:{i}'
        v = await redis.get(key)
        new_v = 1 if v is None else int(v) + 1
        await redis.set(key, new_v, ex=600)
    
    redis = CoredisRedis(
        host='redis',
        port=6379,
        db=13,
        max_connections=1,
        decode_responses=False,
        protocol_version=2,
    )

    tasks = [asyncio.create_task(task(i, redis)) for i in range(n)]
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    t = time.perf_counter() - start
    print(f'coredis: {n} tasks with single connection: {t}s')


async def run_glide(n=1500):
    async def task(i, redis):
        key = f'key:{i}'
        v = await redis.get(key)
        new_v = 1 if v is None else int(v) + 1
        await redis.set(key, str(new_v).encode())
    
    config = GlideClientConfiguration(
        [NodeAddress('redis', 6379)],
        request_timeout=500,
        inflight_requests_limit=10000
    )
    redis = await GlideClient.create(config)

    tasks = [asyncio.create_task(task(i, redis)) for i in range(n)]
    start = time.perf_counter()
    await asyncio.gather(*tasks)
    t = time.perf_counter() - start
    print(f'valkey-glide: {n} tasks with single connection: {t}s')
    await redis.close()






if __name__ == "__main__":
    print("\n----Single----\n")
    asyncio.run(run_aioredis(n=1500))
    asyncio.run(run_redispy(n=1500))
    asyncio.run(run_coredis(n=1500))
    asyncio.run(run_glide(n=1500))
    print("\n----Pool----\n")
    asyncio.run(run_aioredis_pool(n=1500))
    asyncio.run(run_redispy_pool(n=1500))
    asyncio.run(run_coredis_pool(n=1500))
