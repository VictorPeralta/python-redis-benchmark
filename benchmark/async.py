import asyncio
import pytest
import asyncio_redis
from coredis.client.basic import Redis as CoRedis

from glide import GlideClient, RangeByIndex, Batch


def execute(loop, coro_func, *args, **kwargs):
    async def _run():
        result = await coro_func(*args, **kwargs)
        assert result is not None
        return result
    return loop.run_until_complete(_run())

@pytest.mark.benchmark(group='async-ping')
def benchmark_ping(benchmark, async_redis, loop):
    """Test the smallest and most simple PING command."""
    benchmark(execute, loop, async_redis.ping)


@pytest.mark.benchmark(group='async-set')
def benchmark_set(benchmark, async_redis, loop, key_set, value_set):
    """Test SET command with value of 1KiB size."""
    benchmark(execute, loop, async_redis.set, key_set, value_set)


@pytest.mark.benchmark(group="async-get")
def benchmark_get(benchmark, async_redis, loop, key_get):
    """Test get from Redis single 1KiB string value."""
    benchmark(execute, loop, async_redis.get, key_get)


@pytest.mark.benchmark(group="async-hgetall")
def benchmark_hgetall(benchmark, async_redis, loop, key_hgetall):
    """Test get from hash few big items."""
    benchmark(execute, loop, async_redis.hgetall, key_hgetall)


@pytest.mark.benchmark(group="async-lrange")
def benchmark_lrange(benchmark, async_redis, loop, key_lrange):
    benchmark(execute, loop, async_redis.lrange, key_lrange, 0, -1)


@pytest.mark.benchmark(group="async-zrange")
def benchmark_zrange(benchmark, async_redis, loop, key_zrange):
    """Test get from sorted set 1k items."""
    # NOTE: asyncio_redis implies `withscores` parameter
    if isinstance(async_redis, asyncio_redis.Pool):
        kw = {}
        benchmark(execute, loop, async_redis.zrange, key_zrange, 0, -1, **kw)
    elif isinstance(async_redis, GlideClient):
        range_query = RangeByIndex(0, -1)
        benchmark(execute, loop, async_redis.zrange_withscores, key_zrange, range_query)
    else:
        kw = {'withscores': True}
        benchmark(execute, loop, async_redis.zrange, key_zrange, 0, -1, **kw)


@pytest.mark.benchmark(group="async-pipeline-get")
def benchmark_pipeline_get_100(benchmark, async_redis, loop, key_get):
    """Test 100 GET operations in a pipeline."""
    async def pipeline_get_100():
        if isinstance(async_redis, asyncio_redis.Pool):
            transaction = await async_redis.multi()
            futures = []
            for _ in range(100):
                future = await transaction.get(key_get)
                futures.append(future)
            await transaction.exec()
            results = []
            for future in futures:
                result = await future
                results.append(result)
            assert len(results) == 100
            return results
        elif isinstance(async_redis, GlideClient):
            batch = Batch(is_atomic=False)
            for _ in range(100):
                batch.get(key_get)
            results = await async_redis.exec(batch,True)
            assert len(results) == 100
            return results
        elif isinstance(async_redis, CoRedis):
            async with await async_redis.pipeline(transaction=False) as pipe:
                for _ in range(100):
                    pipe.get(key_get)
                results = await pipe.execute()
                assert len(results) == 100
                return results
        else:
            # aioredis and redis-py style pipeline
            pipeline = async_redis.pipeline()
            for _ in range(100):
                pipeline.get(key_get)
            results = await pipeline.execute()
            assert len(results) == 100
            return results
    
    benchmark(execute, loop, pipeline_get_100)


@pytest.mark.benchmark(group="async-concurrent-increment")
def benchmark_concurrent_increment_1500(benchmark, async_redis, loop):
    """Test 1500 concurrent get-increment-set operations."""
    async def concurrent_increment_1500():
        async def task(i, redis):
            key = f'key:{i}'
            v = await redis.get(key)
            if isinstance(async_redis, GlideClient):
                # valkey-glide needs bytes for set values
                new_v = 1 if v is None else int(v) + 1
                await redis.set(key, str(new_v).encode())
            elif isinstance(async_redis, asyncio_redis.Pool):
                new_v = 1 if v is None else int(v) + 1
                await redis.set(key, str(new_v))
            elif isinstance(async_redis, CoRedis):
                # coredis with decode_responses=False returns bytes
                new_v = 1 if v is None else int(v) + 1
                await redis.set(key, new_v,)
            else:
                # redis.asyncio returns bytes by default
                new_v = 1 if v is None else int(v.decode()) + 1
                await redis.set(key, new_v)
        
        tasks = [asyncio.create_task(task(i, async_redis)) for i in range(1500)]
        await asyncio.gather(*tasks)
        
        final_val = await async_redis.get('key:0')
        assert final_val is not None
        return final_val
    
    benchmark(execute, loop, concurrent_increment_1500)
