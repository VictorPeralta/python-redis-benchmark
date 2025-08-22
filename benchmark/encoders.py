import pytest

from aioredis.util import encode_command


@pytest.fixture(params=[
    'string', b'bytess', 10**5, 10 / 3,
])
def data(request):
    return request.param


@pytest.mark.encoder
@pytest.mark.benchmark(group='encoder')
@pytest.mark.parametrize('repeat', [
    1, 10, 100, 1000,
])
def benchmark_aioredis_encoder(benchmark, data, repeat):

    def do(cmd, args):
        val = encode_command(cmd, *args)
        assert isinstance(val, bytearray)
    benchmark(do, 'foo', [data] * repeat)


