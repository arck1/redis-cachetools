import pytest
import redis

from redis_cachetools.ttl_cache import RedisTTLCache


class PatchedRedisClient(redis.Redis):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = {}

    def get(self, key, *args, **kwargs):
        if key in self.cache:
            return self.cache.get(key)
        raise ValueError("not found {}".format(key))

    def set(self, key, value, *args, **kwargs):
        self.cache[key] = value


@pytest.fixture
def redis_ttl_cache_fixture():
    return RedisTTLCache(ttl=30, client=PatchedRedisClient())


def test_redis_ttl_cache_class(redis_ttl_cache_fixture):
    assert redis_ttl_cache_fixture is not None
