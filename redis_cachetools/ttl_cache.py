from .cache import RedisCache


class RedisTTLCache(RedisCache):
    def __init__(self, ttl: int,  **kwargs):
        super().__init__(**kwargs)
        self.__ttl = ttl

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self.__client.expire(str(key), self.__ttl)

