from typing import Callable

import redis as redis
from cachetools import Cache

try:
    import cPickle as pickle
except:
    import pickle


class RedisCache(Cache):
    def __init__(
        self,
        *,
        maxsize: int,
        namespace: str = "redis_cache",
        client: redis.Redis = None,
        serialize: Callable = None,
        deserialize: Callable = None
    ):
        super().__init__(maxsize)
        self.__namespace = namespace
        self.__maxsize = maxsize

        self.__client = client or redis.Redis()

        if serialize is not None and callable(serialize):
            if not callable(serialize):
                raise ValueError("RedisCache.serialize should be a callable")
            self.serialize = serialize

        if deserialize is not None:
            if not callable(deserialize):
                raise ValueError("RedisCache.deserialize should be a callable")
            self.deserialize = deserialize

    def __repr__(self):
        return "%s(namespace=%s, max_keys=%r)" % (self.__class__.__name__, self.__namespace, self.__maxsize,)

    def __getitem__(self, key: str):
        try:
            val = self.__client.get(str(key))
            if val:
                return self.deserialize(val)
            else:
                raise KeyError()
        except KeyError:
            return self.__missing__(str(key))

    def __setitem__(self, key: str, value):
        while len(self) + 1 > self.__maxsize:
            self.popitem()

        self.__client.set(str(key), self.serialize(value))

    def __delitem__(self, key: str):
        self.__client.delete(str(key))

    def __contains__(self, key: str):
        return self.__client.exists(str(key)) == 1

    def __missing__(self, key: str):
        raise KeyError(key)

    def __iter__(self):
        return self.__client.scan_iter(match=self.__namespace + "*")

    def __len__(self):
        return len(self.__client.keys(pattern=self.__namespace + "*"))

    def clear(self):
        for key in self:
            try:
                del self[key]
            except:
                pass

    @property
    def currsize(self) -> int:
        """The current size of the cache."""
        return len(self)

    @staticmethod
    def deserialize(value) -> bytes:
        return pickle.loads(value)

    @staticmethod
    def serialize(value) -> bytes:
        return pickle.dumps(value)
