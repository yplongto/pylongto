#!/usr/bin/env python3
# -*- coding: utf8 -*-

import logging
import pickle
import time
from collections import defaultdict
from functools import wraps
from typing import Optional, Any, Dict

import redis
from redis import Redis, StrictRedis
from redis.exceptions import ConnectionError

from confs import c, redis_conf
from modules.fastapi_redis import FastApiRedis

logger = logging.getLogger(__name__)

# 安全反序列化白名单
ALLOWED_UNPICKLE_CLASSES = {
    'builtins': [list, dict, str, int, float, bool, tuple],
    'modules.models': []  # 根据实际情况添加
}


def redis_exception_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            endpoint = '{} {}'.format(func.__module__, func.__name__)
            logger.warning(f'{endpoint}', str(e))
            return None

    return wrapper


def retry_connection(retries: int = 3, delay: float = 0.5):
    """重试连接装饰器"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            remaining = retries
            while remaining > 0:
                try:
                    return func(*args, **kwargs)
                except ConnectionError as e:
                    remaining -= 1
                    logger.warning(f"Connection failed. Retries left: {remaining}. Error: {e}")
                    if remaining == 0:
                        raise
                    time.sleep(delay)
            return func(*args, **kwargs)

        return wrapper

    return decorator


class RedisCache(FastApiRedis):
    """Redis缓存扩展类，支持集群模式和自动重试"""

    def __init__(self, app=None, strict=True, **kwargs):
        # 先初始化父类（不立即创建连接）
        super().__init__(app=app, strict=strict, **kwargs)

    @redis_exception_handler
    def exists(self, key: str) -> bool:
        """检查键是否存在"""
        return self._redis_client.exists(key) == 1

    @redis_exception_handler
    def get(self, key: str, default: Any = None) -> Any:
        """安全获取数据并自动反序列化"""
        value = self._redis_client.get(key)
        return self.safe_loads(value) if value else default

    @redis_exception_handler
    def set(self, key: str, value: Any, ex: Optional[int] = None) -> None:
        """序列化存储数据"""
        return self._redis_client.set(key, self.safe_dumps(value), ex=ex)

    @redis_exception_handler
    def mget(self, keys):
        """根据集群批量安全获取多个键并自动反序列化"""
        result = []
        if not self._redis_client or not keys:
            return result
        try:
            if not redis_conf.use_cluster:
                # 非集群配置直接 MGET
                values = [self.safe_loads(v) for v in self._redis_client.mget(keys) if v]
                result.extend(values)
            else:
                # Redis 集群，按 hash slot 分组
                grouped_keys = defaultdict(list)
                for key in keys:
                    grouped_keys[self._redis_client.keyslot(key)].append(key)
                for group in grouped_keys.values():
                    values = [self.safe_loads(v) for v in self._redis_client.mget(group) if v]
                    result.extend(values)
        except redis.exceptions.RedisError as e:  # 明确捕获 Redis 异常
            logger.error(f"Redis mget failed: {e}")
        # 当缓存里是空值的情况下，把键列表删除，避免缓存误用
        if not result:
            for key in keys:
                if self.exists_redis(key):
                    self.delete(key)
        return result

    @redis_exception_handler
    def mset(self, mapping: Dict[str, Any], ex: Optional[int] = None, is_pipeline=True):
        """批量安全存储数据
        :param mapping: 数据字典
        :param ex: 过期时间
        :param is_pipeline: 默认执行管道
        """
        processed = {k: self.safe_dumps(v) for k, v in mapping.items()}
        if not is_pipeline:
            return True if self._redis_client.mset(processed) else False

        with self._redis_client.pipeline() as pipe:
            for key, value in processed.items():
                pipe.set(key, value)
            pipe.execute()

        if ex is not None:
            with self._redis_client.pipeline() as pipe:
                for key in mapping:
                    pipe.expire(key, ex)
                pipe.execute()
        return True

    @redis_exception_handler
    def delete(self, key: str) -> None:
        """删除键"""
        self._redis_client.delete(key)

    @redis_exception_handler
    def delete_pattern(self, pattern):
        count, cursor = 0, 0
        while True:
            cursor, keys = self._redis_client.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                count += self._redis_client.delete(*keys)
            if cursor == 0:
                break
        return count

    def redis_incr(self, key, ex_time):
        """
        查询key的调用次数以及key的过期时间
        :param key:
        :param ex_time:
        :return:
        """
        # 如果没有查到key,则默认返回1，否则，自增
        res = self._redis_client.incr(key)
        # 如果是第一次查询，则设置过期时间
        if res == 1:
            self._redis_client.expire(key, ex_time)
        return res

    def keys(self, pattern):
        """
        返回所有符合pattern的key列表
        :param pattern: 匹配模式
        :return: 符合的key列表
        """
        return self._redis_client.keys(pattern)

    def hset(self, name, key, value):
        """
        存储 hash 结构
        :return:
        """
        res = self._redis_client.hset(name, key, value)
        return res

    def hmset(self, name, mapping):
        """
        存储 hash 结构 高版本hmset 修改 Redis.hmset() is deprecated. Use Redis.hset() instead
        :return:
        """
        res = self._redis_client.hset(name, mapping=mapping)
        return res

    def hget(self, name, key):
        """
        查询 hash 结构
        :return:
        """
        res = self._redis_client.hget(name, key)
        return res

    def hmget(self, name, keys, *args):
        """
        查询 hash 结构
        :return:
        """
        res = self._redis_client.hmget(name, keys, *args)
        return res

    def hgetall(self, name):
        """
        查询 hash 结构
        :return:
        """
        res = self._redis_client.hgetall(name)
        return res

    def hexists(self, name, key):
        res = self._redis_client.hexists(name, key)
        return res

    def hdel(self, name, *keys):
        res = self._redis_client.hdel(name, *keys)
        return res


class RedisQueue(RedisCache):
    def __init__(self, name, namespace='queue'):
        super().__init__()
        self.key = f"{namespace}:{name}"

    def qsize(self):
        """
        返回队列里面list内元素的数量
        :return:
        """
        return self.__redis_client.llen(self.key)

    def right_put(self, item, protocol=None):
        """
        添加新元素到队列最右方
        :param protocol: 协议号
        :param item: 待加密存储内容
        :return:
        """
        if isinstance(item, dict):
            if "id" in item:
                item.pop("_id")
            if "organization" in item:
                item.pop("organization")
            item = pickle.dumps(item, protocol)
        elif isinstance(item, list):
            item = pickle.dumps(item, protocol)
        self.__redis_client.rpush(self.key, item)

    def left_put(self, item):
        """
        添加新元素到队列最左侧
        :param item:
        :return:
        """
        if isinstance(item, dict) or isinstance(item, list):
            item = pickle.dumps(item)
        self.__redis_client.lpush(self.key, item)

    def get_wait(self, timeout=None):
        # 返回队列第一个元素，如果为空则等待至有元素被加入队列（超时时间阈值为timeout，如果为None则一直等待）
        item = self.__redis_client.blpop(self.key, timeout=timeout)
        # if item:
        #     item = item[1]  # 返回值为一个tuple
        return item

    def get_nowait(self):
        # 直接返回队列第一个元素，如果队列为空返回的是None
        item = self.__redis_client.lpop(self.key)
        return item

    def get_range_list(self, list_range):
        """
        获取所需全部元素
        :param list_range: 限制长度
        :return:
        """
        return self.__redis_client.lrange(self.key, 0, list_range)

    def get_last(self):
        """
        返回队列最后一个元素，如果队列为空返回的是None
        :return:
        """
        return self.__redis_client.rpop(self.key)

    def delete_large_data(self, start, end):
        """
        删除一定尺寸的数据
        :return:
        """
        return self.__redis_client.ltrim(self.key, start, end)


class RedisLock(RedisCache):
    """
    Redis锁的应用
    """

    def __init__(self, redis_cache: Redis = None, key: str = None, timeout: int = 10):
        super().__init__()
        if redis_cache:
            self._redis_client = redis_cache
        self.key = f"lock:{key}"
        self.timeout = timeout
        self.token = None

    def __enter__(self) -> bool:
        self.token = str(time.time())
        for _ in range(self.timeout * 2):
            if self.redis.set(self.key, self.token, nx=True, ex=self.timeout):
                return True
            time.sleep(0.5)
        raise TimeoutError(f"Acquire lock timeout for {self.key}")

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.token and self.redis.get(self.key) == self.token.encode():
            self.redis.delete(self.key)


def redis_lock(key: str, timeout: int = 10):
    """分布式锁装饰器"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with RedisLock(redis_client, key, timeout):
                return func(*args, **kwargs)

        return wrapper

    return decorator


redis_client = RedisCache()

if __name__ == '__main__':
    # 使用示例
    with RedisLock(redis_client, "my_resource"):
        print("Do something safely")


    @redis_lock("my_func_lock")
    def critical_function():
        print("Critical section")


    critical_function()
