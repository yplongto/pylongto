#!/usr/bin/env python3
# -*- coding: utf8 -*-
import logging
import pickle
from typing import Any

from redis import Redis, StrictRedis
from redis.cluster import RedisCluster, ClusterNode
from redis.exceptions import ConnectionError, RedisError

from confs import redis_conf

try:
    import redis
except ImportError as e:
    redis = None

logger = logging.getLogger(__name__)


class FastApiRedis(object):
    """Redis客户端基类，支持单节点和集群模式"""

    def __init__(self, app=None, strict=True, **kwargs):
        self._redis_client = None
        self.provider_class = StrictRedis if strict else Redis
        self.provider_kwargs = kwargs
        # 初始化
        self.init_app(app) if app is not None else self.__init_conn()

    def init_app(self, app, **kwargs):
        """初始化应用配置"""
        self.provider_kwargs.update(kwargs)

        self.__init_conn(app)
        # 注册到app.extensions
        if not hasattr(app, 'extensions'):
            app.extensions = {}
        app.extensions['redis'] = self

    def __init_conn(self, app=None):
        # 自动检测集群配置
        use_cluster = getattr(app.config, "use_cluster", redis_conf.use_cluster) if app else redis_conf.use_cluster
        if use_cluster:
            self._init_cluster()
        else:
            self._init_single_node()

    def _init_cluster(self, app=None):
        """初始化Redis集群连接"""
        startup_nodes = app.config.get("startup_nodes", redis_conf.startup_nodes) if app else redis_conf.startup_nodes
        pwd = redis_conf.password
        nodes = [ClusterNode(node["host"], node["port"]) for node in startup_nodes]
        if not self._redis_client:
            self._redis_client = RedisCluster(
                startup_nodes=nodes,
                password=pwd,
                require_full_coverage=False,
                **self.provider_kwargs
            )
            logger.info(f"Connected to Redis cluster: {nodes}")

    def _init_single_node(self, app=None):
        """初始化单节点连接"""
        redis_url = getattr(app.config, "REDIS_URI", redis_conf.REDIS_URI) if app else redis_conf.REDIS_URI
        if not self._redis_client:
            self._redis_client = self.provider_class.from_url(redis_url, **self.provider_kwargs)
            logger.info(f"Single node Redis connected: {redis_url}")

    @classmethod
    def from_url(cls, url, **kwargs):
        instance = cls()
        instance._redis_client = instance.provider_class.from_url(url, **kwargs)
        return instance

    @staticmethod
    def safe_dumps(value: Any) -> bytes:
        """安全序列化"""
        return pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def safe_loads(data: bytes) -> Any:
        """安全反序列化"""
        try:
            return pickle.loads(data)
        except pickle.UnpicklingError as e:
            logger.warning(f"Unpickling error: {e}")
            return data.decode(errors='replace') if isinstance(data, bytes) else data

    def _test_connection(self) -> bool:
        """测试连接可用性"""
        return self.ping()

    def __getattr__(self, name: str) -> Any:
        """代理到Redis客户端"""
        return getattr(self._redis_client, name)

    def __getitem__(self, key: str) -> Any:
        return self._redis_client.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self._redis_client.set(key, value)

    def __delitem__(self, key: str) -> None:
        self._redis_client.delete(key)

    def close(self) -> None:
        """关闭连接"""
        if self._redis_client:
            self._redis_client.close()
