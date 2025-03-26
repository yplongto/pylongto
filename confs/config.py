#!/usr/bin/env python3
# -*- coding: utf8 -*-
import os
import json
import yaml
import logging
import sys
from urllib.parse import quote_plus
from collections import OrderedDict

try:
    from backports import configparser
except ImportError as e:
    import configparser

# 初始化日志模块
import utils.logserver

from env import SERVICE_CONFIG_FILENAME, IS_DEV_ENV

# 禁用python-multipart表单日志
# pip install python-multipart
multipart_multipart_logger = logging.getLogger('multipart.multipart')
multipart_multipart_logger.setLevel(logging.WARNING)

# 获取当前脚本的启动命令行参数标识
service_name = 'pylongto-' + sys.argv[-1].replace('/', ' ').replace('\\', ' ').split(' ')[-1]

# 解析环境配置yaml文件
env_config = yaml.load(open(SERVICE_CONFIG_FILENAME, encoding='utf-8'), Loader=yaml.SafeLoader)


class BaseConfig(object):
    def __init__(self, config_path=None):
        # 通过解析器解析conf等文件
        # self.config = configparser.ConfigParser()
        # self.config.read(config_path or SERVICE_CONFIG_FILENAME)
        self.__dict__ = dict(self.__dict__, **env_config)
        self._config()
        self.__config()

    def get(self, item):
        return getattr(self, item, None)

    def __getattr__(self, item):
        return getattr(self.__dict__, item, None)

    def __getitem__(self, item):
        return self.__dict__[item]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __delitem__(self, key):
        del self.__dict__[key]

    def _config(self):
        pass

    def __config(self):
        pass


class Config(BaseConfig):
    """
    服务器配置获取
    可选： SERVER_CONFIG
    """

    SSL_REDIRECT = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_RECORD_QUERIES = True
    SQLALCHEMY_COMMIT_ON_TEARDOWN = True
    # 服务名称
    SERVICE_NAME = service_name
    # 认证密钥
    SECRET_KEY = os.environ.get('SECRET_KEY')
    # 线程池连接数
    THREAD_POOL_SIZE = int(os.environ.get("THREAD_POOL_SIZE", 100))
    # 连接池最大连接数
    SQLALCHEMY_POOL_SIZE = int(os.environ.get("SQLALCHEMY_POOL_SIZE", 50))
    # 连接数可以超过多少个
    SQLALCHEMY_MAX_OVERFLOW = int(os.environ.get("SQLALCHEMY_MAX_OVERFLOW", 300))
    # 空闲连接回收间隔时间
    SQLALCHEMY_POOL_RECYCLE = int(os.environ.get("SQLALCHEMY_POOL_RECYCLE", 30))
    # 连接池的连接超时时间
    SQLALCHEMY_POOL_TIMEOUT = int(os.environ.get("SQLALCHEMY_POOL_TIMEOUT", 60))
    SQLALCHEMY_ENGINE_OPTIONS = {'pool_pre_ping': True, 'pool_use_lifo': True, 'echo': False}

    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)

        self.SQLALCHEMY_DATABASE_URI = self.create_pg_uri()
        self.REDIS_URI = self.create_redis_uri()

    def create_pg_uri(self):
        engine_str_format = ('postgresql+psycopg://{username}:{password}@{host}:{port}/{db}'
                             '?application_name={service_name}')
        return engine_str_format.format(
            username=self.postgres['username'],
            password=quote_plus(self.postgres['password']),
            host=self.postgres['host'],
            port=self.postgres['port'],
            db=self.postgres['database'],
            service_name=self.SERVICE_NAME
        )

    def create_redis_uri(self, db=None):
        return 'redis://:{password}@{host}:{port}/{database}'.format(
            password=quote_plus(self.redis['password']),
            host=self.redis['host'],
            port=self.redis['port'],
            database=db or self.redis['cache_db']
        )

    @staticmethod
    def init_app(app):
        pass


class MongoConf(BaseConfig):
    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)
        self.MONGO_URI = self.create_mongo_uri()

    def get_mongo_database(self):
        return self.mongo['database']

    def create_mongo_uri(self):
        engine_str_format = ('mongodb://{username}:{password}@{host}:{port}/{database}'
                             '?readPreference=secondaryPreferred&authSource={auth_source}')
        return engine_str_format.format(
            username=self.mongo['username'],
            password=quote_plus(self.mongo['password']),
            host=self.mongo['host'],
            port=self.mongo['port'],
            database=self.mongo['database'],
            auth_source=self.mongo['auth_source']
        )


class RedisConf(BaseConfig):
    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)
        self.__config()

    def create_redis_uri(self, db=None):
        return 'redis://:{password}@{host}:{port}/{database}'.format(
            password=quote_plus(self.redis['password']),
            host=self.redis['host'],
            port=self.redis['port'],
            database=db or self.redis['cache_db']
        )

    def __config(self):
        self.host = self.redis['host']
        self.port = self.redis['port']
        self.password = self.redis['password']
        self.database = self.redis['cache_db']
        self.startup_nodes = self.redis.get('startup_nodes', [])
        self.use_cluster = True if self.startup_nodes else False
        self.REDIS_URI = self.create_redis_uri()


class KafkaConf(BaseConfig):
    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)
        self.__config()

    def __config(self):
        self.bootstrap_servers = self.kafka.get("bootstrap_servers")
        self.user = self.kafka.get("user")
        self.password = self.kafka.get("password")
        self.sasl_mechanism = self.kafka.get("sasl_mechanism")
        self.security_protocol = self.kafka.get("security_protocol")


class FdfsConf(BaseConfig):
    def __init__(self, config_path=None):
        super(FdfsConf, self).__init__(config_path=config_path)
        self.__load_conf()

    def __load_conf(self):
        self.connect_timeout = self.fdfs['connect_timeout']
        self.network_timeout = self.fdfs['network_timeout']
        self.base_path = self.fdfs['base_path']
        self.tracker_server = self.fdfs['tracker_server']
        self.log_level = self.fdfs['log_level']
        self.use_connection_pool = self.fdfs['use_connection_pool']
        self.connection_pool_max_idle_time = self.fdfs['connection_pool_max_idle_time']
        self.load_fdfs_parameters_from_tracker = self.fdfs['load_fdfs_parameters_from_tracker']
        self.use_storage_id = self.fdfs['use_storage_id']
        self.storage_ids_filename = self.fdfs['storage_ids_filename']
        self.http_tracker_server_port = self.fdfs['http.tracker_server_port']

    def fdfs_config(self):
        conf = OrderedDict()
        conf['connect_timeout'] = self.connect_timeout
        conf['network_timeout'] = self.network_timeout
        conf['base_path'] = self.base_path
        conf['tracker_server'] = self.tracker_server
        conf['log_level'] = self.log_level
        conf['use_connection_pool'] = self.use_connection_pool
        conf['connection_pool_max_idle_time'] = self.connection_pool_max_idle_time
        conf['load_fdfs_parameters_from_tracker'] = self.load_fdfs_parameters_from_tracker
        conf['use_storage_id'] = self.use_storage_id
        conf['storage_ids_filename'] = self.storage_ids_filename
        conf['http.tracker_server_port'] = self.http_tracker_server_port
        return conf


class GoFastDFSConf(BaseConfig):
    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)
        self.__config()

    def __config(self):
        self.host = self.go_fastdfs['host']
        self.port = self.go_fastdfs['port']
        self.group = self.go_fastdfs['group']
        self.scene = self.go_fastdfs['scene']


class MQTTConfig(BaseConfig):
    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)
        self.__config()

    def __config(self):
        self.host = self.mqtt['host']
        self.port = self.mqtt['port']
        self.user = self.mqtt['user']
        self.password = self.mqtt['password']
        self.services = self.mqtt['services']


class FTPConfig(BaseConfig):
    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)
        self.__config()

    def __config(self):
        self.host = self.ftp['host']
        self.port = self.ftp['port']
        self.user = self.ftp['user']
        self.password = self.ftp['password']
        self.local_dir = self.ftp['local_dir']


class ElasticSearchConfig(BaseConfig):
    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)
        self.__config()

    def __config(self):
        self.host = self.elastic_search['host']
        self.username = self.elastic_search['username']
        self.password = self.elastic_search['password']


class MailConf(BaseConfig):
    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)
        self.__config()

    def __config(self):
        self.mail_status = self.mail['mail_status']
        self.host = self.mail['host']
        self.port = self.mail['port']
        self.user = self.mail['user']
        self.password = self.mail['password']
        self.title = self.mail['title']
        self.sender = self.mail['sender']


class MicroServiceConf(BaseConfig):
    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)
        self.__config()

    def __config(self):
        self.resource = self.microservices_url.get("resource")
        self.power_noc = self.microservices_url.get("power_noc")
        self.user_center = self.microservices_url.get("user_center")
        self.fsu_svr = self.microservices_url.get("fsusvr")
        self.energy_url = self.microservices_url.get("energy")
        self.remote = self.microservices_url.get("remote")
        self.socket_url = self.microservices_url.get("socket_url")
        self.log_service = self.microservices_url.get("log_service")
        self.sms_interface = self.microservices_url.get("sms_interface")


class CommonConf(BaseConfig):
    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)
        self.__config()

    def __config(self):
        self.x_app_id = self.common['X_APP_ID']
        self.x_app_key = self.common['X_APP_KEY']
        self.threshold = self.common['THRESHOLD']


class ReleaseConf(BaseConfig):
    def __init__(self, config_path=None):
        super().__init__(config_path=config_path)
        self.__config()

    def __config(self):
        self.version = self.release['version']
        self.ios_version = self.release['ios_version']
        self.android_version = self.release['android_version']
        self.release_time = self.release['version_release_time']
