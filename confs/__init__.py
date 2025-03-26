#!/usr/bin/env python3
# -*- coding: utf8 -*-
from utils.async_executor import AsyncExecutorManager
from .config import (Config, FdfsConf, RedisConf,
                     MongoConf, KafkaConf, MicroServiceConf, GoFastDFSConf,
                     CommonConf, MQTTConfig, FTPConfig, ElasticSearchConfig, MailConf, ReleaseConf)

# 基础配置
c = Config()
# Mongo
mongo_conf = MongoConf()
# Redis
redis_conf = RedisConf()
# kafka
kafka_conf = KafkaConf()
# FDFS
fdfs_conf = FdfsConf()
# go-fastdfs
go_fastdfs_conf = GoFastDFSConf()
# MQTT
mqtt_conf = MQTTConfig()
# FTP
ftp_conf = FTPConfig()
# Elasticsearch
elastic_search_conf = ElasticSearchConfig()
# mail config
mail_conf = MailConf()
# micro config
micro_conf = MicroServiceConf()
# common config
common_conf = CommonConf()
# release config
release_conf = ReleaseConf()
# async_executor
async_manager = AsyncExecutorManager()
