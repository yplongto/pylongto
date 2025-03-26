#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
pip install gssapi kafka-python krbticket

centos:
yum install krb5-workstation

Ubuntu:
sudo apt install krb5-user gcc libkrb5-dev

以上操作主要为了kinit、klist、kdestory、kpasswd命令
    kinit    : 用户登录。登录过程就是客户端从KDC获取票据TGT的过程。登录成功后，票据被缓存在本地。实际上就是建立了安全会话。
    klist    : 用户查看当前票据缓存中内容。
    kdestroy : 用于退出登录，即销毁缓存中票据。
    kpasswd  : 用于修改用户(主体)口令。

修改
/etc/hosts

135.161.97.177 beh-dn-01.bonc.com
135.161.97.178 beh-dn-02.bonc.com
135.161.97.179 beh-dn-03.bonc.com

135.161.97.158 beh-zk-01.bonc.com BEH-ZK-01
135.161.97.159 beh-zk-02.bonc.com BEH-ZK-02
...

将krb5.conf 放到/etc目录
"""
import six
import sys
import pickle
import time
import logging
import json
from typing import Optional

# fix python version >= 3.12.0 related "No module named 'kafka.vendor.six.moves'"
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer
from datetime import datetime, timedelta

from kafka.errors import KafkaTimeoutError, KafkaError
from krbticket import KrbConfig, KrbCommand

from confs import kafka_conf

logger = logging.getLogger("kafka-kerberos")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s %(filename)s:%(lineno)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


class KProducer(object):
    """Kafka生产者客户端，支持Kerberos认证和消息可靠传输"""

    logger = logger

    def __init__(self, bootstrap_servers=None, username=None, password=None, **kwargs):
        """
        初始化Kafka生产者配置
        :param bootstrap_servers: Kafka broker地址列表
        :param username: SASL认证用户名
        :param password: SASL认证密码
        :param kwargs: 扩展参数：
            - sasl_mechanism: 认证机制 (默认PLAIN)
            - security_protocol: 安全协议 (默认SASL_PLAINTEXT)
            - request_timeout_ms: 请求超时时间 (默认30000ms)
        """
        # 默认连接本地kafka_server的, 如果没开启会导致报错
        self.producer: Optional[KafkaProducer] = None
        self.k_config = None
        self.topic = None

        self.config = {
            'bootstrap_servers': bootstrap_servers or kafka_conf.bootstrap_servers,
            'sasl_mechanism': kafka_conf.sasl_mechanism or 'PLAIN',
            'security_protocol': kafka_conf.security_protocol or 'SASL_PLAINTEXT',
            'sasl_plain_username': username or kafka_conf.user,
            'sasl_plain_password': password or kafka_conf.password,
            'request_timeout_ms': kwargs.get('request_timeout_ms', 20000),
            'value_serializer': self._safe_serializer
        }
        if kwargs:
            self.config.update(**kwargs)

    def __del__(self):
        if self.producer:
            self.Destory()
        else:
            self.logger.debug('关闭连接')

    def Destory(self):
        self.producer.close(30)

    def _safe_serializer(self, raw_data):
        """安全序列化方法（增加异常处理）"""
        try:
            return json.dumps(raw_data).encode('utf-8')
        except json.JSONDecodeError:
            logger.error("JSON序列化失败，尝试Pickle解析")
            return pickle.dumps(raw_data)
        except Exception as e:
            logger.error(f"消息序列化失败: {str(e)}")
            return {"raw_data": raw_data, "error": str(e)}

    def _kerberos_auth(self, principal, keytab, kinit_bin="kinit", klist_bin="klist",
                       renewal_threshold=timedelta(minutes=30)):
        """执行Kerberos认证流程
        principal|keytab|jaas_file|krb5_file|kinit_bin|klist_bin|
        """
        self.k_config = KrbConfig(
            principal=principal, keytab=keytab, kinit_bin=kinit_bin,
            klist_bin=klist_bin, renewal_threshold=renewal_threshold
        )
        KrbCommand.kinit(self.k_config)
        self.logger.info(f"Kerberos auth success: {principal}")

    def connect(self, topic, **kerberos_params):
        """
        @summary:连接kafka.
        @param topic: 主题
        @return: 连接成功返回1，否则返回-1.
        """
        try:
            self.topic = topic
            if self.config['sasl_mechanism'] == 'GSSAPI':
                if not all([kerberos_params.get('principal'), kerberos_params.get('keytab')]):
                    raise ValueError('Kerberos authentication parameters are abnormal')
                self._kerberos_auth(**kerberos_params)

            self.producer = KafkaProducer(**self.config)
            self.logger.info(f"Connected to {self.config['bootstrap_servers']}")
            return True
        except KafkaError as e:
            self.logger.error(f"Connection failed: {str(e)}")
            return False

    def send_info(self, info):
        """
        @summary:发送字符串给kafka.
        @param info: 信息.
        @return: 连接成功返回1，否则返回-1.
        """
        try:
            future = self.producer.send(self.topic, info, partition=0)
            future.get(timeout=10)
            self.producer.flush()
            self.logger.debug('send string success!!!')
            return True
        except Exception as e:
            self.logger.info('send info error!{}'.format(e.message))
            return False

    def send_json_data(self, params, partition=None, timeout=30):
        """
        @summary:发送Json信息给kafka.
        @param params: 信息.
        @param partition: 分区
        @param timeout: 超时
        @return: 连接成功返回1，否则返回-1.
        """
        if not self.producer:
            logger.warning("Producer not initialized")
            return False
        try:
            if partition != None:
                result = self.producer.send(self.topic, params, partition=partition)
            else:
                result = self.producer.send(self.topic, params)
            result.get(timeout=timeout)
            # 在调用 future.get() 方法时，程序会被阻塞，直到异步操作完成，并返回异步操作的结果。如果异步操作在规定的超时时间内没有完成，则会抛出 TimeoutError 异常。
            result.add_callback(self.on_send_success)
            result.add_errback(self.on_send_error)

            self.producer.flush()
            self.logger.debug('send string success!!!')
            return True
        except Exception as e:
            self.logger.warning('send info error!', exc_info=True)
            return False

    def create_kafka_connector(self, topic_name):
        while True:
            try:
                logger.info(f'Create kafka connector Topic Name: {topic_name} ...')
                self.connect(topic_name)
                return self
            except Exception as e:
                logger.warning('failed to create KProducer', exc_info=True)
            time.sleep(5)

    def on_send_success(self, record_metadata):
        self.logger.info(
            f"Message sent to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

    def on_send_error(self, excp):
        self.logger.error(f"kafka failed to send message: {excp}")


if __name__ == '__main__':
    xxx = KProducer()
    result = []
    for i in range(100):
        result.append({'id': i, 'name': f'name-{i}'})

    conn = xxx.create_kafka_connector(topic_name='pylongto_common_test')
    conn.send_json_data(result)
