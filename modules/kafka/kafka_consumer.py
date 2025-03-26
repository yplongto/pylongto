#!/usr/bin/env python3
# -*- coding: utf8 -*-
import six
import sys
import json
import logging
import pickle
import socket
import time
from datetime import datetime

# fix python version >= 3.12.0 related "No module named 'kafka.vendor.six.moves'"
if sys.version_info >= (3, 12, 0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, KafkaConnectionError

from confs import kafka_conf
from utils.util import get_zone_now_time

logger = logging.getLogger('kafka_consumer')


class KConsumer(object):
    """Kafka消费者客户端封装类

    功能特性：
    - 支持SASL/PLAIN认证
    - 支持自动/手动提交偏移量
    - 支持指定偏移量消费
    - 支持JSON消息反序列化
    - 内置消费异常处理和重连机制
    """

    def __init__(self, bootstrap_servers=None, username=None, password=None):
        """初始化消费者配置

        :param bootstrap_servers: Kafka集群地址列表，格式如['host1:port1', 'host2:port2']
        :param username: SASL认证用户名
        :param password: SASL认证密码
        """
        # 增强配置类型检查
        self.bootstrap_servers = bootstrap_servers or kafka_conf.bootstrap_servers
        if not isinstance(self.bootstrap_servers, list):
            raise TypeError("bootstrap_servers must be a list of 'host:port' strings")

        self.username = username or kafka_conf.user
        self.password = password or kafka_conf.password
        self.sasl_mechanism = kafka_conf.sasl_mechanism
        self.security_protocol = kafka_conf.security_protocol or 'PLAINTEXT'
        self.consumer = None
        self._retry_count = 0  # 重连计数器

    def connect(self, topics, group_id='sotc_sdhjk_delay_alarms', offset_reset='latest', enable_auto_commit=False):
        """建立Kafka消费者连接

        :param topics: 订阅主题列表
        :param group_id: 消费者组ID
        :param offset_reset: 无偏移量时的重置策略（'earliest'/'latest'）
        :param enable_auto_commit: 是否自动提交偏移量
        :raises KafkaConnectionError: 连接失败时抛出
        """
        try:
            self.consumer = KafkaConsumer(
                topics,
                group_id=group_id,
                api_version=(0, 10, 2),
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset=offset_reset,
                enable_auto_commit=enable_auto_commit,
                max_poll_interval_ms=300000,
                max_poll_records=30,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.username,
                sasl_plain_password=self.password,
                value_deserializer=self._safe_deserializer  # 使用安全反序列化方法
            )
            # 验证连接状态
            if not self.consumer.bootstrap_connected():
                raise KafkaError("Failed to bootstrap connect")
        except KafkaError as ke:
            logger.error(f"Kafka连接失败: {ke}")
            raise KafkaConnectionError(f"无法连接到Kafka集群: {self.bootstrap_servers}") from ke
        except Exception as e:
            logger.error(f"未知连接错误: {e}")
            raise

    def _safe_deserializer(self, raw_data):
        """安全反序列化方法（增加异常处理）"""
        try:
            return json.loads(raw_data.decode('utf-8'))
        except json.JSONDecodeError:
            logger.error("JSON反序列化失败，尝试Pickle解析")
            return pickle.loads(raw_data)
        except Exception as e:
            logger.error(f"消息反序列化失败: {e}")
            return {"raw_data": raw_data, "error": str(e)}

    def consumer_offset(self, topic, offset=None):
        """
        指定偏移量消费数据（不会影响到正常消费数据的偏移量）
        :param topic: kafka主题
        :param offset: 当偏移量小于最小偏移量，默认读取最新数据；当偏移量大于最大偏移量，会报错
        :return:
        """
        partitions_for_topic = self.consumer.partitions_for_topic(topic)  # 分区集合
        logger.info(f"分区信息:{partitions_for_topic}, {self.consumer.subscription()}")
        topic_partitions = [TopicPartition(topic, _) for _ in partitions_for_topic]
        # 分配主题
        self.consumer.assign(topic_partitions)
        # 获取当前消费者topic、分区信息
        # print(self.consumer.assignment())
        # 获取每个分区最新的偏移量
        logger.info([({_}, self.consumer.position(_)) for _ in topic_partitions])
        if offset:
            # 指定偏移量量
            for topic_partition in topic_partitions:
                self.consumer.seek(topic_partition, offset)

        # 从指定的偏移量开始读取数据
        for message in self.consumer:
            try:
                value = pickle.loads(message.value)
            except:
                value = json.loads(message.value)
            dt = datetime.fromtimestamp(message.timestamp // 1000)
            logger.info(message.partition, message.offset, dt, value)

            if message.offset % 100000 == 0:
                dt = datetime.fromtimestamp(message.timestamp // 1000)
                logger.info(message.partition, message.offset, dt)

    def consumer_poll(self, callback_func, topic, group_id=None, timeout_ms=100, max_records=None,
                      enable_auto_commit=False, offset_log=None, is_write_time=False, *args, **kwargs):
        """轮询消费消息主循环

        :param callback_func: 消息处理回调函数
        :param topic: 消费主题
        :param group_id: 消费组
        :param timeout_ms: 轮询超时时间（毫秒）
        :param max_records: 单次最大拉取记录数
        :param enable_auto_commit: 是否自动提交
        :param offset_log: 偏移量日志间隔
        :param is_write_time: 是否记录写入时间
        """
        self.connect(topic, group_id, enable_auto_commit=enable_auto_commit)
        if self.consumer.bootstrap_connected():
            logger.error(
                f"消费者已成功连接到Kafka集群 {self.consumer.config['bootstrap_servers']} {group_id} {self.consumer.subscription()}")
        else:
            logger.error(f"消费者未能连接到Kafka集群 {self.consumer.config['bootstrap_servers']}")
        while True:
            try:
                consumer_records_dict = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
                data_list = []
                for key, records in consumer_records_dict.items():
                    for msg in records:
                        try:
                            data = pickle.loads(msg.value)
                            if isinstance(data, list):
                                data_list.extend(data)
                            else:
                                data_list.append(data)
                            if offset_log and msg.offset % offset_log == 0:
                                dt = datetime.fromtimestamp(msg.timestamp // 1000)
                                logger.error(
                                    f"{topic} 推送时间:{dt} 当前时间:{get_zone_now_time()} 分区:{msg.partition} 偏移量:{msg.offset}")
                            if is_write_time:
                                # 写入kafka时间
                                kwargs["kafka_write_time"] = datetime.fromtimestamp(msg.timestamp // 1000)
                        except Exception as e:
                            logger.error(e)
                            continue
                if data_list:
                    try:
                        callback_func(data_list, *args, **kwargs)
                    except Exception as e:
                        logger.error(e)
                if not enable_auto_commit:
                    try:
                        # 轮询一个batch 手动提交一次
                        if consumer_records_dict:
                            self.consumer.commit_async(callback=self._on_send_response)

                    except Exception as e:
                        logger.warning(e)
                        self.consumer.commit()

            except socket.error as e:
                logger.error(e)
                self.consumer.close()  # 关闭消费者
                time.sleep(2)  # 等待2秒钟

                try:
                    self.consumer.subscribe([topic])  # 重新订阅主题
                except KafkaError as e:
                    logger.error(e)

            except KafkaError as e:
                logger.error(e)
                time.sleep(2)  # 等待2秒钟

            except Exception as e:
                logger.error(e)
                time.sleep(1)  # 等待2秒钟

    def _on_send_response(self, *args, **kwargs):
        """
        提交偏移量涉及的回调函数
        :param args:
        :param kwargs:
        :return:
        """
        # 获取消费者组信息
        group_id = self.consumer.config.get('group_id', 'unknown_group')

        if isinstance(args[1], Exception):
            logger.error(f'{group_id} 异步偏移量提交异常:{args[1]}')
            try:
                self.consumer.commit()
                logger.info(f'{group_id} 异步提交失败后成功执行同步提交')
            except Exception as sync_ex:
                logger.error(f'{group_id} 同步偏移量提交失败: {sync_ex}')

    def _on_send_response_log(self, offsets, response):
        """
        提交偏移量涉及的回调函数（增强日志记录）

        :param offsets: 提交的偏移量字典 {TopicPartition: OffsetAndMetadata}
        :param response: 响应对象或异常
        """
        # 获取消费者组信息
        group_id = self.consumer.config.get('group_id', 'unknown_group')

        if isinstance(response, Exception):
            # 错误日志（带消费者组和分区详情）
            error_msg = (
                f"消费者组 [{group_id}] 异步提交失败 | "
                f"异常类型: {type(response).__name__} | "
                f"错误详情: {str(response)} | "
                f"影响分区: {list(offsets.keys())}"
            )
            logger.error(error_msg)

            try:
                # 同步提交重试
                self.consumer.commit(offsets)
                logger.info(
                    f"消费者组 [{group_id}] 同步提交成功 | "
                    f"提交偏移量: { {tp: om.offset for tp, om in offsets.items()} }"
                )
            except Exception as sync_ex:
                # 记录完整错误堆栈
                logger.exception(
                    f"消费者组 [{group_id}] 同步提交失败 | "
                    f"错误详情: {str(sync_ex)} | "
                    f"最后尝试偏移量: { {tp: self.consumer.position(tp) for tp in offsets} }"
                )
        else:
            # 成功日志（带提交元数据）
            success_msg = (
                f"消费者组 [{group_id}] 异步提交成功 | "
                f"提交时间: {datetime.utcnow().isoformat()}Z | "
                f"分区偏移量: { {tp.topic + ':' + str(tp.partition): om.offset for tp, om in offsets.items()} }"
            )
            logger.debug(success_msg)
            print(f"[Kafka监控] {success_msg}")  # 控制台输出简化信息

    def get_json_data(self, topic, group_id):
        """
        consumer json messages
        :param topic:
        :param group_id:
        :return:
        """
        self.connect(topic, group_id)
        for msg in self.consumer:
            print(msg.value)

    def consume_json_message_by_topic(self, topics, group_id=None, enable_auto_commit=True):
        """获取主题列表【自动提交】，并订阅消费主题的细节"""
        self.consumer = KafkaConsumer(
            api_version=(0, 10, 2),
            group_id=group_id,
            bootstrap_servers=self.bootstrap_servers,
            # auto_offset_reset='earliest',
            enable_auto_commit=enable_auto_commit,
            auto_commit_interval_ms=1000,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.username,
            sasl_plain_password=self.password,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        # 获取主题列表
        print(self.consumer.topics())
        # 获取当前消费者订阅的主题
        print(self.consumer.subscription())
        # 获取当前消费者topic、分区信息
        print(self.consumer.assignment())

        # 订阅要消费的主题
        self.consumer.subscribe(topics=topics)
        for message in self.consumer:
            print(f"{message.topic}:{message.partition}:{message.offset}: "
                  f"key={message.key} value={message.value}")


if __name__ == '__main__':
    xxx = KConsumer()
    topics = ['pylongto_common_test']
    xxx.consume_json_message_by_topic(topics=topics)
