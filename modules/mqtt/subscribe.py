#!/usr/bin/env python3
# -*- coding: utf8 -*-
import json
import time
from typing import Callable, List, Union

from confs import mqtt_conf
from modules.mqtt import MQTTBase


class MQTTSubscriber(MQTTBase):
    """优化后的MQTT订阅客户端，支持集群订阅和消息处理"""

    def __init__(self, message_handler: Callable = None):
        super().__init__(mqtt_conf, 'cloud-mqtt-subscribe')
        self.message_handler = message_handler or self.default_message_handler
        self.connect()

    def _setup_client_callbacks(self, client):
        """设置客户端回调函数"""
        client.on_message = self._on_message
        client.user_data_set({
            "handler": self.message_handler,
            "logger": self.logger
        })

    def _on_message(self, client, userdata, msg):
        """统一消息回调处理"""
        try:
            payload = msg.payload.decode()
            message = json.loads(payload)
            userdata["handler"](msg.topic, message)
        except Exception as e:
            userdata["logger"].error(f"消息处理失败: {str(e)}", exc_info=True)

    @staticmethod
    def default_message_handler(topic: str, message: dict):
        """默认消息处理器"""
        print(f"Received message from {topic}: {json.dumps(message, indent=2)}")

    def subscribe(self, topics: Union[str, List[str]], qos: int = 0):
        """
        订阅指定主题
        :param topics: 主题或主题列表
        :param qos: 服务质量等级
        """
        if not isinstance(topics, list):
            topics = [topics]

        for client in self.clients:
            for topic in topics:
                shared_topic = f"$share/group/{topic}" if len(self.clients) > 1 else topic
                client.subscribe(shared_topic, qos)
                self.logger.debug(f"已订阅主题: {shared_topic}")

    def batch_subscribe(self, topics: List[str], batch_size: int = 1000):
        """
        批量订阅主题（支持大数量级主题订阅）
        :param topics: 主题列表
        :param batch_size: 每批处理数量
        """
        for i in range(0, len(topics), batch_size):
            batch = topics[i:i + batch_size]
            self.subscribe(batch)

    def start_listening(self):
        """启动持续监听"""
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("订阅服务已停止")
        finally:
            self.close()


if __name__ == '__main__':
    # 使用示例
    def custom_handler(topic, message):
        print(f"[自定义处理] {topic}: {message['value']}")


    subscriber = MQTTSubscriber(message_handler=custom_handler)

    # 订阅单个主题
    subscriber.subscribe("realtimedata/get/#")

    # 批量订阅示例
    # topics = [f"device/{i}/data" for i in range(1500)]
    # subscriber.batch_subscribe(topics)

    subscriber.start_listening()
