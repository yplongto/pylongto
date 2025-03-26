#!/usr/bin/env python3
# -*- coding: utf8 -*-
import json
import logging
import threading
import time
from paho.mqtt import client as mqtt_client


class MQTTBase:
    """MQTT客户端基类，封装通用功能"""

    def __init__(self, config_module, logger_name):
        # 初始化配置
        self._load_config(config_module)
        self.logger = logging.getLogger(logger_name)
        self.clients = []
        self.lock = threading.Lock()
        self.current_client_index = 0
        self.retries = 0

        # 重连参数
        self.reconnect_delay = 2
        self.max_reconnect_delay = 120

    def _load_config(self, config):
        """加载MQTT配置"""
        try:
            self.accounts = json.loads(config.services)
            self.single_host = config.host
            self.single_port = int(config.port)
            self.single_user = config.user
            self.single_pass = config.password
        except Exception as e:
            self.logger.error(f"配置加载失败: {str(e)}")
            self.accounts = []

    def _create_client(self, account=None):
        """创建客户端实例的工厂方法"""
        client = mqtt_client.Client()
        if account:  # 集群模式
            client.username_pw_set(account["user"], account["password"])
            host, port = account["host"], account["port"]
        else:  # 单机模式
            client.username_pw_set(self.single_user, self.single_pass)
            host, port = self.single_host, self.single_port

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        return client, host, port

    def connect(self):
        """建立MQTT连接"""
        targets = self.accounts if self.accounts else [None]

        for target in targets:
            client, host, port = self._create_client(target)
            try:
                client.connect(host, port)
                client.loop_start()
                self.clients.append(client)
                self.logger.info(f"成功连接到 {host}:{port}")
            except Exception as e:
                host_info = target["host"] if target else f"{self.single_host}:{self.single_port}"
                self.logger.error(f"连接失败 {host_info}: {str(e)}")

    def _on_connect(self, client, userdata, flags, rc):
        """连接回调基础实现"""
        if rc == 0:
            self.retries = 0
        else:
            self.logger.error(f"连接失败，错误码: {rc}")
            self._retry_connection(client)

    def _on_disconnect(self, client, userdata, rc):
        """断开回调基础实现"""
        if rc != 0:
            self.logger.warning("连接异常断开，尝试重连...")
            self._retry_connection(client)

    def _retry_connection(self, client):
        """指数退避重连机制"""
        with self.lock:
            delay = min(self.max_reconnect_delay,
                        self.reconnect_delay * 2 ** self.retries)
            self.retries += 1

        time.sleep(delay)
        try:
            client.reconnect()
            with self.lock:
                self.retries = 0
        except Exception as e:
            self.logger.error(f"重连失败: {str(e)}")

    def get_available_client(self):
        """轮询获取可用客户端"""
        with self.lock:
            for _ in range(len(self.clients)):
                client = self.clients[self.current_client_index]
                self.current_client_index = (self.current_client_index + 1) % len(self.clients)
                if client.is_connected():
                    return client
        return None
