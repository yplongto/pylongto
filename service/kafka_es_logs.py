#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志存储服务 - 基于Kafka+Elasticsearch的分布式日志处理系统

功能特性：
1. 多进程消费 + 协程处理的混合并发模型
2. 自动索引管理（创建/映射更新）
3. 智能重试机制（指数退避策略）
4. 死信队列处理（失败消息持久化）
5. 完善的监控指标和错误处理
"""
import eventlet

eventlet.monkey_patch()

import os
import time
import json
import logging
import multiprocessing
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass

# 自定义模块
from modules.elasticsearch import ElasticSearch
from modules.kafka import KConsumer, KProducer

# 配置常量
DEFAULT_WORKERS = int(os.getenv("WORKERS", 1))
MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 1  # 初始重试延迟(秒)
DEAD_LETTER_TOPIC = "logs_dead_letter"
METRICS_INTERVAL = 60  # 监控指标输出间隔(秒)


@dataclass
class LogRecord:
    """日志数据模型"""
    index_name: str
    body: Dict[str, Any]
    mappings: Optional[Dict] = None


class LogStorageService:
    def __init__(self):
        # 服务组件初始化
        self.es = ElasticSearch()
        self.k_consumer = KConsumer()
        self.dlq_producer = KProducer()  # 死信队列生产者
        self.pool = eventlet.GreenPool(50)

        # 状态管理
        self._indices_cache = set()  # 索引缓存
        self._last_metrics_time = time.time()
        self._processed_count = 0
        self._failed_count = 0

        # 日志配置
        self.logger = logging.getLogger('kafka_es_logs')
        self._setup_logging()

    def _setup_logging(self):
        """配置日志格式"""
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def refresh_indices_cache(self):
        """刷新ES索引缓存"""
        try:
            self._indices_cache = set(self.es.get_alias().keys())
            self.logger.debug(f"Refreshed indices cache: {len(self._indices_cache)} indices")
        except Exception as e:
            self.logger.error(f"Failed to refresh indices cache: {e}")
            raise

    def start(self, topic: str, group_id: str = "log_storage_consumer"):
        """
        启动服务入口

        :param topic: 订阅的Kafka主题
        :param group_id: 订阅的Kafka消费组ID
        """
        self.logger.info(f"Starting LogStorageService with {DEFAULT_WORKERS} workers")

        if DEFAULT_WORKERS <= 1:
            self._start_single_process(topic, group_id)
        else:
            self._start_multi_process(topic, group_id)

    def _start_single_process(self, topic: str, group_id: str):
        """单进程模式"""
        self.processing_receive_message(topic, group_id)

    def _start_multi_process(self, topic: str, group_id: str):
        """多进程模式"""
        processes = []
        for _ in range(DEFAULT_WORKERS):
            p = multiprocessing.Process(
                target=self.processing_receive_message,
                args=(topic, group_id)
            )
            p.start()
            processes.append(p)

        try:
            for p in processes:
                p.join()
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal")
            for p in processes:
                p.terminate()

    def processing_receive_message(self, topic: str, group_id: str):
        """
        Kafka消息处理主循环

        :param topic: 订阅主题
        :param group_id: 消费者组ID
        """
        # 初始化时刷新索引缓存
        self.refresh_indices_cache()
        self.k_consumer.consumer_poll(
            callback_func=self.thread_save_log, topic=topic, group_id=group_id,
            timeout_ms=1000, max_records=1000
        )

    def thread_save_log(self, data: List[Dict]):
        """
        协程调度入口

        :param data: 原始消息数据
        """
        self.pool.spawn_n(self.process_batch, data)

    def process_batch(self, raw_batch: List[Dict]):
        """
        批量数据处理流程

        :param raw_batch: 原始数据批次
        """
        # 数据预处理
        records = self._parse_records(raw_batch)
        if not records:
            return

        # 索引预处理
        self._ensure_indices_exist(records)

        # 执行ES批量操作
        success = self._bulk_index_with_retry(records)

        # 更新监控指标
        self._update_metrics(success, len(records))

    def _parse_records(self, raw_data: List[Dict]) -> List[LogRecord]:
        """数据解析和验证"""
        valid_records = []
        for item in raw_data:
            try:
                record = LogRecord(
                    index_name=item["index_name"],
                    body=item["body"],
                    mappings=item.get("mappings")
                )
                if not isinstance(record.body, (dict, list)):
                    raise ValueError("Invalid body type")
                valid_records.append(record)
            except (KeyError, ValueError) as e:
                self._send_to_dlq(item, reason=str(e))
                self.logger.warning(f"Invalid record: {item} - Error: {e}")

        return valid_records

    def _ensure_indices_exist(self, records: List[LogRecord]):
        """确保所有需要的索引存在"""
        needed_indices = {r.index_name for r in records}
        missing_indices = needed_indices - self._indices_cache

        if not missing_indices:
            return

        # 批量创建缺失索引
        for index_name in missing_indices:
            # 查找该索引对应的mappings（取最后一个有mappings定义的记录）
            mappings = next(
                (r.mappings for r in records
                 if r.index_name == index_name and r.mappings is not None),
                None
            )

            try:
                self.es.create_index(index_name, mappings)
                self._indices_cache.add(index_name)
                self.logger.info(f"Created index: {index_name}")
            except Exception as e:
                self.logger.error(f"Failed to create index {index_name}: {e}")
                # 将该索引的记录标记为失败
                for r in records:
                    if r.index_name == index_name:
                        self._send_to_dlq(r.__dict__, reason=f"Index creation failed: {e}")

    def _bulk_index_with_retry(self, records: List[LogRecord]) -> bool:
        """
        带重试机制的批量索引

        :param records: 待索引记录
        :return: 是否全部成功
        """
        bulk_actions = []
        for record in records:
            docs = record.body if isinstance(record.body, list) else [record.body]
            for doc in docs:
                bulk_actions.append({"create": {"_index": record.index_name}})
                bulk_actions.append(doc)

        attempt = 0
        delay = INITIAL_RETRY_DELAY

        while attempt <= MAX_RETRIES:
            try:
                result = self.es.raw_bulk_data(index_name=None, body=bulk_actions)
                if result["errors"]:
                    self._handle_partial_failure(result, bulk_actions)
                return True
            except Exception as e:
                attempt += 1
                if attempt > MAX_RETRIES:
                    self.logger.error(f"Bulk operation failed after {MAX_RETRIES} attempts: {e}")
                    self._send_batch_to_dlq(records, reason=str(e))
                    return False
                self.logger.warning(f"Retry {attempt}/{MAX_RETRIES} after {delay:.1f}s for bulk error: {e}")

                time.sleep(delay)
                delay *= 2  # 指数退避

    def _handle_partial_failure(self, result: Dict, actions: List):
        """处理部分失败的批量操作"""
        for i, item in enumerate(result["items"]):
            if "error" in item.get("create", {}):
                error_info = {
                    "action": actions[i * 2],
                    "document": actions[i * 2 + 1],
                    "error": item["create"]["error"]
                }
                self._send_to_dlq(error_info, reason=error_info["error"]["reason"])
                self.logger.error(f"Failed to index document: {error_info}")

    def _send_to_dlq(self, data: Dict, reason: str):
        """发送到死信队列"""
        try:
            message = {
                "original": data,
                "timestamp": time.time(),
                "reason": reason,
                "metadata": {
                    "service": "log_storage",
                    "version": "1.0"
                }
            }
            dlq_conn = self.dlq_producer.create_kafka_connector(topic_name=DEAD_LETTER_TOPIC)
            dlq_conn.send_json_data(message)
            self.logger.info(f"Sent to DLQ: {reason}")
        except Exception as e:
            self.logger.error(f"Failed to send to DLQ: {e}")

    def _send_batch_to_dlq(self, records: List[LogRecord], reason: str):
        """批量发送到死信队列"""
        for record in records:
            self._send_to_dlq(record.__dict__, reason)

    def _update_metrics(self, success: bool, batch_size: int):
        """更新监控指标"""
        self._processed_count += batch_size
        if not success:
            self._failed_count += batch_size

        # 定期输出指标
        current_time = time.time()
        if current_time - self._last_metrics_time > METRICS_INTERVAL:
            self.logger.info(
                "Metrics: processed=%d, failed=%d, success_rate=%.2f%%",
                self._processed_count,
                self._failed_count,
                (self._processed_count - self._failed_count) / max(1, self._processed_count) * 100
            )
            self._last_metrics_time = current_time


if __name__ == '__main__':
    service = LogStorageService()
    try:
        service.start("sotc_longto_log_es", "sotc_log_storage_consumer")
    except KeyboardInterrupt:
        service.logger.info("Service stopped by user")
    except Exception as e:
        service.logger.error(f"Service crashed: {e}")
        raise
