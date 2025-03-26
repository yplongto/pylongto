#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""Celery分布式任务队列配置（生产/预发布环境隔离版本）"""

from datetime import timedelta
from celery.schedules import crontab

from env import SERVICE_ENV
from confs import c as config

# ------------------- 环境隔离配置 -------------------
# 生产环境使用独立Redis DB（0、7、8）
# 非生产环境使用测试DB（2、5、6），避免影响生产数据
__broker_db = 5  # 消息代理默认DB
__backend_db = 6  # 结果存储默认DB
if SERVICE_ENV == 'pro':
    __broker_db = 7
    __backend_db = 8

# ------------------- 核心配置 -------------------
# Redis连接配置（使用单实例模式）
broker_url = config.create_redis_uri(db=__broker_db)  # 消息代理地址
result_backend = config.create_redis_uri(db=__backend_db)  # 结果存储地址

# 序列化配置
task_serializer = 'json'  # 任务消息序列化格式（兼顾性能与可读性）
result_serializer = 'json'  # 任务结果序列化格式
accept_content = ['json', 'msgpack', 'pickle']  # 允许接收的内容类型白名单

# Worker配置
worker_concurrency = 14  # 并发worker数量（建议设置为CPU核心数+2）
timezone = 'Asia/Shanghai'  # 时区设置
result_expires = 60  # 任务结果保留时间（单位：秒）

# ------------------- 可靠性配置 -------------------
broker_connection_retry_on_startup = True  # 启动时自动重试broker连接
worker_cancel_long_running_tasks_on_connection_loss = True  # 网络中断时取消长任务
CELERY_FORCE_EXECV = True  # 防止死锁  默认处于开启状态
task_acks_late = True  # 延迟ACK确认（允许任务重试）
worker_max_tasks_per_child = 100  # 单个worker最大任务数（防止内存泄漏）
worker_hijack_root_logger = False
broker_transport_options = {  # 消息传输重试策略
    'max_retries': 3,  # 最大重试次数
    'interval_start': 0.2,  # 初始重试间隔
    'interval_step': 0.3,  # 重试间隔步长
    'interval_max': 1.0  # 最大重试间隔
}

# ------------------- 任务模块配置 -------------------
imports = (  # 需要自动发现的任务模块
    'celery_app.backend.control_test_task',
)

# ------------------- 队列路由配置 -------------------
task_queues = {
    # 数据采集控制队列（直连交换机模式）
    'backend:acquisition_control': {
        'exchange': 'backend.acquisition_control',
        'exchange_type': 'direct',  # 使用直接路由模式
        'binding_key': 'backend.aircon.acquisition_control'
    },
}

task_routes = {
    # 数据采集任务路由配置
    'celery_app.backend.control_test_task.distribution_task': {
        'queue': 'backend:acquisition_control',
        'routing_key': 'backend.aircon.acquisition_control'  # 精确路由键匹配
    },
}

# ------------------- 定时任务配置 -------------------
beat_schedule = {
    # 每日回风温度告警值同步任务
    'backend.aircon.sync_rtw_by_everyday': {
        'task': 'celery_app.backend.return_tp_warning_task.sync_rtw_by_everyday',
        'schedule': crontab(hour='1', minute='20'),  # 每天01:20执行
        'args': (1,),  # 固定参数
        'options': {  # 任务执行选项
            'routing_key': 'backend.r_tp_warning.everyday',  # 专用路由键
            'retry_policy': {
                'max_retries': 3,  # 失败后最大重试次数
            },
            'countdown': 1  # 延迟执行时间（秒）
        }
    },
}
