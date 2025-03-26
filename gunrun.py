#!/usr/bin/env python3
# -*- coding: utf8 -*-
import os
# from gevent import monkey
#
# monkey.patch_all()

import multiprocessing
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

LOGGING_FOLDER = BASE_DIR / 'logs'

if not os.path.exists(LOGGING_FOLDER):
    os.makedirs(LOGGING_FOLDER)

debug = False
# 监听内网端口10030
bind = '0.0.0.0:50001'
# 监听队列
backlog = 2048
# 并行工作进程数
workers = 4
# workers = multiprocessing.cpu_count() * 2 + 1
# 每个进程的开启线程
threads = 2
# threads = multiprocessing.cpu_count() * 2
loglevel = 'debug'
accesslog = '-'

# pidfile = f'{BASE_DIR}/logs/gunicorn.pid'
# accesslog = f'{BASE_DIR}/logs/access.log'
# errorlog = f'{BASE_DIR}/logs/error.log'

# workers = multiprocessing.cpu_count() * 2 + 1
# threads = multiprocessing.cpu_count() * 2

# 设置守护进程,将进程交给supervisor管理
daemon = False
# 工作模式协程。使用gevent模式，还可以使用sync模式，默认的是sync模式
worker_class = 'uvicorn.workers.UvicornWorker'
# worker_connections最大客户端并发数量，默认情况下这个值为1000。此设置将影响gevent和eventlet工作模式
worker_connections = int(os.getenv('GUN_WORKER_CONNECTIONS') or 2000)
# 超时
timeout = int(os.getenv('GUN_TIMEOUT') or 180)

# 启动命令
# gunicorn -c $(pwd)/gun_config.py run:app