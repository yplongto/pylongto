#!/usr/bin/env python3
# -*- coding: utf8 -*-
import sys
import logging
from pathlib import Path

import celery
from celery import Celery, group
# celery不能用root用户启动问题, 解决方法：
from celery import platforms

logger = logging.getLogger('crontab')
platforms.C_FORCE_ROOT = True
CELERY_BASE_DIR = Path(__file__).resolve().parent

# sys.path = [CELERY_BASE_DIR.__str__()] + sys.path

from confs import celery_config

app = Celery('celery_worker')
app.config_from_object(celery_config)


class SignalTask(celery.Task):
    logger = logger

    def before_start(self, task_id, args, kwargs):
        ...

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        ...

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        ...

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        ...

    def on_success(self, retval, task_id, args, kwargs):
        ...


# 启动方式
"""
# Beat
celery -A celery_app beat -l info

# Worker
## 采集调控
celery -A celery_app worker -P eventlet -l info -Q backend:pylongto_task_test1, backend:pylongto_task_test2

"""
