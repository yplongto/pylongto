#!/usr/bin/env python3
# -*- coding: utf8 -*-
import logging.config
from io import StringIO

from env import LOGGING_CONFIG_FILENAME, LOGGING_FILEDIR, SERVICE_ENV

with open(LOGGING_CONFIG_FILENAME, 'r', encoding='utf8') as f:
    # 将路径中的反斜杠转义为双反斜杠
    base_dir_escaped = str(LOGGING_FILEDIR).replace('\\', r'\\')
    t = f.read().replace(r'{$BASE_DIR}', base_dir_escaped).replace(r'{$BASE_ENV}', SERVICE_ENV)

logging.config.fileConfig(StringIO(t), disable_existing_loggers=False)

# 修改uvicorn的日志格式
from uvicorn.config import LOGGING_CONFIG

fmt = "%(asctime)s [%(name)s %(filename)s:%(lineno)s] %(levelname)s - %(message)s"
LOGGING_CONFIG['formatters']['default']['fmt'] = fmt
LOGGING_CONFIG['formatters']['default']['datefmt'] = '%Y-%m-%d %H:%M:%S'
LOGGING_CONFIG['formatters']['access']['fmt'] = fmt
LOGGING_CONFIG['formatters']['access']['datefmt'] = '%Y-%m-%d %H:%M:%S'

logging.config.dictConfig(LOGGING_CONFIG)
logging.getLogger("watchfiles").setLevel(logging.ERROR)

"""
已存在的logger： main, jumpmark, forecast, crontab, alarm
"""
