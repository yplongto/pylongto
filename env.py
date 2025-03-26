#!/usr/bin/env python3
# -*- coding: utf8 -*-
from pathlib import Path
import os
from dotenv import load_dotenv, find_dotenv

# Base Dir
BASE_DIR = Path(__file__).resolve().parent

# 优先级 系统自定义(export) > .env
# 自动寻找.env文件
load_dotenv(find_dotenv(), verbose=True, override=False, encoding='utf8')

LEVEL_DB_DATA_DIR = BASE_DIR / 'leveldata/'

if not os.path.exists(LEVEL_DB_DATA_DIR):
    os.mkdir(LEVEL_DB_DATA_DIR)

LOGGING_FILEDIR = BASE_DIR / 'logs'

if not os.path.exists(LOGGING_FILEDIR):
    os.makedirs(LOGGING_FILEDIR)

# 获取当前使用的环境
SERVICE_ENV = os.getenv('SERVICE_ENV', 'dev').lower()

# 检测是否为预正式、正式环境
IS_DEV_ENV = not (SERVICE_ENV in ['pre', 'pro'])

LOG_LEVEL = "info" if IS_DEV_ENV else "error"

LOGGING_CONFIG_FILENAME = BASE_DIR / f'confs/logs/logging.conf'

SERVICE_CONFIG_FILENAME = BASE_DIR / f'confs' / f'{SERVICE_ENV}.yaml'
