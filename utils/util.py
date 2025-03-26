#!/usr/bin/env python3
# -*- coding: utf8 -*-
from datetime import datetime

import pytz
from bson import CodecOptions
from ulid import ULID

from utils.exceptions import DatabaseConnectionException, RaiseException


def ulid_bytes_convert_string(value):
    if not value:
        return ''
    if not isinstance(value, bytes):
        return value
    return str(ULID.from_bytes(value))


def get_zone_now_time(zone='Asia/Shanghai', enable_zone=False):
    tzinfo = pytz.timezone(zone)
    return datetime.now(tz=tzinfo) if enable_zone else datetime.now()


def get_collection(database, collection_str, use_tzinfo=True):
    if database is None:
        raise DatabaseConnectionException('Database Engine is None.')

    tzinfo = pytz.timezone('Asia/Shanghai')
    options = CodecOptions(tz_aware=True, tzinfo=tzinfo)
    if not use_tzinfo:
        options = None
    collection = database.get_collection(collection_str, codec_options=options)
    return collection
