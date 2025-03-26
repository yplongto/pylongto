#!/usr/bin/env python3
# -*- coding: utf8 -*-
import gzip
import math
import requests
import mimetypes
import traceback
from io import BytesIO
from ulid import ULID
from bson import ObjectId
from random import choice

try:
    # python 2.7
    from urllib import quote_plus
except ImportError as e:
    # Python 3
    from urllib.parse import quote_plus
from datetime import datetime, date
from fastapi import Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse

from utils.error_codes import CODEMAPPING
from utils.UA import USER_AGENTS

sess = requests.Session()
sess.headers.update({
    "User-Agent": choice(USER_AGENTS)
})


def format_datetime_to_string(date_time, format_str='%Y-%m-%d %H:%M:%S'):
    """
    转换时间类型为特定格式的字符串
    :param date_time:
    :param format_str:
    :return:
    """
    if not isinstance(date_time, datetime) or not isinstance(date_time, date):
        return date_time
    else:
        return datetime.strftime(date_time, format_str)


def return_date_time_no_ms(time_stamp):
    """
    时间格式去除微秒
    :param time_stamp:
    :return:
    """
    if isinstance(time_stamp, datetime):
        return datetime.strftime(time_stamp, '%Y-%m-%d %H:%M:%S')
    else:
        return time_stamp[0:19]


def return_objectid2string(value):
    if isinstance(value, ObjectId):
        return str(value)
    return value


def handle_special_floats(value):
    if isinstance(value, float):
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        elif math.isnan(value):
            return None
    return value


def handle_bytes(value):
    try:
        value = str(ULID.from_bytes(value))
    except Exception as e:
        pass
    return value


def service_json_response(code, data, columns=None, total=None, http_code=200, message='',
                          convert_json_response=True, *args, **kwargs):
    if isinstance(data, (JSONResponse, StreamingResponse, Response)):
        return data
    _message = message if code == -1 else CODEMAPPING[str(code)]
    result = {'code': code, 'data': data, 'message': _message}
    if total is not None:
        result['total'] = total
    if columns is not None:
        result['columns'] = columns
    if not convert_json_response:
        return result
    json_content = jsonable_encoder(result, custom_encoder={
        datetime: return_date_time_no_ms,
        ObjectId: return_objectid2string,
        float: handle_special_floats,
        bytes: handle_bytes
    })
    return JSONResponse(json_content, status_code=http_code)


def compress(content, compresslevel=6):
    gzip_buffer = BytesIO()
    if isinstance(content, BytesIO):
        content = content.read()
    with gzip.GzipFile(mode='wb',
                       compresslevel=compresslevel,
                       fileobj=gzip_buffer) as gzip_file:
        gzip_file.write(content)
    return gzip_buffer.getvalue()


def make_response_by_gzip(content, *args, **kwargs):
    compresslevel = kwargs.pop('compresslevel')
    gzip_content = compress(content, compresslevel=compresslevel)
    try:
        response = StreamingResponse(BytesIO(gzip_content))
    except Exception as e:
        response = Response(gzip_content)

    response.headers['Content-Length'] = str(len(gzip_content))
    response.headers['Content-Encoding'] = 'gzip'

    vary = response.headers.get('Vary')
    if not vary:
        response.headers['Vary'] = 'Accept-Encoding'
    elif 'accept-encoding' not in vary.lower():
        response.headers['Vary'] = '{}, Accept-Encoding'.format(vary)

    return response


def make_response_on_file_by_gzip(file_contents, filename, compresslevel=5):
    """
    :param file_contents:
    :param filename:
    :param compresslevel: 文件下载时建议不要压缩 设置为 0
    :return:
    """
    response = make_response_by_gzip(file_contents, compresslevel=compresslevel)
    mime_type = mimetypes.guess_type(filename)[0]
    response.headers['Content-Type'] = mime_type or 'application/octet-stream'
    response.headers['Access-Control-Expose-Headers'] = 'Content-Disposition'
    response.headers['Content-Disposition'] = (
        'attachment; filename={}'.format(
            quote_plus(filename, safe="/:@&+$,-_.!~*'()"))
    )
    return response


def download_file_to_bytesio(url, chunk_size=1024 * 1024, num_chunks=10):
    """
    文件下载
    :param url:
    :param chunk_size:
    :param num_chunks:
    :return:
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()
    byte_stream = BytesIO()
    for chunk in response.iter_content(chunk_size=chunk_size):
        if chunk:
            byte_stream.write(chunk)

    byte_stream.seek(0)
    return byte_stream


if __name__ == '__main__':
    import json

    # response = make_response_by_gzip(json.dumps({'1': [1, 2, 3]}).encode('utf8'), compresslevel=9)
    z = service_json_response(code=0, data={}, http_code=200)
    service_json_response(code=0, data=z, http_code=200)
