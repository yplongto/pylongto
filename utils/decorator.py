#!/usr/bin/env python3
# -*- coding: utf8 -*-
import functools
import logging
import inspect
import time
from collections import defaultdict
from functools import wraps
from random import random
from threading import Thread

import requests

from confs import async_manager
from env import SERVICE_ENV
from utils.exceptions import RaiseException
from utils.error_codes import CODEMAPPING, ERROR_INFO

logger = logging.getLogger('console')

timer_counts = defaultdict(int)


def get_class_that_defined_method(meth):
    if isinstance(meth, functools.partial):
        return get_class_that_defined_method(meth.func)
    if inspect.ismethod(meth) or (inspect.isbuiltin(meth) and getattr(meth, '__self__',
                                                                      None) is not None and getattr(
        meth.__self__, '__class__', None)):
        for cls in inspect.getmro(meth.__self__.__class__):
            if meth.__name__ in cls.__dict__:
                return cls
        meth = getattr(meth, '__func__', meth)  # fallback to __qualname__ parsing
    if inspect.isfunction(meth):
        cls = getattr(inspect.getmodule(meth),
                      meth.__qualname__.split('.<locals>', 1)[0].rsplit('.', 1)[0],
                      None)
        if isinstance(cls, type):
            return cls
    return getattr(meth, '__objclass__', None)  # handle special descriptor objects


def timeit_(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        st = time.time()
        ret = func(*args, **kwargs)
        dt = time.time() - st
        cls = get_class_that_defined_method(func)
        cls_name = f".{cls.__name__}" if cls else ""
        endpoint = '{}{}.{}'.format(func.__module__, cls_name, func.__name__)
        timer_counts[endpoint] += 1
        logger.info(
            '{}[{}] finished, exec {}s'.format(
                endpoint, '%05d' % timer_counts[endpoint], round(dt, 4)))
        return ret

    return wrapper  # 返回


class RESOURCETIMER:
    total_time = {}  # type: dict

    def __init__(self, tag='', enable_total=False, threshold_ms=0):
        self.st = None
        self.et = None
        self.tag = tag

        self.thr = threshold_ms
        self.enable_total = enable_total
        if self.enable_total:
            if self.tag not in self.total_time.keys():
                self.total_time[self.tag] = []

    def __enter__(self):

        self.st = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.et = time.time()
        dt = (self.et - self.st) * 1000
        if self.enable_total:
            self.total_time[self.tag].append(dt)

        if dt > self.thr:
            logger.info("{}: {}s".format(self.tag, round(dt / 1000, 4)))


def parse_e_message(e):
    """

    :param e:
    :retzurn:
    """
    result = {'code': 2400, 'data': [], 'message': CODEMAPPING['2400']}
    error_msg, error_type = str(e), str(type(e))
    try:
        error_type = error_type.replace("<class '", '').replace("'>", '')
    except:
        pass

    if e.__class__ is RaiseException:
        message = CODEMAPPING.get(str(e.code))
        if e.code in [-1, '-1']:
            message = e.args[0].get('message') if e.args and isinstance(e.args[0], dict) else e
        result = {'code': int(e.code), 'data': [], 'message': message}
    elif error_type in ERROR_INFO:
        error_code, extra_data = ERROR_INFO[error_type]
        result = {'code': error_code, 'data': [], 'message': extra_data}

    if SERVICE_ENV != 'pro':
        result["error_msg"] = str(e)
    return result


def system_error_decorator(func):
    """
    A decorator that handles both synchronous and asynchronous functions,
    providing error handling and a standardized return format.
    """

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        try:
            result = await func(*args, **kwargs)
        except Exception as e:
            if isinstance(e, RaiseException) and e.code in [2721, 2722, 2720, 2713, 2704]:
                pass
            else:
                logger.error('ASync --->:', exc_info=True)
            result = parse_e_message(e)
        return result

    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            # raise RaiseException(2500)
        except Exception as e:
            if isinstance(e, RaiseException) and e.code in [2721, 2722, 2720, 2713, 2704]:
                pass
            else:
                logger.error('Sync --->:', exc_info=True)
            result = parse_e_message(e)
        return result

    return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper


def requests_error_decorator(func):
    """
    接口请求保存提示(对返回接口进行处理)
    :param func:
    :return:
    """

    def wrapper(*args, **kwargs):
        data = {}
        max_retries = kwargs.get("max_retries", 3)  # 最大重试次数，默认为3
        retry_count = 0

        while retry_count <= max_retries:
            req_cnt = ", ".join(f"{key}: {value}" for key, value in
                                {"Service": kwargs.get('service_type'),
                                 "url": args[1],
                                 "method": kwargs.get('method'),
                                 "params": kwargs.get('params'),
                                 "data": kwargs.get('json'),
                                 "files": kwargs.get('files')
                                 }.items() if value)
            with RESOURCETIMER(req_cnt):
                try:
                    result = func(*args, **kwargs)
                    if result.status_code == 200:
                        response = result.json()
                        if kwargs.get("raw_result"):  # 不判断结果，直接返回
                            data = response
                        elif "code" in response and str(response["code"]) in ["200", "2000"]:
                            if response.get("data") is True:
                                data = True
                            elif "response" in response.get("data", {}):
                                data = response["data"]["response"]
                            else:
                                data = response["data"]
                        else:
                            error_msg = f"接口（url:{result.request.method} {args[1]}）响应结果：{response}"
                            raise Exception(str(error_msg))
                    elif result.status_code == 503:
                        logger.error(f"请求服务崩溃! 状态码: 503，接口：{result.url}")
                        break
                    else:
                        try:
                            error = result.json()
                        except:
                            error = result.reason
                        logger.error(f"请求服务接口失败! 状态码:{result.status_code},"
                                     f"接口：{result.request.method} {result.url}，错误信息：{error}")
                        break
                except requests.exceptions.ReadTimeout as e:
                    logger.error(f"接口请求超时 url:{args[1]}")
                except TypeError as e:
                    logger.error(f"传参类型错误:{args[1]} {e}")
                except Exception as e:
                    logger.error(f"接口请求错误：{args[1]} {e}")

                # 只有在发生可重试的错误时才会重试
                if retry_count < max_retries:
                    logger.info(f"重试 {retry_count + 1}/{max_retries}...")
                    retry_count += 1
                    time.sleep(random())
                else:
                    break

        return data

    return wrapper


def async_decorator(f):
    """
    异步装饰器
    :param f:
    :return:
    """

    def wrapper(*args, **kwargs):
        async_manager.executor.submit(f, *args, **kwargs)

    return wrapper
