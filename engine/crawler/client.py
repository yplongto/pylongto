#!/usr/bin/env python3
# -*- coding: utf8 -*-
import sys
import time
import requests
import logging
import logging.config

from random import choice, random
from typing import Optional, Tuple, Any, Dict

from utils.UA import USER_AGENTS
from utils.decorators import requests_error_decorator

LOGGING_CONFIG_DEFAULTS: Dict[str, Any] = dict(
    version=1,
    disable_existing_loggers=False,
    formatters={
        "crawler": {
            "level": "INFO",
            "handlers": ["crawler_console"],
            "propagate": False,  # 日志记录是否向上传播到他的父节点，default=True
            "qualname": "crawler"
        },
    },
    handlers={
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "generic",
            "stream": sys.stdout,
        },
    },
    loggers={
        "generic": {
            "format": "%(asctime)s [%(name)s %(filename)s:%(lineno)s] %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
            "class": "logging.Formatter",
        },
    },
)


class CrawlerBase(object):

    def __init__(self, *args, **kwargs):
        self.sess = requests.Session()
        self.sess.headers.update({'User-Agent': choice(USER_AGENTS)})
        time.sleep(random())  # 为了避免过于频繁地访问目标网站，从而降低被封禁的风险

        if 'crawler' not in logging.Logger.manager.loggerDict:
            logging.config.dictConfig(LOGGING_CONFIG_DEFAULTS)

        self.logger = logging.getLogger('crawler')

    def update_headers(self, headers):
        self.sess.headers.update(headers)

    def raw_get(self, url, *args, **kwargs) -> Optional[Tuple[requests.Response, str]]:
        resp = self.sess.get(url, *args, **kwargs)
        html = resp.text
        return resp, html

    def raw_post(self, url, data=None, json=None, **kwargs) -> Optional[Tuple[requests.Response, str]]:
        resp = self.sess.post(url, data=data, json=json, **kwargs)
        html = resp.text
        return resp, html

    def raw_request(self, method, url, **kwargs) -> Optional[Tuple[requests.Response, str]]:
        resp = self.sess.request(method=method, url=url, **kwargs)
        html = resp.text
        return resp, html

    def __del__(self):
        self.sess.close()

    def is_https(self, url):
        return url.lower().startswith("https://")

    @property
    def headers(self):
        try:
            return {
                "Connection": "close",
                # "Authorization": self.token or request.headers.get("Authorization")
            }

        except Exception as e:
            return {
                "Connection": "close",
            }

    @requests_error_decorator
    def get(self, url, params, timeout=None, **kwargs):
        # 正式、预正式环境，request请求https取消认证
        verify = False if self.is_https(url) else True
        result = self.sess.get(url, params=params, headers=self.headers, timeout=timeout, verify=verify)
        return result

    @requests_error_decorator
    def put(self, url, data, params=None, timeout=None, **kwargs):
        verify = False if self.is_https(url) else True
        result = self.sess.put(url, json=data, params=params, headers=self.headers, timeout=timeout, verify=verify)
        return result

    @requests_error_decorator
    def post(self, url, data=None, files=None, timeout=None, **kwargs):
        verify = False if self.is_https(url) else True
        result = self.sess.post(url, json=data, files=files, headers=self.headers, timeout=timeout, verify=verify)
        return result

    @requests_error_decorator
    def delete(self, url, params=None, data=None, timeout=None, **kwargs):
        verify = False if self.is_https(url) else True
        result = self.sess.delete(url, params=params, json=data, headers=self.headers, timeout=timeout, verify=verify)
        return result
