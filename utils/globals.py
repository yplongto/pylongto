#!/usr/bin/env python3
# -*- coding: utf8 -*-

import threading
from contextvars import ContextVar


class RequestProvider(object):
    __instance = None
    __instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            with cls.__instance_lock:
                if not hasattr(cls, '_instance'):
                    cls.__instance = object.__new__(cls)
        return cls.__instance

    def __init__(self):
        self._request = ContextVar('fastapi.request_ctx')

    @property
    def request(self):
        return self._request.get()

    @request.setter
    def request(self, value):
        self._request.get(value)


request_provider = RequestProvider()
