#!/usr/bin/env python3
# -*- coding: utf8 -*-
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger("async_executor")


class AsyncExecutorManager(object):
    """异步任务管理器"""

    def __init__(self, max_workers=30):
        self.max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.Lock()

    @property
    def executor(self):
        try:
            with self._lock:
                if getattr(self._executor, '_shutdown', False):
                    self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        except Exception as e:
            logger.error(e)
            with self._lock:
                self._executor = ThreadPoolExecutor(max_workers=self.max_workers)
        return self._executor

    def shutdown(self):
        """显式关闭当前executor"""
        with self._lock:
            if self._executor:
                self._executor.shutdown(wait=True)
