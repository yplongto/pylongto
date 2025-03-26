#!/usr/bin/env python3
# -*- coding: utf8 -*-
class DatabaseConnectionException(Exception):
    pass


class RaiseException(Exception):
    def __init__(self, code, /, *args, **kwargs):
        super().__init__(*args)
        self.code = code
