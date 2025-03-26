#!/usr/bin/env python3
# -*- coding: utf8 -*-
import asyncio
import logging
import threading
# import traceback
from asyncio import current_task
from contextlib import contextmanager
from importlib.metadata import metadata
from urllib.parse import quote_plus

from sqlalchemy import create_engine, MetaData, select, Table, text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm.session import Session
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.ext.asyncio import (create_async_engine, async_sessionmaker,
                                    async_scoped_session, AsyncSession)

from confs import c

logger = logging.getLogger('cloud-postgres')

CONNECTION_MAPPING = {}


class PostgresConn(object):
    logger = logger

    def __init__(self, host=None, port=5432, user=None, password=None,
                 database=None, create_database_engine=False, *args, **kwargs):
        if create_database_engine:
            uri = self.__generate_uri(host, port, user, password, database)
            self.engine, self.session_factory, self.session = \
                self.create_engine_factory_session(uri, *args, **kwargs)

        self._lock = threading.Lock()
        self.__map_data = {}
        self.__metadata = {}

    def __repr__(self):
        return f"PostgresConn(url={self.engine.url.host}:{self.engine.url.port})"

    def init_app(self, app):
        self.engine, self.session_factory, self.session = \
            self.create_engine_factory_session(
                app.config.SQLALCHEMY_DATABASE_URI
            )

    def from_uri(self, uri, *args, **kwargs):
        engine, session_factory, session = self.create_engine_factory_session(
            uri, *args, **kwargs
        )
        self.engine = engine
        self.session_factory = session_factory
        self.session = session
        return self

    @contextmanager
    def context_session(self):
        """Provide a transactional scope around a series of operations."""
        session = self.session()
        try:
            yield session
        except:
            session.rollback()
            raise
        finally:
            session.close()

    def __generate_uri(self, host=None, port=None, user=None, password=None, database=None):
        engine_str_format = 'postgresql+psycopg://{username}:{password}@{host}:{port}/{db}?application_name={app_name}'
        engine_str = engine_str_format.format(
            username=user, password=quote_plus(password), host=host,
            port=port, db=database, app_name=f"PG_{c.SERVICE_NAME}"
        )
        return engine_str

    def create_engine_factory_session(self, uri, *args, **kwargs):
        """
        :param uri:
        :param args:
        :param kwargs:
        :return:
        """
        kwargs.update(c.SQLALCHEMY_ENGINE_OPTIONS)
        engine = create_engine(
            uri, future=True,
            max_overflow=c.SQLALCHEMY_MAX_OVERFLOW,
            pool_size=c.SQLALCHEMY_POOL_SIZE,
            pool_timeout=c.SQLALCHEMY_POOL_TIMEOUT,
            pool_recycle=c.SQLALCHEMY_POOL_RECYCLE,
            *args, **kwargs,
        )

        session_factory = sessionmaker(
            bind=engine, expire_on_commit=False, class_=Session
        )
        session = scoped_session(session_factory)
        return engine, session_factory, session

    def metadata_table(self, tablename, schema=None):
        if self.engine not in self.__metadata:
            with self._lock:
                if self.engine not in self.__metadata:
                    self.__metadata[self.engine] = MetaData()
                    self.__metadata[self.engine].reflect(
                        bind=self.engine, schema=schema)

                    self.__map_data[self.engine] = automap_base(
                        metadata=self.__metadata[self.engine])
                    self.__map_data[self.engine].prepare()

        if tablename not in self.__map_data[self.engine].classes.keys():
            with self._lock:
                table = Table(tablename, self.__metadata[self.engine],
                              autoload_with=self.engine, schema=schema)
                # Table Object
                return table

        return getattr(self.__map_data[self.engine].classes, tablename)

    def select(self, models, **config):
        """

        :param models:
        :param config:
        :return:
        """
        # 是否使用默认处理
        nop_ = None
        if 'nop_' in config:
            nop_ = config.pop('nop_')

        sql = select(models)
        for key, value in config.items():
            f = getattr(sql, key)
            if isinstance(value, dict):
                sql = f(**value)
            elif isinstance(value, (tuple, list)):
                sql = f(*value)
            else:
                sql = f(value)

        result = self.session.execute(sql)
        if nop_:
            return result

        __func = 'fetchall' if isinstance(result, CursorResult) else 'scalars'
        return getattr(result, __func)()

    def select_filter_by(self, models, **kwargs):
        return self.select(models, **{'filter_by': kwargs})

    def select_filter(self, models, *args):
        return self.select(models, **{'filter': args})

    def to_dict(self, data):
        return {c.name: getattr(self, c.name) for c in data.columns}
