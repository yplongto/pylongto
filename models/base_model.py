#!/usr/bin/env python3
# -*- coding: utf8 -*-
from ulid import ULID
from datetime import datetime
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Float, DateTime, func
from sqlalchemy.ext.hybrid import hybrid_property

from engine.database import Model
from utils.util import ulid_bytes_convert_string


class BaseModel(Model):
    __tablename__ = '__baseModel__'
    __abstract__ = True

    def __repr__(self):
        return f'<{self.__class__.__name__} ID={self.id}>'

    @classmethod
    def get_filters(cls, **kwargs):
        filters = []
        for field, value in kwargs.items():
            filters.append(getattr(cls, field) == value)
        return filters


class BaseClass(object):
    id = Column(Integer, autoincrement=True, primary_key=True, index=True, comment='自增主键ID')
    is_active = Column(Boolean, default=True, comment='是否活跃')
    create_time = Column(DateTime, default=datetime.now, nullable=False, comment='创建时间')
    update_time = Column(DateTime, default=datetime.now, nullable=False,
                         onupdate=datetime.now, comment='更新时间')


class BaseAreaClass(object):
    province_id = Column(String(36), nullable=False, comment='省ID')
    province_name = Column(String(100), comment='省份名称')
    city_id = Column(String(36), nullable=False, comment='地市ID')
    city_name = Column(String(100), comment='地市名称')
    county_id = Column(String(36), nullable=False, comment='区县ID')
    county_name = Column(String(100), comment='区县名称')
    station_id = Column(String(36), nullable=False, comment='局站ID')
    station_name = Column(String(100), comment='局站名称')
    room_id = Column(Integer, nullable=False, index=True, comment='机房ID')
    room_name = Column(String(100), nullable=True, comment='机房名称')


class BaseClassMixin(object):

    @hybrid_property
    def operator_id(self):
        return ulid_bytes_convert_string(self.update_user_id)
        # return str(ULID.from_bytes(self.update_user_id))

    @operator_id.setter
    def operator_id(self, value):
        if not isinstance(value, bytes):
            raise ValueError('Value must be ULID bytes')
        self.update_user_id = value

    @operator_id.expression
    def operator_id(cls):
        return cls.update_user_id
