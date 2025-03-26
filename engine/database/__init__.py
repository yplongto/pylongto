#!/usr/bin/env python3
# -*- coding: utf8 -*-
try:
    from sqlalchemy.orm import declarative_base
except ImportError as e:
    from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Model(Base):
    """
        applying to others for help is not as good as applying to oneself.
    """
    __tablename__ = '__base__'
    __abstract__ = True
    __mapper_args__ = {"eager_defaults": True}
    __table_args__ = (

    )

    def __repr__(self):
        return f'<{self.__class__.__name__}>'

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
