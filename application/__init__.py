#!/usr/bin/env python3
# -*- coding: utf8 -*-
from fastapi import FastAPI

from confs import c
from engine.database.redis.server import redis_client
from env import IS_DEV_ENV
from .external import postgres_conn

from models import Base


def create_table():
    Base.metadata.create_all(bind=postgres_conn.engine)


def pre_close_doc(app: FastAPI):
    if IS_DEV_ENV:
        return
    routes = app.router.routes
    for index, route in enumerate(routes):
        if route.path in ['/docs', '/redoc', '/docs/oauth2-redirect']:
            routes.pop(index)


def create_app():
    app = FastAPI()
    pre_close_doc(app)
    if not hasattr(app, 'config'):
        app.config = c
    c.init_app(app)
    redis_client.init_app(app)

    return app
