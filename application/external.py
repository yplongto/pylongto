#!/usr/bin/env python3
# -*- coding: utf8 -*-
from confs import c
from engine.database.mongo.server import MongoManager
from engine.database.postgres.server import PostgresConn
from engine.database.redis.server import redis_client
from modules.kafka.kafka_producer import KProducer

postgres_conn = PostgresConn().from_uri(c.SQLALCHEMY_DATABASE_URI)

# Mongo
mongo_manager = MongoManager()
# Kafka
kafka_producer = KProducer()
