#!/usr/bin/env python3
# -*- coding: utf8 -*-
from pymongo import MongoClient
# from bson.codec_options import CodecOptions

from confs import mongo_conf, c
from utils.mapping import MAX_EXPORT_TOTAL
from utils.util import get_collection
from utils.exceptions import DatabaseConnectionException


class MongoManager(object):

    def __init__(self):
        try:
            self.db = self.get_db_manager()
        except Exception as e:
            self.db = None

    def get_db_manager(self, config_name=f'Mongo_{c.SERVICE_NAME}'):
        uri = mongo_conf.create_mongo_uri(config_name=config_name)
        client = MongoClient(uri, connect=False)
        database = mongo_conf.get_mongo_database(config_name=config_name)

    def get_collection(self, collection_str, db_engine=None, use_tzinfo=True):
        """
        获取 collection
        :param collection_str:
        :param db_engine:
        :param use_tzinfo:
        :return:
        """
        database = db_engine or self.db
        collection = get_collection(
            database=database, collection_str=collection_str, use_tzinfo=use_tzinfo
        )
        return collection

    def find_all_conf(self, collection_str, query, projection=None,
                      sort_=None, offset_size=0, page_size=20, db_engine=None):
        """
        获取指定表的数据
        :param collection_str:
        :param query:
        :param projection:
        :param sort_:
        :param offset_size:
        :param page_size:
        :param db_engine:
        :return:
        """

        collection = self.get_collection(collection_str, db_engine=db_engine)
        count = collection.count_documents(query)
        total = count if count else 0

        offset_size = -1 if offset_size < 0 else int(offset_size)
        if not sort_:
            sort_ = [('_id', -1)]
        query = collection.find(query, projection).sort(sort_)
        if offset_size < 0:
            result = list(query.limit(MAX_EXPORT_TOTAL))
        else:
            result = list(query.skip(offset_size).limit(page_size))
        return total, result

    def find_all(self, collection_str, query, offset=None, limit=None, values=None, sort_=None):
        """
        获取指定表排序后的数据列表，支持分页+总数
        :param collection_str:
        :param query:
        :param offset:
        :param limit:
        :param values:
        :param sort_:
        :return:
        """
        if not sort_:
            sort_ = [('_id', -1)]
        if offset is not None and limit is not None:
            total = self.find_count(collection_str, query) or 0
            if values:
                res = list(self.find_values_skip_limit(collection_str, query, sort_, offset, limit, values))
            else:
                res = list(self.find_sort_skip_limit(collection_str, query, sort_, offset, limit))
            return res, total
        else:
            if values:
                res = list(self.find_values(collection_str, query, sort_, values))
            else:
                res = list(self.find_sort(collection_str, query, sort_))
            return res

    def find_select(self, collection_str, json_str, select_str):
        collection = self.get_collection(collection_str)
        return collection.find(json_str, select_str)

    def find_sort(self, collection_str, json_str, sort_str):
        collection = self.get_collection(collection_str)
        return collection.find(json_str).sort(sort_str)

    def find_one(self, collection_str, json_str):
        collection = self.get_collection(collection_str)
        return collection.find_one(json_str)

    def find_values(self, collection_str, json_str, sort_str, value_str):
        collection = self.get_collection(collection_str)
        return collection.find(json_str, value_str).sort(sort_str)

    def find_count(self, collection_str, json_str):
        collection = self.get_collection(collection_str)
        return collection.count_documents(json_str)

    def find_sort_skip_limit(self, collection_str, json_str, sort_str, skip, limit):
        collection = self.get_collection(collection_str)
        return collection.find(json_str).sort(sort_str).skip(skip).limit(limit)

    def find_values_skip_limit(self, collection_str, json_str, sort_str, skip, limit, value_str):
        collection = self.get_collection(collection_str)
        return collection.find(json_str, value_str).sort(sort_str).skip(skip).limit(limit)

    def aggregate(self, collection_str, pipeline, **options):
        collection = self.get_collection(collection_str)
        return collection.aggregate(pipeline, **options)

    def update_one(self, collection_str, find_json_str, update_json_str, upsert=False):
        # 新增 upsert 字段，可以在查询的时候，判断有无，然后硬更新。
        collection = self.get_collection(collection_str)
        return collection.update_one(find_json_str, update_json_str, upsert)

    def find_one_and_update(self, collection_str, find_json_str, update_json_str, kwargs):
        collection = self.get_collection(collection_str)
        return collection.find_one_and_update(find_json_str, update_json_str, return_document=kwargs)


if __name__ == '__main__':
    manager = MongoManager()
    points, total = manager.find_all(
        "hierarchy_elements", {"category": "point"}, offset=0, limit=100, values={"SPID": 1})
    for room in points:
        print(room)
    print(total, points)
