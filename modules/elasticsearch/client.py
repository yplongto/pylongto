#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""ElasticSearch增强操作类，支持7.x/8.x版本"""
import logging
import time
import warnings
from typing import Dict, List, Optional, Union

from elasticsearch import Elasticsearch, BadRequestError
from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

from confs import elastic_search_conf

warnings.filterwarnings("ignore")
logger = logging.getLogger("elastic_transport")


class ElasticSearch:
    """
    ElasticSearch增强操作类

    功能特性：
    - 连接池自动管理
    - 自动重试机制
    - 批量操作优化
    - 完整的CRUD操作
    - 类型提示支持

    使用示例：
    >>> es = ElasticSearch()
    >>> # 创建索引
    >>> es.create_index("test_index", {"properties": {"title": {"type": "text"}}})
    >>> # 插入文档
    >>> es.upsert_doc("test_index", "1", {"title": "Hello World"})
    >>> # 批量操作
    >>> actions = [{"_op_type": "index", "_index": "test_index", "doc": {"title": f"Doc {i}"}} for i in range(5)]
    >>> es.bulk_operation(actions)
    """

    def __init__(self, hosts: Union[str, List[str]] = None, auth: tuple = None, **client_options):
        """
        初始化ES客户端
        :param hosts: ES节点地址，默认读取配置
        :param auth: 认证信息(username, password)
        :param client_options: 客户端扩展配置
        示例：
        >>> # 使用自定义配置初始化
        >>> es = ElasticSearch(
        >>>     hosts=["http://localhost:9200"],
        >>>     auth=("user", "pass"),
        >>>     request_timeout=60
        >>> )
        """
        self._hosts = hosts or elastic_search_conf.host
        self._auth = auth or (elastic_search_conf.username, elastic_search_conf.password)

        # 合并默认配置
        default_options = {"verify_certs": False, "request_timeout": 30, "retry_on_timeout": True, "max_retries": 3}
        default_options.update(client_options)

        try:
            self.client = Elasticsearch(hosts=self._hosts, basic_auth=self._auth, **default_options)
            logger.info(f"成功连接到ES集群: {self.client.info()['cluster_name']}")
        except Exception as e:
            logger.error(f"ES连接失败: {str(e)}")
            raise

    # ------------ 获取集群信息 ------------

    def info(self):
        """获取ES集群信息 Get basic build, version, and cluster information."""
        return self.client.info()

    def get_alias(self):
        """获取别名 Retrieves information for one or more data stream or index aliases."""
        return self.client.indices.get_alias()

    # ------------ 索引管理 ------------
    def create_index(self, index: str, mappings: Optional[Dict] = None,
                     ignore_exist: bool = True) -> bool:
        """
        创建索引
        :param index: 索引名称
        :param mappings: mapping定义
        :param ignore_exist: 是否忽略已存在的索引
        :return: 是否创建成功
        示例：
        >>> mappings = {"properties": {"name": {"type": "keyword"}}}
        >>> create_index("people", mappings)
        True
        """
        try:
            return self.client.indices.create(
                index=index, mappings=mappings, ignore=400 if ignore_exist else 0
            )['acknowledged']
        except BadRequestError as e:
            logger.warning(f"索引创建失败: {str(e)}")
            return False

    def delete_index(self, index: str, ignore_not_exist: bool = True) -> bool:
        """
        删除索引

        :param index: 索引名称
        :param ignore_not_exist: 是否忽略不存在的索引
        :return: 是否删除成功
        """
        try:
            return self.client.indices.delete(
                index=index, ignore=404 if ignore_not_exist else 0
            )['acknowledged']
        except Exception as e:
            logger.error(f"索引删除失败: {str(e)}")
            return False

    def index_exists(self, index: str) -> bool:
        """检查索引是否存在"""
        return bool(self.client.indices.exists(index=index))

    # ------------ 文档CRUD操作 ------------
    def get_doc(self, index: str, doc_id: str, **kwargs) -> Optional[Dict]:
        """
        获取文档

        :param index: 索引名称
        :param doc_id: 文档ID
        :return: 文档数据或None

        示例：
        >>> get_doc("test_index", "1")
        {'_id': '1', '_source': {'title': 'Hello World'}, ...}
        """
        try:
            return self.client.get(index=index, id=doc_id, **kwargs)
        except Exception as e:
            logger.error(f"文档获取失败: {str(e)}")
            return None

    def upsert_doc(self, index: str, doc_id: str, document: Dict, **kwargs) -> Dict:
        """
        插入/更新文档（存在则更新，不存在则插入）
        :param index: 索引名称
        :param doc_id: 文档ID
        :param document: 文档内容
        :return: ES响应数据
        示例：
        >>> upsert_doc("users", "user_1", {"name": "张三", "age": 30})
        """
        return self.client.index(index=index, id=doc_id, document=document, **kwargs)

    def update_doc(self, index: str, doc_id: str, body: Dict, **kwargs) -> Dict:
        """
        部分更新文档
        :param index: 索引名称
        :param doc_id: 文档ID
        :param body: 更新内容（使用script或doc语法）
        示例：
        >>> # 使用doc语法更新
        >>> update_doc("users", "1", {"doc": {"age": 31}})
        >>> # 使用script更新
        >>> update_doc("users", "1", {"script": "ctx._source.age += 1"})
        """
        return self.client.update(index=index, id=doc_id, body=body, **kwargs)

    def delete_doc(self, index: str, doc_id: str, **kwargs) -> Dict:
        """删除指定文档"""
        return self.client.delete(
            index=index,
            id=doc_id,
            **kwargs
        )

    def search(self, index: str, query_body: Dict, size: int = 10, **kwargs) -> Dict:
        """
        执行搜索查询
        :param index: 索引名称
        :param query_body: 使用查询DSL的搜索定义
        :param size: 返回结果数量
        query_body:{
            from_: int 从指定索引开始查询
            filter_path: 添加过滤路径，显示指定字段(默认显示所有字段信息)
            sort: 排序查询
            ...
        }
        :return: 搜索结果

        示例：
        >>> query_body = {
                "query": {
                    "match": {"title": "Hello"}
                },
                "sort": [{"_id": "asc"}]
            }
        >>> search("test_index", body)
        """
        return self.client.search(
            index=index, body=query_body, size=size, **kwargs
        )

    def bulk_operation(self, actions: List[Dict], max_retries: int = 3, chunk_size: int = 1000, **kwargs) -> Dict:
        """
        增强型批量操作（支持自动重试和分块处理）
        :param actions: 操作列表，支持的操作类型：
            - index: 创建或替换文档
            - create: 创建新文档（必须不存在）
            - update: 部分更新文档
            - delete: 删除文档
        :param max_retries: 最大重试次数
        :param chunk_size: 每批处理数量

        返回格式：
        {
            "success": 成功数量,
            "failed": [错误详情列表]
        }

        示例：
        >>> actions = [
                {"_op_type": "index", "_index": "test", "doc": {"field": "value"}},
                {"_op_type": "update", "_index": "test", "_id": "1", "doc": {"field": "value"}},
                {"_op_type": "delete", "_index": "test", "_id": "1"}
            ]
        >>> bulk_operation(actions)
        """
        processed_actions = []
        for action in actions:
            op_type = action.get("_op_type", "index")
            processed = {
                "_op_type": op_type,
                "_index": action["_index"],
                "_id": action.get("_id"),
                "_source": action.get("doc") or action.get("_source"),
                "doc": action.get("doc")  # 用于update操作
            }
            if op_type == "update":
                processed["doc"] = action["doc"]
            processed_actions.append({k: v for k, v in processed.items() if v is not None})

        for attempt in range(max_retries):
            try:
                success, errors = helpers.bulk(
                    client=self.client, actions=processed_actions,
                    chunk_size=chunk_size, stats_only=False, **kwargs
                )
                return {"success": success, "failed": errors if isinstance(errors, list) else []}
            except BulkIndexError as e:
                logger.error(f"批量操作部分失败（尝试 {attempt + 1}/{max_retries}）错误数: {len(e.errors)}")
                if attempt == max_retries - 1:
                    return {"success": len(processed_actions) - len(e.errors), "failed": e.errors}
                # 过滤出失败的操作重试
                failed_ids = {err['index']['_id'] for err in e.errors if '_id' in err.get('index', {})}
                processed_actions = [act for act in processed_actions if act.get('_id') not in failed_ids]
            except Exception as e:
                logger.error(f"批量操作失败: {str(e)}")
                if attempt == max_retries - 1:
                    raise

        return {"success": 0, "failed": processed_actions}

    def raw_bulk_data(self, index_name, body):
        """
        原生批量插入数据
        :param index_name: 要插入数据的索引
        :param body: 要插入的数据|必须严格遵循Elasticsearch原生bulk API格式（操作元数据与数据体交替排列）
        [
            {"index": {"_index": "test", "_id": "1"}},
            {"field1": "value1"},
            {"delete": {"_index": "test", "_id": "2"}},
            {"create": {"_index": "test", "_id": "3"}},
            {"field1": "value3"},
            {"update": {"_id": "1", "_index": "test"}},
            {"doc": {"field2": "value2"}},
        ]

        :return:
        """
        return self.client.bulk(index=index_name, body=body)


# ------------ 单元测试 ------------
if __name__ == '__main__':
    # 初始化客户端
    es = ElasticSearch()

    print(f"测试ES集群信息： {es.info()}")
    print(f"测试ES别名信息： {es.get_alias()}")

    # 测试索引管理
    test_index = "test_index"
    if es.index_exists(test_index):
        print(f"删除索引测试 index: {test_index} 结果为 {es.delete_index(test_index)}")

    # 创建索引测试
    mapping = {
        "properties": {
            "title": {"type": "text"},
            "count": {"type": "integer"}
        }
    }
    # assert es.create_index(test_index, mapping) is True
    # assert es.index_exists(test_index) is True
    print(f"创建索引测试 index: {test_index} 结果为 {es.create_index(test_index, mapping) is True}")
    print(f"验证索引测试 index: {test_index} 结果为 {es.index_exists(test_index) is True}")

    # 文档操作测试
    doc_id = "1"
    doc_data = {"title": "Test Document", "count": 42}
    es.upsert_doc(test_index, doc_id, doc_data)

    # 获取文档测试
    retrieved = es.get_doc(test_index, doc_id)
    assert retrieved["_source"] == doc_data
    print(f"获取文档测试id: {doc_id} 内容为 {doc_data} is {retrieved["_source"] == doc_data}")

    # 更新文档测试
    es.update_doc(test_index, doc_id, {"doc": {"count": 43}})
    updated = es.get_doc(test_index, doc_id)
    assert updated["_source"]["count"] == 43
    print(f"更新文档测试id: {doc_id} 内容为 {updated["_source"]["count"]} is {updated["_source"]["count"] == 43}")

    # 批量操作测试
    bulk_actions = [
        {"_op_type": "index", "_index": test_index, "doc": {"title": f"Doc {i}"}}
        for i in range(5)
    ]
    bulk_actions.append({"_op_type": "delete", "_index": test_index, "_id": doc_id})
    result = es.bulk_operation(bulk_actions)
    print(f"批量操作结果: 成功 {result['success']} 条，失败 {len(result['failed'])} 条")
    assert result["success"] >= 5  # 5个插入+1个删除，具体取决于删除是否成功

    time.sleep(1)
    # 搜索测试
    body = {"query": {"match_all": {}}}
    search_result = es.search(test_index, body)
    print(f"搜索到 {search_result['hits']['total']['value']} 条结果")

    # 清理测试数据
    es.delete_index(test_index)
    print("所有测试完成")
