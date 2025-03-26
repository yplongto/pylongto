#!/usr/bin/env python3
# -*- coding: utf8 -*-
import json
import time
import logging
from datetime import datetime
from hashlib import md5

import requests

from confs import WebCTFdfsConf


logger = logging.getLogger('fdfsServer')


class WebCTDfs:
    # web_ct_dfs_token = None
    web_dfs_conf = None
    status_code_translate = {
        0: "请求处理成功",
        -1: "请求失败，安全认证未通过",
        -2: "请求失败，URL路径错误",
        -3: "请求失败，参数错误",
        -4: "请求失败，请求资源不存在",
        -5: "请求失败，服务端处理异常",
    }

    def __init__(self, web_dfs_conf=None, timeout=None, session=None):
        """
        初始化web dfs客户端
        :param web_dfs_conf:
        :param timeout: 连接超时，转发到请求处理程序。在放弃之前等待服务器发送数据的时间，浮点数或 元组。如果达到超时，将引发适当的异常
        :param session:
        """
        self.web_dfs_conf = WebCTFdfsConf() if not web_dfs_conf else web_dfs_conf
        self.headers(self.web_dfs_conf)
        self.timeout = timeout
        self.session = session

    def md5_convert(self, data):
        """
        md5加密
        :param data:
        :return:
        """
        m = md5()
        m.update(data)
        md5_pwd = m.hexdigest()
        return md5_pwd.upper()

    def headers(self, web_dfs_conf):
        user_name = web_dfs_conf.USER_NAME
        password = web_dfs_conf.PASSWORD
        this_time = datetime.now()
        timestamp = round(this_time.timestamp() * 1000)
        web_ct_dfs_token = self.md5_convert(f"{password}{timestamp}".encode())
        return {
            "webctdfs-token": web_ct_dfs_token,
            "webctdfs-username": user_name,
            "webctdfs-timestamp": str(timestamp),
        }

    def upload_by_buffer(self, file_contents, file_name, params=None, default_prefix=None, schema=None):
        if not file_contents and not params:
            return "upload file is empty"
        if not default_prefix:
            default_prefix = self.web_dfs_conf.UPLOAD_PREFIX
        if not schema:
            schema = self.web_dfs_conf.SCHEMA
        if file_contents:
            headers = self.headers(self.web_dfs_conf)
            url = "".join([self.web_dfs_conf.ADDRESS, default_prefix, schema])
            data = {
                "params": json.dumps({"overwrite": True}),
                "file": (file_name, file_contents),
            }
            result = requests.post(url=url, files=data, headers=headers, timeout=self.timeout)
            if result.status_code != 200:
                content = json.loads(result.content) if result.content else result.text
                # source: 1 web ct 云 2: 本地fdfs
                return {'code': 2450, 'message': content, 'source': 1}
            else:
                data = result.json()
                # source: 1 web ct 云 2: 本地fdfs
                return {
                    'code': data.get('code', 2450),
                    'fileid': data.get('result'),
                    'message': 'success',
                    'source': 1
                }
        else:
            pass

    def make_dirs(self, dir_path, default_prefix=None, schema=None):
        if not default_prefix:
            default_prefix = self.web_dfs_conf.DIR_PREFIX
        if not schema:
            schema = self.web_dfs_conf.SCHEMA
        if not dir_path.startwith('/'):
            dir_path = '/' + dir_path
        url = self.web_dfs_conf.ADDRESS + default_prefix + schema + dir_path
        headers = self.headers(self.web_dfs_conf)
        result = requests.post(url=url, headers=headers, timeout=self.timeout).json()
        return self.status_code_translate[result['code']]

    def exists_file(self, file_name, schema=None):
        if not schema:
            schema = self.web_dfs_conf.SCHEMA
        url = self.web_dfs_conf.ADDRESS + '/webctdfs/existence/' + schema + '/' + file_name
        headers = self.headers(self.web_dfs_conf)
        result = requests.get(url=url, headers=headers).json()
        return True if result.get(result) == 'true' else False

    def download_to_file(self, local_file_name, remote_file_url, schema=None):
        if not schema:
            schema = self.web_dfs_conf.SCHEMA
        remote_file_url = remote_file_url.replace("\\", "/")
        headers = self.headers(self.web_dfs_conf)
        url = "".join([self.web_dfs_conf.ADDRESS, self.web_dfs_conf.UPLOAD_PREFIX,
                       schema, '/', remote_file_url])
        result = requests.get(url=url, headers=headers, timeout=self.timeout)
        return result.content

    def download_to_buffer(self, remote_file_url, schema=None):
        """
        下载存储在WebCT DFS中的文件
        :param remote_file_url: 要下载的WebCT DFS的文件名
        :return: file_buffer
        """
        if not schema:
            schema = self.web_dfs_conf.SCHEMA
        remote_file_url = remote_file_url.replace("\\", "/")
        headers = self.headers(self.web_dfs_conf)
        headers.update({"Content-Type": "multipart/form-data"})
        url = "".join([self.web_dfs_conf.ADDRESS, self.web_dfs_conf.CONTENT_PREFIX,
                       schema, '/', remote_file_url])
        result = requests.get(url=url, headers=headers, timeout=self.timeout).json()
        return result.get('result')

    def delete_file(self, remote_file_url):
        """
        将FastDFS中保存的文件删除
        :param remote_file_url: 要删除的WebCTDFS的文件地址
        :return: True/ False
        """
        if not remote_file_url:
            return False
        # 系统路径默认不会有反斜杠, 为解决windows文件上传后无法删除的问题, 可前置做个转义
        remote_file_url = remote_file_url.replace("\\", "/").encode()
        # print remote_file_id
        result = {}
        try:
            headers = self.headers(self.web_dfs_conf)
            headers.update({
                "Content-Type": "multipart/form-data"
            })
            url = self.web_dfs_conf.ADDRESS + self.web_dfs_conf.UPLOAD_PREFIX + remote_file_url
            result = requests.delete(url=url, headers=headers, timeout=self.timeout).json()
            # print "remove file :", remote_file_id, result
        except Exception as e:
            logger.error(e, exc_info=True)
        return True if result.get('code') == 0 else False


if __name__ == "__main__":
    wd = WebCTDfs()
