#!/usr/bin/env python3
# -*- coding: utf8 -*-
import base64
import threading

from .go_fastdfs import FileCrawler


class FileManage(object):
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not hasattr(FileManage, "_instance"):
            with FileManage._instance_lock:
                if not hasattr(FileManage, "_instance"):
                    FileManage._instance = object.__new__(cls)
        return FileManage._instance

    def __init__(self):
        # self.file_client = FastDFS() if IS_DEV_ENV else FileCrawler()
        self.file_client = FileCrawler()

    def upload_by_buffer(self, content, file_name, timeout=30):
        """
        统一处理文件内容和文件名
        :param content:
        :param file_name:
        :param timeout:
        :return:
        """
        b64file = base64.b64encode(content)
        t = b64file + ('~~~' + file_name).encode()
        new_b64file = base64.b64encode(t)
        return self.file_client.upload_by_buffer(new_b64file, file_name)

    def download_to_buffer(self, remote_file_url, timeout=10):
        file_buffer = self.file_client.download_to_buffer(remote_file_url)
        try:
            d64file = base64.b64decode(file_buffer)
        except ValueError:  # 老版本的文件内容，部分文件可能存储的不是base64的文件内容
            return file_buffer
        t = d64file.split('~~~'.encode())
        new_d64file, filename = (t[0], t[1].decode()) if len(t) > 1 else (t, 'unknown.txt')
        file_content = base64.b64decode(new_d64file)
        return file_content, filename

    def download_to_file(self, local_file_name, remote_file_url, timeout=10):
        return self.file_client.download_to_file(local_file_name, remote_file_url)

    def make_dirs(self, dfs_path, timeout=10):
        return self.file_client.make_dirs(dfs_path)

    def delete_file(self, remote_file_url, timeout=10):
        return self.file_client.delete_file(remote_file_url)

    @staticmethod
    def exists_file(self, file_name, schema=None, timeout=10):
        return self.file_client.exists_file(file_name, schema)
