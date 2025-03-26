#!/usr/bin/env python3
# -*- coding: utf8 -*-
import base64
from os import path
from configparser import NoOptionError
from fdfs_client.client import *
from fdfs_client.exceptions import DataError
from pathlib import Path

from confs import fdfs_conf

base_dir = Path(__file__)


def init_client_config():
    """
    从config.ini读取FDFS配置信息并更新到client.conf文件
    :return:
    """
    conf = fdfs_conf.fdfs_config()
    file_name = str(base_dir.parent / 'client.conf')
    with open(file_name, 'w') as f:
        for i in conf:
            f.write("{}={}\r\n".format(i, conf[i]))


class FastDFS:
    def __init__(self):
        """
        初始化
        :param
        """
        init_client_config()
        try:
            client_conf = str(base_dir.parent / 'client.conf')
            self.client = Fdfs_client(get_tracker_conf(client_conf))
            # self.client = Fdfs_client(client_conf)
        except (TypeError, NoOptionError):
            src = path.dirname(path.dirname(path.abspath(__file__)))
            client_conf = path.join(src, 'fastdfs', "client.conf")
            # self.client = Fdfs_client(client_conf)
            self.client = Fdfs_client(get_tracker_conf(client_conf))

    def upload_by_buffer(self, content, file_name=None):
        """
        在FastDFS中保存文件
        :param content: 文件内容
        :param file_name: 文件名
        :return: 保存到数据库中的FastDFS的文件名
        """
        if not isinstance(content, bytes):
            content = content.encode()
        # Lib\site-packages\fdfs_client\tracker_client.py 388行
        # store_serv.ip_addr = ip_addr.strip(b'\x00') 要设为str
        # ip_addr.strip(b'\x00').decode()
        result = self.client.upload_by_buffer(content)
        if result.get("Status") != "Upload successed.":
            # source: 1 web ct 云 2: 本地fdfs
            return {'code': 2450, 'message': 'file upload failed.', 'source': 2}
        remote_file_id = result.get("Remote file_id")
        # return remote_file_id  # a bytes-like object is required, not 'str'
        # return remote_file_id.decode()
        # source: 1 web ct 云 2: 本地fdfs
        return {'code': 0, 'fileid': remote_file_id.decode(), 'message': 'success', 'source': 2}

    def upload_by_filename(self, filename):
        """
        在FastDFS中保存文件
        :param filename: 文件的存储路径+文件名
        :return: remote_file_id 保存到数据库中的FastDFS的文件名
        :return: local_filename 本地上传的文件名
        """
        result = self.client.upload_by_filename(filename)
        if result.get("Status") != "Upload successed.":
            return "upload file failed"
        remote_file_id = result.get("Remote file_id")
        local_filename = result.get("Local file name")
        return remote_file_id.decode(), local_filename

    def delete_file(self, remote_file_id):
        """
        将FastDFS中保存的文件删除
        :param remote_file_id: 要删除的FastDFS的文件名
        :return: True False
        """
        if not remote_file_id:
            return False
        # 系统路径默认不会有反斜杠, 为解决windows文件上传后无法删除的问题, 可前置做个转义
        remote_file_id = remote_file_id.replace("\\", "/")
        # print remote_file_id
        try:
            remote_file_id = remote_file_id.encode()
            result = self.client.delete_file(remote_file_id)
            # print "remove file :", remote_file_id, result
        except DataError:  # DataError: 2, No such file or directory
            result = "Delete file successed."
        return True if "Delete file successed." in result else False

    def download_to_buffer(self, remote_file_id):
        """
        下载存储在FastDFS中的文件
        :param remote_file_id: 要下载的FastDFS的文件名
        :return: file_buffer
        """
        remote_file_id = remote_file_id.replace("\\", "/")
        remote_file_id = remote_file_id.encode()
        try:
            result = self.client.download_to_buffer(remote_file_id)
            file_buffer = result.get('Content')
        except DataError as e:
            raise e
        return file_buffer.decode('utf-8', "ignore")

    def download_to_file(self, local_file_name, remote_file_id):
        """
        下载存储在FastDFS中的文件
        :param local_file_name: 本地存储的文件名
        :param remote_file_id: 要下载的FastDFS的文件名
        :return: local_filename
        """
        remote_file_id = remote_file_id.replace("\\", "/")
        remote_file_id = remote_file_id.encode()
        result = self.client.download_to_file(local_file_name, remote_file_id)
        content = result.get('Content')
        return content

    def make_dirs(self, dfs_path):
        return True

    def exists_file(self, file_name, schema=None):
        return True

    def upload_file_by_buffer(self, content, filename):
        """文件上传"""
        b64file = base64.b64encode(content)
        t = b64file + ('~~~' + filename).encode()
        new_b64file = base64.b64encode(t)
        result = self.upload_by_buffer(content=new_b64file)
        if result == 'upload file failed':
            return {'code': 2450, 'message': 'file upload failed.'}
        return {'code': 0, 'fileid': result, 'message': 'success'}

    def download_file_by_fileid(self, fileid):
        """文件下载"""
        filebuffer = self.download_to_buffer(remote_file_id=str(fileid))
        d64file = base64.b64decode(filebuffer)
        t = d64file.split('~~~'.encode())
        new_d64file, filename = (t[0], t[1].decode()) if len(t) > 1 else (t, 'unknown.txt')
        filecontent = base64.b64decode(new_d64file)
        return filecontent, filename


if __name__ == '__main__':
    # src = path.dirname(path.dirname(path.dirname(path.abspath(__file__))))
    # print(src)
    # file_name = path.join(src, 'aa.png')
    # fastdfs_client = FastDFS()
    # remote_id, file_name = fastdfs_client.upload_by_filename(file_name)
    # print(remote_id, file_name)
    # aa = fastdfs_client.download_to_buffer("group1/M00/02/10/yMjINWECb0iACf7TAACXKySF7JA270.png")
    # print(aa)
    # fastdfs_client.download_to_file('bb.png', remote_id)
    # fastdfs_client.delete_file('group1/M00/04/15/yMjINWToQ3aAM1NCAA8TlGW-iQY1546331')
    pass
