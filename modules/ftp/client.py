#!/usr/bin/env python3
# -*- coding: utf8 -*-
import os
from ftplib import FTP

from confs import ftp_conf


class BaseFTP(FTP):
    def __init__(self, host=None, port=None, user=None, passwd=None,
                 acct=None, timeout: int = None, source_address=None):
        super(BaseFTP, self).__init__()
        self.host = host or ftp_conf.host
        self.port = int(port or ftp_conf.port)
        self.user = user or ftp_conf.user
        self.passwd = passwd or ftp_conf.password
        self.acct = acct
        self.timeout = timeout
        self.source_address = source_address

    def connect_login(self, cwd_path=True):
        self.connect(host=self.host, port=self.port, timeout=self.timeout, source_address=self.source_address)
        self.login(user=self.user, passwd=self.passwd, acct=self.acct)
        if cwd_path is True:
            # 切换至指定目录
            self.cwd(ftp_conf.local_dir)

    def download_file(self, local_file, remote_file):
        """
        文件下载
        :param local_file: 本地文件路径
        :param remote_file: 远端文件路径
        :return:
        """
        if remote_file in self.nlst():
            with open(local_file, 'wb') as f:
                self.retrbinary(f'RETR {remote_file}', f.write)
            return True
        else:
            return False

    def upload_file(self, local_file, remote_file):
        """
        文件上传
        :param local_file: 本地文件路径
        :param remote_file: 远端文件路径
        :return:
        """
        if not os.path.isfile(local_file):
            return False
        with open(local_file, 'rb') as f:
            self.storbinary(f'STOR {remote_file}', f, 4096)
        return True

    def get_latest_file(self, filename):
        """获取指定文件名的最新上传文件名"""
        all_files = self.nlst()
        if len(all_files) == 0:
            return None
        res_files = []
        for file in all_files:
            if filename in file:
                res_files.append(file)
        if len(res_files) == 0:
            return None
        res_files.sort(reverse=True)
        return res_files[0]


if __name__ == '__main__':
    ftp = BaseFTP()
    ftp.connect_login(cwd_path=True)
    last_file_name = ftp.get_latest_file('CM_NH_SITE_POWER_LOG')
    ftp.download_file(last_file_name, last_file_name)
    # 删除本地文件
    os.remove(last_file_name)
    # 切换到/ms目录
    # ftp.cwd('/ms')

    # # 上传文件
    # with open('CM_NH_SITE_POWER_LOG_20230615.csv', 'rb') as f:
    #     ftp.storbinary('STOR CM_NH_SITE_POWER_LOG_20230615.csv', f)
