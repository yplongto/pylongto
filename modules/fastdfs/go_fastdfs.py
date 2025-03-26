#!/usr/bin/env python3
# -*- coding: utf8 -*-
import base64
from typing import Union, Dict

from engine.crawler import CrawlerBase
from confs import go_fastdfs_conf

from utils.service_util import download_file_to_bytesio


class FileCrawler(CrawlerBase):
    def __init__(self):
        super().__init__()
        self.URI = f'http://{go_fastdfs_conf.HOST}:{go_fastdfs_conf.PORT}'

    def upload_by_buffer(self, filename, binary_data=None, scene='heatmap'):
        """
        文件上传
        :param filename:
        :param binary_data:
        :param scene:
        :return:
        """
        url = f'{self.URI}/{go_fastdfs_conf.GROUP}/upload'
        _sence = '/'.join([go_fastdfs_conf.SCENE, scene])
        files = {'file': binary_data}
        options = {'output': 'json', 'path': '', 'scene': _sence, 'filename': filename}
        try:
            resp, html = self.post(url, files=files, data=options)
            jbody = resp.json()
        except Exception as e:
            self.logger.warning('Found File Upload Error.', exc_info=True)
            return False, {'code': 2450, 'message': 'file upload failed.'}

        retcode = jbody['retcode']
        # {
        #   'code': 0,
        #   'data': {
        #       'domain': 'http://192.168.200.128:8080',
        #       'md5': '3bc725c6246def3f5dc41a53078aadfd',
        #       'mtime': 1719279979,
        #       'path': '/group1/heatmap/20240625/09/46/8/01J16H51PY6EQ44QPV3NDXHYMX',
        #       'retcode': 0,
        #       'retmsg': '',
        #       'scene': 'heatmap',
        #       'scenes': 'heatmap',
        #       'size': 60,
        #       'src': '/group1/heatmap/20240625/09/46/8/01J16H51PY6EQ44QPV3NDXHYMX',
        #       'url': 'http://192.168.200.128:8080/group1/heatmap/20240625/09/46/8/01J16H51PY6EQ44QPV3NDXHYMX?name=01J16H51PY6EQ44QPV3NDXHYMX&download=1'
        #   },
        #   'message': 'success'
        # }
        return True if retcode == 0 else False, {'code': 0, 'data': jbody, 'message': 'success'}

    def download_to_buffer(self, fileid):
        url = f'{self.URI}{fileid}'
        try:
            # resp, _ = self.get(url)
            # content = resp.content
            bytes_stream = download_file_to_bytesio(url)
            content = bytes_stream.read()
        except Exception as e:
            return False, {'code': 2470, 'message': 'file download failed.'}
        return True, {'data': content, 'fileid': fileid}


    def upload(self, filename, binary_data, scene='heatmap'):
        """
        文件上传
        :param filename:
        :param binary_data:
        :param scene:
        :return:
        """
        # 重新编码
        b64file = base64.b64encode(binary_data)
        t = b64file + ('~~~' + filename).encode()
        new_b64file = base64.b64encode(t)
        return self.upload_by_buffer(filename, new_b64file, scene)

    def download(self, fileid):
        """
        文件下载
        :param fileid:
        :return:
        """
        status, resp_content = self.download_to_buffer(fileid)
        if not status:
            return False, resp_content
        content = resp_content['data']
        d64file = base64.b64decode(content)
        t = d64file.split('~~~'.encode())
        new_d64file, fileid = t[0], t[1].decode()
        file_content = base64.b64decode(new_d64file)
        return True, {'data': file_content, 'fileid': fileid}

    def delete(self, fileid) -> bool:
        """
        文件删除
        :param fileid:
        :return:
        """
        group = f'/{go_fastdfs_conf.GROUP}'
        if not fileid.startswith(group):
            group = fileid.split('/')[1]

        url = f'{self.URI}/{group}/delete'
        params = {'path': fileid}
        resp, _ = self.get(url, params=params)
        jbody = resp.json()
        return True if jbody['status'] == 'ok' else False

    def download_to_file(self, *args, **kwargs):
        pass

    def make_dirs(self, *args, **kwargs):
        pass

    def exists_file(self, *args, **kwargs):
        pass

    def delete_file(self, fileid):
        return self.delete(fileid=fileid)


if __name__ == '__main__':
    from ulid import ULID
    vfilename = str(ULID())
    binary_data = b'Binary data'
    fc = FileCrawler()
    status, x = fc.upload(vfilename, binary_data)
    print(x)
    fileid = x['data'].get('path')
    print(fileid)
    # fileid = '/group1/heatmap/20240624/16/52/8/01J14Q5FE2NV7BCK3TGQEQ0MTG'
    x = fc.download(fileid)
    print(x)
    # fileid = 'f98938b963d75bf87e828a549ef3ff49'
    x = fc.delete(fileid)
    print(x)