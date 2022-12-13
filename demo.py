#!usr/bin/env python
# -*- coding:utf-8 _*-
"""
@author: goupper
@file: demo.py
@version: v1
@time: 2022/12/13
"""
import threading

import requests
import consul


class HTTPClient(consul.base.HTTPClient):

    _instance_lock = threading.Lock()
    _session = None

    def __init__(self, *args, **kwargs):
        super(HTTPClient, self).__init__(*args, **kwargs)
        if not self.__class__._session:
            # multiplexing tcp connection
            # solve consul error: 429 Your IP is issuing too many concurrent connections, please rate limit your calls
            with self._instance_lock:
                if not self.__class__._session:
                    self.__class__._session = requests.Session()
        self.session = self.__class__._session

    @staticmethod
    def response(response):
        response.encoding = 'utf-8'
        return consul.base.Response(
            response.status_code, response.headers, response.text, response.content
        )

    def get(self, callback, path, params=None, headers=None):
        uri = self.uri(path, params)
        return callback(
            self.response(
                self.session.get(
                    uri,
                    headers=headers,
                    verify=self.verify,
                    cert=self.cert,
                    timeout=self.timeout,
                )
            )
        )

    def put(self, callback, path, params=None, data='', headers=None):
        uri = self.uri(path, params)
        return callback(
            self.response(
                self.session.put(
                    uri,
                    data=data,
                    headers=headers,
                    verify=self.verify,
                    cert=self.cert,
                    timeout=self.timeout,
                )
            )
        )

    def delete(self, callback, path, params=None, data='', headers=None):
        uri = self.uri(path, params)
        return callback(
            self.response(
                self.session.delete(
                    uri,
                    data=data,
                    headers=headers,
                    verify=self.verify,
                    cert=self.cert,
                    timeout=self.timeout,
                )
            )
        )

    def post(self, callback, path, params=None, headers=None, data=''):
        uri = self.uri(path, params)
        return callback(
            self.response(
                self.session.post(
                    uri,
                    data=data,
                    headers=headers,
                    verify=self.verify,
                    cert=self.cert,
                    timeout=self.timeout,
                )
            )
        )


class CustomConsul(consul.Consul):
    @staticmethod
    def http_connect(host, port, scheme, verify=True, cert=None, timeout=None):
        return HTTPClient(host, port, scheme, verify, cert, timeout)


if __name__ == '__main__':
    consul_client = CustomConsul()
    key = 'test-key'
    consul_client.kv.put(key, 'test put')
    _, data = consul_client.kv.get(key)
    value = data['Value'].decode() if (data and data['Value']) else None
    assert value == 'test put'
