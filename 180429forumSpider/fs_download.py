# coding:utf8

import requests
from fs_common import *


class Downloader:
    """ Non Thread-Safe """

    DEFAULTS = {'timeout': TIMEOUT, 'headers': HEADERS}

    def __init__(self, cookies=None):
        if not cookies:
            kvpd = {}
        elif isinstance(cookies, str):
            kvpl = cookies.strip('"').split('; ')
            kvp = None
            try:
                kvpd = {kvp.split('=')[0]: kvp.split('=')[1] for kvp in kvpl}
            except IndexError:
                raise ValueError("not supported format '{0}'".format(kvp))
        elif isinstance(cookies, dict):
            kvpd = cookies.copy()
        else:
            raise TypeError('not supported type {0}'.format(type(cookies)))

        self._session = requests.session()
        requests.utils.add_dict_to_cookiejar(self._session.cookies, kvpd)

    def download(self, url, params=None, **kwargs):
        kwargs.update(self.DEFAULTS)
        response = None
        try:
            response = self._session.get(url, params=params, **kwargs)
            response.raise_for_status()
        except Exception as err:
            __log__.error('[-]Error: {0}'.format(err))
        return response

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        self._session.close()
