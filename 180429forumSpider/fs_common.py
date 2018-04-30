# coding:utf8

from bs4 import BeautifulSoup as _Bs

try:
    __import__('lxml')
    BeautifulSoup = (lambda markup: _Bs(markup, 'lxml'))
except ImportError:
    BeautifulSoup = (lambda markup: _Bs(markup, 'html.parser'))

# -------------------------------- Constants --------------------------------
BASE_URL = 'http://qgc.qq.com/317307354/t/'
COOKIES = ''  # !!

TIMEOUT = 12
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)'
}
