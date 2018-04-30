# coding:utf8

import bs4
from fs_common import *


class HtmlConstructor:
    def __init__(self):
        soup = BeautifulSoup('')

        html_tag = self.new_tag(name='html')

        head_tag = self.new_tag(name='head')
        title_tag = self.new_tag(name='title')
        meta_tag = self.new_tag(name='meta', attrs={
            'http-equive': 'Content-Type',
            'content': 'text/html',
            'charset': 'utf-8'
        })
        link_tag = self.new_tag(name='link', attrs={
            'rel': 'stylesheet',
            'href': './table.css',
            'type': 'text/css'
        })
        body_tag = self.new_tag(name='body')

        div_tag = self.new_tag(name='div', attrs={
            'id': 'content'
        })

        table_tag = bs4.Tag(name='table', attrs={
            'cellspacing': '0'
        })
        thead_tag = self.new_tag(name='thead')
        tbody_tag = self.new_tag(name='tbody')

        soup.append(html_tag)
        html_tag.append(head_tag)
        head_tag.append(title_tag)
        head_tag.append(meta_tag)
        head_tag.append(link_tag)
        html_tag.append(body_tag)
        body_tag.append(div_tag)
        div_tag.append(table_tag)
        table_tag.append(thead_tag)
        table_tag.append(tbody_tag)

        self._soup = soup
        self._thead_tag = thead_tag
        self._tbody_tag = tbody_tag

    def set_head(self, widths, *args):
        self._thead_tag.clear()

        tr_tag = self.new_tag(name='tr')
        for width, arg in zip(widths, args):
            th_tag = self.new_tag(name='th', obj=arg, attrs={
                'width': str(width) + '%'
            })
            tr_tag.append(th_tag)

        self._thead_tag.append(tr_tag)

    def add_row(self, *args):
        tr_tag = self.new_tag(name='tr')
        for arg in args:
            td_tag = self.new_tag(name='td', obj=arg)
            tr_tag.append(td_tag)

        self._tbody_tag.append(tr_tag)

    @classmethod
    def seq_type_check(cls, seq):
        if not seq:
            return None

        t = None
        for item in seq:
            if t != type(item) and t is not None:
                return None
            t = type(item)
        return t

    @classmethod
    def new_tag(cls, obj=None, **kwargs):
        tag = bs4.Tag(**kwargs)

        if not obj or isinstance(obj, str):
            tag.string = obj if obj else ''
        elif isinstance(obj, bs4.Tag):
            tag.append(obj)
        elif hasattr(obj, '__iter__'):
            seq_type = cls.seq_type_check(obj)
            if seq_type is bs4.Tag:
                for t in obj:
                    tag.append(t)
            elif seq_type is str:
                tag.append(''.join(obj))
            else:
                raise TypeError('not all of the value is same type')
        else:
            raise TypeError('not supported type {0}'.format(type(obj)))

        return tag

    @property
    def soup(self):
        return self._soup
