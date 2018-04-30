# coding:utf8

import enum
from fs_common import *


class MemberPage:

    class Status(enum.Enum):
        error = 0
        active = 1
        reserved = 2
        no_such_topic = 3

    def __init__(self, page_source, page_id):
        assert isinstance(page_source, str)
        self._soup = BeautifulSoup(page_source)

        assert isinstance(page_id, int)
        self._page_id = page_id

        # Information on this page
        self._page_status = None
        self._member_name = None
        self._title = None
        self._data = None

    @property
    def error(self):
        return self.page_status is self.Status.error

    @property
    def active(self):
        return self.page_status is self.Status.active

    @property
    def reserved(self):
        return self.page_status is self.Status.reserved

    @property
    def no_such_topic(self):
        return self.page_status is self.Status.no_such_topic

    @property
    def no_such_member(self):
        return self.page_status in (self.Status.reserved,
                                    self.Status.no_such_topic)

    @property
    def page_status(self):
        if self._page_status is None:
            elem_class_crumbs = self._soup.select_one('.crumbs')
            if elem_class_crumbs:
                elemlist_a = elem_class_crumbs.select('a')
                if len(elemlist_a) == 2 and \
                        elemlist_a[0].text == 'S成长会2018' and elemlist_a[1].text == '预留':
                    self._page_status = self.Status.reserved
                else:
                    self._page_status = self.Status.active
            else:
                div_class_error = self._soup.select_one('.error')
                if not div_class_error or '请登录后再进行此操作' in div_class_error.text:
                    self._page_status = self.Status.error
                else:
                    self._page_status = self.Status.no_such_topic

        return self._page_status

    @property
    def data(self):
        self.raise_for_invalid_operation()

        if self._data is None:
            self._data = [tag.text
                          for tag in self._soup.select('.pctmessage.mbm')]

        return self._data

    @property
    def title(self):
        self.raise_for_invalid_operation()

        if self._title is None:
            self._title = self._soup.select_one('#threadtitle').string

        return self._title

    @property
    def name(self):
        self.raise_for_invalid_operation()

        if self._member_name is None:
            self._member_name = '<PAGE>\n{0}\n<THREADTITLE>\n{1}'.format(
                self._page_id, self.title)

        return self._member_name

    def raise_for_invalid_operation(self):
        if self.no_such_member or self.error:
            raise RuntimeError('invalid operation, status {0}'.format(self._page_status))
