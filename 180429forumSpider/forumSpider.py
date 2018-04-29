# coding:utf8

import bs4
import enum
import shelve
import logging
import requests
import threading
# import memory_profiler
from concurrent.futures import ThreadPoolExecutor

# -------------------------------- Logger Init --------------------------------
assert __name__ != '__mp_main__'

__log__ = logging.getLogger(__name__)
__log__.setLevel(logging.DEBUG)
f = logging.Formatter('%(relativeCreated)d, th%(thread)d: %(message)s')

sh = logging.StreamHandler()
sh.setFormatter(f)
fh = logging.FileHandler(filename='forumSpider.log', mode='w', encoding='utf8')
fh.setFormatter(f)

__log__.addHandler(sh)
__log__.addHandler(fh)
del f
del sh
del fh

# -------------------------------- Constants --------------------------------
BASE_URL = 'http://qgc.qq.com/317307354/t/'
COOKIES = ''  # !!

TIMEOUT = 12
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)'}

try:
    __import__('lxml')
    BS_FEATURES = 'lxml'
except ImportError:
    BS_FEATURES = 'html.parser'


# -------------------------------- Codes --------------------------------
class MemberPage:

    class Status(enum.Enum):
        error = 0
        active = 1
        reserved = 2
        no_such_topic = 3

    def __init__(self, page_source, page_id):
        assert isinstance(page_source, str)
        self._soup = bs4.BeautifulSoup(page_source, BS_FEATURES)

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


class Downloader:
    """ Non Thread-Safe """

    DEFAULTS = {'timeout': TIMEOUT, 'headers': HEADERS}

    def __init__(self, cookies=None):
        if cookies is None:
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


class HtmlConstructor:
    def __init__(self):
        soup = bs4.BeautifulSoup('', 'lxml')

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
                break
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


class ForumSpider:
    NUM_TASKS_PER_THREAD_LOWERLIMIT = 50
    NO_TOPIC_FOUND_UPPERLIMIT = 50

    def __init__(self, max_page_id, base_url, cookies=None, nthreads=3):
        assert isinstance(cookies, (type(None), str, dict))
        self._cookies = cookies
        self._base_url = base_url

        assert max_page_id > 0
        self._max_page_id = max_page_id

        self._data = shelve.open('./membd.tmp')
        self._data.clear()
        self._data_lock = threading.Lock()

        self._nthreads = min(max(nthreads, 1), 20)

    def run(self):
        # 将任务分配给多个线程

        ntasks_per_thread = int(max(self._max_page_id / self._nthreads,
                                    self.NUM_TASKS_PER_THREAD_LOWERLIMIT))
        if ntasks_per_thread == self.NUM_TASKS_PER_THREAD_LOWERLIMIT:
            nthreads = self._max_page_id / ntasks_per_thread
            nthreads = int(nthreads if nthreads == int(nthreads) else nthreads + 1)
        else:
            nthreads = self._nthreads

        tasks_seq = [(i * ntasks_per_thread + 1,
                      min((i + 1) * ntasks_per_thread, self._max_page_id))
                     for i in range(nthreads)]
        __log__.debug('task_seq: {0}'.format(tasks_seq))
        __log__.debug('ntasks_per_thread: {0}'.format(ntasks_per_thread))
        __log__.debug('nthreads: {0}'.format(nthreads))

        with ThreadPoolExecutor(max_workers=nthreads) as executor:
            futures = [executor.submit(self.procedure_enum_range, *args)
                       for args in tasks_seq]
            for future in futures:
                err = future.exception()
                if err:
                    __log__.error('[-]Some thread was broken\n{0}'.format(err))

        __log__.info('[+]HTML is constructing...')
        hc = HtmlConstructor()
        hc.set_head((16, 84), 'Index', 'Content')
        for member_name, member_data in self._data.items():
            hc.add_row(member_name, member_data)

        __log__.info('[+]Writing... ')
        with open('test.html', mode='w', encoding='utf8') as wf:
            wf.write(str(hc.soup).replace('\n', '<br/>'))

        __log__.info('[+]Done')

    def _front_page_iterator(self, start, stop):
        """ Yield page info, like (id, url, is_retry) """
        
        retries = []

        stop = int(stop)
        page_id = int(start)
        while page_id <= stop:
            dropped_tup = yield (page_id, self._base_url + str(page_id), False)
            if dropped_tup is not None:
                retries.append(dropped_tup)
            page_id += 1

        for dropped_tup in retries:
            yield (dropped_tup, True)

    # @memory_profiler.profile
    def procedure_enum_range(self, start, stop):
        """ Enumerate member front page and get information store into self._data """

        no_topic_found_cnt = 0

        with Downloader(self._cookies) as downloader:
            page_iter = self._front_page_iterator(start, stop)
            for page_id, page_url, is_retry in page_iter:

                response = downloader.download(page_url)
                if not response:
                    if is_retry:
                        __log__.info('[-]Cannot download page {0}'.format(page_url))
                    else:
                        __log__.info('[-]Page {0} download failure'.format(page_url))
                        page_iter.send((page_id, page_url))
                    continue

                mp = MemberPage(response.content.decode('utf8'), page_id)
                if mp.no_such_topic:
                    no_topic_found_cnt += 1
                    if no_topic_found_cnt > self.NO_TOPIC_FOUND_UPPERLIMIT:
                        __log__.info("[+]Procedure exit, 'no_topic_found_cnt' > {0}".format(self.NO_TOPIC_FOUND_UPPERLIMIT))
                        break
                    __log__.info('[+]No topic found {0}, count {1}'.format(page_url, no_topic_found_cnt))
                elif mp.reserved:
                    __log__.info('[+]Id reserved {0}'.format(page_id))
                elif mp.active:
                    with self._data_lock:
                        if mp.name in self._data:
                            __log__.warning("[-]Same member found on page {0} - {1}".format(page_url, mp.name))
                        else:
                            self._data[mp.name] = mp.data
                            __log__.info('[+]{0}'.format(page_url))
                else:
                    __log__.error('[-]Status.error found: procedure ready to quit, maybe the cookies are out of date.')
                    break

    def close(self):
        self._data.close()


def main():
    fs = ForumSpider(max_page_id=1300, base_url=BASE_URL,
                     cookies=COOKIES, nthreads=20)
    fs.run()
    fs.close()


def test():
    import cProfile
    cProfile.run('main()')


if __name__ == '__main__':
    main()
