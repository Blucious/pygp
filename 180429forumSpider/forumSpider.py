# coding:utf8

import shelve
import logging
import threading
# import memory_profiler
from concurrent.futures import ThreadPoolExecutor

from fs_mempage import MemberPage
from fs_download import Downloader
from fs_htmlcon import HtmlConstructor
from fs_common import *

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

# -------------------------------- Codes --------------------------------


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
                    __log__.info('[+]No topic found on page {0}, count {1}'.format(page_url, no_topic_found_cnt))
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
