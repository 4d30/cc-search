#!/usr/bin/env python

import os
import gzip
import json
import codecs
import struct
import bisect
from functools import partial
from itertools import repeat
from operator import methodcaller
from configparser import ConfigParser
import multiprocessing as mp
from multiprocessing.pool import ThreadPool
from multiprocessing import shared_memory
from multiprocessing import queues
import threading
from time import sleep
from collections import deque

import urllib3
from fastwarc.warc import ArchiveIterator, WarcRecordType


class CCFetcher:
    def __init__(self):
        self.hostname = "https://data.commoncrawl.org/"
        collinfo = "https://index.commoncrawl.org/collinfo.json"
        cache_path = "/tmp/collinfo.json"
        retries = urllib3.util.Retry(total=5,
                                     backoff_factor=1,
                                     read=5,
                                     status_forcelist=[429, 500,
                                                       502, 503,
                                                       504],
                                     raise_on_status=False,
                                     )
        self.http = urllib3.PoolManager(retries=retries,
                                        maxsize=32)
        self.headers = {'User-Agent': 'CareerAggregator/1.0 kaleh.net/cagg'}
        self.decoder = codecs.getreader('utf-8')
        if os.path.exists(cache_path):
            with open(cache_path, 'r') as handle:
                self.crawl = json.load(handle)[0]
        else:
            response = self.http.request('GET', collinfo,
                                         headers=self.headers)
            crawl = response.json()
            with open(cache_path, 'w') as handle:
                json.dump(crawl, handle)
            self.crawl = crawl[0]
            response.release_conn()

    def get_wat_paths(self):
        """Fetches the list of all WAT files in the current crawl."""
        path_url = (f"https://data.commoncrawl.org/crawl-data"
                    f"/{self.crawl['id']}/wat.paths.gz")
        with self.http.request('GET', path_url,
                               preload_content=False) as response:
            try:
                with gzip.GzipFile(fileobj=response) as unzipped:
                    paths = self.decoder(unzipped)
                    paths = map(methodcaller('strip'), paths)
                    yield from paths
            finally:
                response.release_conn()

    def get_wat_file(self, path):
        url = ''.join((self.hostname, path))
        response = self.http.request('GET', url, preload_content=False)
        try:
            yield from response
        finally:
            response.release_conn()


def search_idx(query_str):
    cc = CCFetcher()
    params = {
        "url": query_str,  # eg "*.teamtailor.com/*",
        "output": "json",
        "fl": "url,filename,offset,length",
        "filter": "=status:200",
        }
    response = cc.http.request('GET', cc.crawl['cdx-api'],
                               fields=params,
                               preload_content=False)
    try:
        records = map(cc.decoder, response)
        records = map(json.loads, records)
        # records = map(methodcaller('get', 'url', None), records)
        yield from records
    finally:
        response.release_conn()


class BufferManager:
    def __init__(self):
        MiB = pow(2, 20)
        shm_len = 128 * MiB
        self.shm = shared_memory.SharedMemory(name="cc_shm",
                                              create=True,
                                              size=shm_len)
        self.lock = mp.Lock()
        self._freelist = [(0, shm_len)]
        self._max_fragments = pow(2, 16)

    def get_percent_free(self):
        with self.lock:
            total_free = sum(ee - ss for ss, ee in self._freelist)
            free_ratio = (total_free / self.shm.size)
            return round(free_ratio * 100, 3)

    def get_percent_fragmented(self):
        with self.lock:
            total_free = sum(ee - ss for ss, ee in self._freelist)
            if total_free == 0:
                return 0.0
            largest_block = max(ee - ss for ss, ee in self._freelist)
            frag_ratio = (total_free - largest_block) / total_free
            return round(frag_ratio * 100, 3)

    def allocate_offset(self, record_length):
        with self.lock:
            for i, (start, end) in enumerate(self._freelist):
                block_size = end - start
                if block_size >= record_length:
                    allocated_offset = start
                    new_start = start + record_length
                    if new_start < end:
                        self._freelist[i] = (new_start, end)
                    else:
                        self._freelist.pop(i)
                    return allocated_offset
            return None

    def release_chunk(self, offset, length):
        start = offset
        end = offset + length
        insert_index = bisect.bisect_left(self._freelist,
                                          (start, end))
        self._freelist.insert(insert_index, (start, end))

    def coalesce(self):
        if not self._freelist:
            return None
        new_list = [self._freelist[0]]
        for start, end in self._freelist[1:]:
            last_start, last_end = new_list[-1]
            if start <= last_end:
                new_list[-1] = (last_start, max(last_end, end))
            else:
                new_list.append((start, end))
        self._freelist = new_list
        if len(self._freelist) > self._max_fragments:
            raise MemoryError(("Freelist exceeded maximum"
                               " allowed fragments."))

    def cleanup(self):
        self.shm.close()
        self.shm.unlink()


def init_fetcher():
    global http, headers
    retries = urllib3.util.Retry(total=5,
                                 backoff_factor=1,
                                 read=5,
                                 status_forcelist=[429, 500,
                                                   502, 503,
                                                   504],
                                 raise_on_status=False)
    http = urllib3.PoolManager(retries=retries, maxsize=4)
    headers = {'User-Agent': 'CareerAggregator/1.0 kaleh.net/cagg'}


def process_record(wat_record):
    url = wat_record.headers.get('WARC-Target-URI')
    url_bytes = url.encode('utf-8')
    url_len = len(url_bytes)
    url_len_struct = struct.pack('H', url_len)
    content_len = wat_record.content_length
    # 'H' -- unsigned short is 2-bytes
    total_length = 2 + url_len + content_len
    content = wat_record.reader.read()
    bytes = (url_len_struct, url_bytes, content,)
    bytes = b''.join(bytes)
    return bytes, total_length


def write_to_shm(mgr, record):
    bytes, total_length = record
    while True:
        offset = mgr.allocate_offset(total_length)
        if offset is not None:
            break
        sleep(0.01)
    mgr.shm.buf[offset:(offset + total_length)] = bytes
    return offset, total_length


def fetcher_process(mgr, work_queue, path):
    url = f"https://data.commoncrawl.org/{path}"

    def predicate(record):
        return record.record_type == WarcRecordType.metadata

    with http.request('GET', url,
                      headers=headers,
                      preload_content=False) as response:
        try:
            records = ArchiveIterator(response, parse_http=False)
            records = filter(predicate, records)
            records = map(process_record, records)
            records = map(write_to_shm, repeat(mgr), records)
            proc = map(work_queue.put, records)
            deque(proc, maxlen=0)
        finally:
            response.release_conn()


def worker_process(shm_name, work_queue, done_queue):
    config = ConfigParser()
    config.read('platform_rules.ini')
    rules = []
    for section in config.sections():
        rules.append({
            'section': section,
            'page_match': config[section]['page_match'].encode('utf-8'),
            'link_match': config[section]['link_match'].encode('utf-8'),
            'destination': config[section].get('destination')
        })
    shm = shared_memory.SharedMemory(name=shm_name, create=False)
    done_batch = []
    while True:
        msg = work_queue.get()
        if msg is None:
            done_queue.put(done_batch)
            break  # Shutdown signal
        offset, total_length = msg
        url_len = struct.unpack('H', shm.buf[offset:(offset + 2)])[0]
        start = offset + 2 + url_len
        end = offset + total_length
        for rule in rules:
            if shm.buf.obj.find(rule['link_match'], start, end) != -1:
                url = bytes(shm.buf[(offset + 2):(offset + 2 + url_len)])
                url = url.decode('utf-8')
                with open(rule['destination'], 'a') as handle:
                    handle.write(url + '\n')
                break
        done_batch.append((offset, total_length))
        if len(done_batch) > pow(2, 8):
            done_queue.put(done_batch)
            done_batch = []
    shm.close()


def reaper_thread(manager_obj, done_queue):
    while True:
        done_batch = done_queue.get()
        if done_batch is None:
            break
        all_chunks = []
        all_chunks.extend(done_batch)
        while True:
            try:
                batch = done_queue.get_nowait()
                if batch is None:
                    break
                all_chunks.extend(batch)
            except queues.Empty:
                break
        with manager_obj.lock:
            for offset, length in all_chunks:
                manager_obj.release_chunk(offset, length)
            manager_obj.coalesce()


def wat_stream(mgr, work_queue):
    fetcher = CCFetcher()
    paths = fetcher.get_wat_paths()
    proc = partial(fetcher_process, mgr, work_queue)
    with ThreadPool(processes=2, initializer=init_fetcher) as pool:
        pool.map(proc, paths)


def main():
    mgr = BufferManager()
    work_queue = mp.Queue()
    done_queue = mp.Queue()
    t = threading.Thread(target=reaper_thread,
                         args=(mgr, done_queue),
                         daemon=True)
    t.start()
    workers = []
    num_workers = 2
    for _ in range(num_workers):
        ww = mp.Process(target=worker_process,
                        args=(mgr.shm.name, work_queue, done_queue,))
        ww.start()
        workers.append(ww)
    try:
        wat_stream(mgr, work_queue)
    finally:
        for _ in range(num_workers):
            work_queue.put(None)
        for ww in workers:
            ww.join()
            ww.close()
        done_queue.put(None)
        t.join()
        mgr.cleanup()


if __name__ == '__main__':
    main()
