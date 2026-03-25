#!/usr/bin/env python

import io
import re
import os
import sys
import gzip
import json
import codecs
import struct
import time
import ctypes
import heapq
import bisect
import random
from functools import partial
from itertools import chain, repeat
from operator import methodcaller, itemgetter
from dataclasses import dataclass
from configparser import ConfigParser
import multiprocessing as mp
from multiprocessing.pool import ThreadPool, Pool
from multiprocessing import shared_memory
from multiprocessing import queues
import threading
from time import sleep
from concurrent.futures import ThreadPoolExecutor

import urllib3
from warcio.archiveiterator import ArchiveIterator


class NewCCFetcher:
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
        self.headers = {'User-Agent':'CareerAggregator/1.0 kaleh.net/cagg'}
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


def wat_stream_old():
    fetcher = NewCCFetcher()
    paths = fetcher.get_wat_paths()
    records = map(fetcher.get_wat_file, paths)
    records = chain.from_iterable(records)
    yield from records


def search_idx(query_str):
    cc = NewCCFetcher()
    params = {
        "url": query_str,  # eg "*.teamtailor.com/*",
        "output": "json",
        "fl": "url,filename,offset,length",
        "filter": "=status:200",
        }
    response = cc.http.request('GET', self.crawl['cdx-api'],
                               fields=params,
                               preload_content=False)
    try:
        records = map(cc.decoder, response)
        records = map(json.loads, records)
        # records = map(methodcaller('get', 'url', None), records)
        yield from records
    finally:
        response.release_conn()


@dataclass
class WATQuery:
    name: str
    page_match: str
    link_match: str
    destination: str


def search_wat(link_match, wat_match=None):
    cc = CCFetcher()
    pat = re.compile(link_match)
    paths = cc.get_wat_paths()
    links = map(cc.extract_links_from_wat, paths, repeat(wat_match))
    links = chain.from_iterable(links)
    for record_uri, link in links:
        if pat.search(link):
            yield record_uri

class RawBufferManager:
    def __init__(self):
        GiB = pow(2, 30)
        shm_len = 6 * GiB
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
                                 raise_on_status=False )
    http = urllib3.PoolManager(retries=retries, maxsize=4)
    headers = {'User-Agent': 'CareerAggregator/1.0 kaleh.net/cagg'}


def fetcher_process(path):
    # Each process creates its own PoolManager with the same retry logic
    url = f"https://data.commoncrawl.org/{path}"
    with http.request('GET', url,
                     headers=headers,
                      preload_content=False) as response:
        # Stream and decompress on the fly
        records = ArchiveIterator(response)
        records = filter(lambda x: x.rec_type == 'metadata', records)
        records = map(process_record, records)
        yield from records


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
            break # Shutdown signal
        offset, total_length = msg
        url_len = struct.unpack('H', shm.buf[offset : offset + 2])[0]
        raw_view = shm.buf[offset + 2 + url_len : offset + total_length]
        for rule in rules:
            if rule['link_match'] in raw_view:
                url = bytes(shm.buf[offset + 2 : offset + 2 + url_len])
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
    reap_bytes = 0
    reap_chunks = 0
    reap_time = 0
    while True:
        done_batch = done_queue.get() # Wait for the first one
        if done_batch is None: break
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

        t0 = time.perf_counter()
        with manager_obj.lock:
            for offset, length in all_chunks:
                manager_obj.release_chunk(offset, length)
            manager_obj.coalesce()
        t1 = time.perf_counter()
        reap_time += (t1 - t0)
        reap_chunks += len(all_chunks)
        reap_bytes += sum(length for _, length in all_chunks)
        msg = (f"{round(manager_obj.get_percent_free(), 3)} "
               f"{round(manager_obj.get_percent_fragmented(), 3)} "
               "\r")
        sys.stdout.write(msg)
        #print("REAPER:",
              ##manager_obj._freelist
              #"reap_bytes: " , reap_bytes,
              #"chunks/sec:", reap_chunks / reap_time,
              #"MB/sec:", reap_bytes / reap_time / (1024*1024),
              #)

def process_file(wat_file):
    wat_records = ArchiveIterator(io.BytesIO(wat_file))
    wat_records = filter(lambda x: x.rec_type == 'metadata', wat_records)
    records = map(process_record, wat_records)
    return tuple(records)

def process_record(wat_record):
    url = wat_record.rec_headers.get_header('WARC-Target-URI')
    url_bytes = url.encode('utf-8')
    url_len = len(url_bytes)
    url_len_struct = struct.pack('H', url_len)
    content_len = wat_record.length
    # 'H' -- unsigned short is 2-bytes
    total_length = 2 + url_len + content_len
    content = wat_record.content_stream().read()
    return (url_len, url_len_struct, url_bytes, total_length,
            content,)

def wat_stream_old():
    fetcher = NewCCFetcher()
    paths = fetcher.get_wat_paths()
    with ThreadPool(processes=8) as thread_pool:
        wat_files = thread_pool.imap_unordered(fetcher.get_wat_file, paths)
        with mp.Pool(processes=8) as proc_pool:
            records = proc_pool.imap_unordered(process_file, wat_files)
            yield from filter(bool, records)

def wat_stream():
    fetcher = NewCCFetcher()
    paths = fetcher.get_wat_paths()
    with ThreadPool(processes=2, initializer=init_fetcher) as pool:
        record_iterators = pool.imap_unordered(fetcher_process, paths)
        records = chain.from_iterable(record_iterators)
        yield from records

def main():
    mgr = RawBufferManager()
    work_queue = mp.Queue()
    done_queue = mp.Queue()
    t = threading.Thread(target=reaper_thread, 
                         args=(mgr, done_queue),
                         daemon=True)
    t.start()
    workers = []
    num_workers = 8
    alloc_bytes = 0
    alloc_count = 0
    elapsed = 0
    for _ in range(num_workers):
        ww = mp.Process(target=worker_process,
                        args=(mgr.shm.name, work_queue, done_queue,))
        ww.start()
        workers.append(ww)
    try:
        t0 = None
        for processed_record in wat_stream():
            url_len, url_len_struct, url_bytes, total_length, content = processed_record
            while True:
                offset = mgr.allocate_offset(total_length) # offset
                #print("Allocated: ", offset, total_length)
                if offset is not None:
                    break
                sleep(0.01)
                print(f"{mgr.get_percent_free()*pow(2,30)} free, cannot allocate {total_length}")
                breakpoint()
            mgr.shm.buf[offset : offset + 2] = url_len_struct
            mgr.shm.buf[offset + 2 : offset + 2 + url_len] = url_bytes
            mgr.shm.buf[(offset + 2 + url_len):(offset + total_length)] = content
            work_queue.put((offset, total_length,))
            alloc_bytes += total_length
            alloc_count += 1
            tt = time.perf_counter()
            if t0 is not None:
                #print(round(tt - t0, 6),
                #      mgr.get_percent_free(),
                #      mgr.get_percent_fragmented())
                elapsed += (tt - t0)
                t0 = tt
                #if random.random() > 0.99:
                    #print("ALLOC:",
                          ##mgr._freelist
                          #"alloc_bytes:", alloc_bytes,
                          #"chunks/sec:", alloc_count / elapsed,
                          #"MB/sec:", alloc_bytes / elapsed / (1024*1024),
                          #)
            else:
                t0 = tt
            msg = (f"{round(mgr.get_percent_free(), 3)} "
                   f"{round(mgr.get_percent_fragmented(), 3)} "
                   "\r")
            sys.stdout.write(msg)

    finally:
        for _ in range(num_workers):
            work_queue.put(None)
        for ww in workers:
            ww.join()
            ww.close()
        done_queue.put(None)
        t.join()
        mgr.cleanup()
#     pat = r'https://www\.teamtailor\.com/\?utm_campaign=poweredby'    
#     path = 'teamtailor.dat'
#     with open(path, 'w') as handle:
#         for uri in search_wat(pat, wat_match=path):
#             print(uri, file=handle)
#             handle.flush()


if __name__ == '__main__':
    main()
