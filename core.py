#!/usr/bin/env python

import re
import os
import sys
import gzip
import json
import codecs
import struct
from itertools import chain, repeat
from operator import methodcaller, itemgetter
from dataclasses import dataclass
from configparser import ConfigParser
import multiprocessing as mp
from multiprocessing import shared_memory
import threading
from time import sleep

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
        self.http = urllib3.PoolManager(retries=retries)
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
        with self.http.request('GET', url,
                               preload_content=False) as response:
            try:
                iterator = ArchiveIterator(response)
                yield from iterator
            finally:
                response.release_conn()

def wat_stream():
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


class BufferManager:
    def __init__(self):
        GiB = pow(2, 30)
        shm_len = 6*GiB
        self.shm = shared_memory.SharedMemory(create=True,
                                              size=shm_len)
        self.manager = mp.Manager()
        self.free_list = self.manager.list([(0, shm_len,)])
        self.lock = self.manager.Lock()


    def allocate_offset(self, record_length):
        with self.lock:
            for i, (start, end) in enumerate(self.free_list):
                if (end - start) >= record_length:
                    allocated_start = start
                    if (end - start) == record_length:
                        self.free_list.pop(i)
                    else:
                        # Shrink the gap: move the start point forward
                        self.free_list[i] = (start + record_length, end)
                    return allocated_start
            return None 


    def _coalesce(self):
        """
        Mutates the free_list in-place to merge touching blocks.
        Example: [(0, 100), (100, 300)] -> [(0, 300)]
        """
        if len(self.free_list) < 2:
            return None
        i = 0
        while i < len(self.free_list) - 1:
            current_start, current_end = self.free_list[i]
            next_start, next_end = self.free_list[i + 1]
            # If the current block ends exactly where the next one starts
            if current_end == next_start:
                # Merge them into the current slot
                self.free_list[i] = (current_start, next_end)
                # Remove the now-redundant next slot
                self.free_list.pop(i + 1)
                # Don't increment i; check if this new merged block 
                # can also merge with the NEW next block.
            else:
                i += 1


    def release_offset(self, offset, record_length):
        with self.lock:
            # 1. Put the block back
            new_block = (offset, offset + record_length)
            self.free_list.append(new_block)
            # 2. Sort so adjacent blocks are neighbors
            # (Necessary for the coalesce logic to work in one pass)
            self.free_list.sort(key=itemgetter(0))
            # 3. Stitch them together
            self._coalesce()

    def cleanup(self):
        self.shm.close()
        self.shm.unlink()
        self.manager.shutdown()


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
    while True:
        msg = work_queue.get()
        if msg is None: break # Shutdown signal
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
        done_queue.put((offset, total_length))
    shm.close()


def reaper_thread(manager_obj, done_queue):
    while True:
        msg = done_queue.get()
        if msg is None: break  # Poison pill to stop the thread
        offset, length = msg
        # Now it has the 'manager_obj' to call the release logic
        manager_obj.release_offset(offset, length)



#   rules = ConfigParser()
#   rules.read('platform_rules.ini')
#   for section in rules.sections():
#       for key in rules[section]:
#           print(section, key, rules[section][key][:30])
def main():
    mgr = BufferManager()
    work_queue = mp.Queue()
    done_queue = mp.Queue()
    t = threading.Thread(target=reaper_thread, 
                         args=(mgr, done_queue),
                         daemon=True)
    t.start()
    workers = []
    num_workers = 4
    for _ in range(num_workers):
        ww = mp.Process(target=worker_process,
                        args=(mgr.shm.name, work_queue, done_queue,))
        ww.start()
        workers.append(ww)
    
    try:
        wat_records = wat_stream()
        wat_records = filter(lambda x: x.rec_type == 'metadata',
                             wat_records)
        # TODO: filter for config'd substrings
        for wat_record in wat_records:
            url = wat_record.rec_headers.get_header('WARC-Target-URI')
            url_bytes = url.encode('utf-8')
            url_len = len(url_bytes)
            content_len = wat_record.length
            total_length = 2 + url_len + content_len
            while True:
                offset = mgr.allocate_offset(total_length) # offset
                if offset is not None:
                    break
                sleep(5)
            # 'H' -- unsigned short is 2-bytes
            mgr.shm.buf[offset : offset + 2] = struct.pack('H', url_len)
            mgr.shm.buf[offset + 2 : offset + 2 + url_len] = url_bytes
            aa = offset + 2 + url_len
            bb = offset + total_length
            mgr.shm.buf[aa:bb] = wat_record.content_stream().read()
            work_queue.put((offset, total_length,))
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
