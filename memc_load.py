#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io
import os
import sys
import gzip
import glob
import logging
import time
import collections as cs
from functools import partial
from optparse import OptionParser, Values
import multiprocessing as mp
# import multiprocessing.dummy as mt
import threading as mt
import queue as q
from tqdm import tqdm

# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2

# pip install python-memcached
import memcache

MEMCACHE_TIMEOUT = 4.  # secs
MEMCACHE_ATTEMPTS = 2
MEMCACHE_RETRY_TIMEOUT = 0.1

WORKER_STOP = 'beer'
WORKER_BUFFER_RECORDS = 128

NORMAL_ERR_RATE = 0.01

AppsInstalled = cs.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

# if 'line_profiler' not in dir() and 'profile' not in dir():
#     def profile(func):
#         return func

def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))

def parse_appsinstalled(line: bytes):
    line_parts = list(map(bytes.strip, line.split(b'\t')))
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = tuple(int(a.strip()) for a in raw_apps.split(b','))
    except ValueError:
        apps = tuple([int(a.strip()) for a in raw_apps.split(b',') if a.isidigit()])
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)

def pack_appsinstalled(appsinstalled):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    ua.apps.extend(appsinstalled.apps)
    return b'%s:%s' % (appsinstalled.dev_type, appsinstalled.dev_id),\
           ua.SerializeToString()

#@profile
def memc_worker(memc: memcache.Client, tasks: q.Queue, results: q.Queue, is_dry: bool = False, attempts: int = MEMCACHE_ATTEMPTS, retry_timeout: float = MEMCACHE_RETRY_TIMEOUT):
    processed = errors = 0
    cp = mp.current_process()
    ct = mt.current_thread()
    logging.debug('-%s:%s- start processing memcache on %s', cp.name, ct.name, memc.servers[0])
    while True:
        try:
            records = tasks.get()  # timeout = 0.01
            if records == WORKER_STOP:
                logging.debug('-%s:%s- put results from %s: %d / %d', cp.name, ct.name, memc.servers[0], errors, processed)
                results.put((processed, errors))
                logging.debug('-%s:%s- stop processing memcache on %s: %d / %d', cp.name, ct.name, memc.servers[0], errors, processed)
                break
            if is_dry:
                for key, value in records.items():
                    logging.debug("-%s:%s- %s - %s -> %s" % (cp.name, ct.name, memc.servers[0], key, value[:64].replace(b'\n', b' ')))
                    processed += 1
                continue
            retry = 0
            error_keys = memc.set_multi(records, noreply=False)
            while retry < attempts:
                error_keys = memc.set_multi({key: records[key] for key in error_keys})
                retry += 1
                time.sleep(retry_timeout)
            errors += len(error_keys)
            processed += len(records)
            logging.debug('-%s:%s- saving into memcache %s: %d / %d', cp.name, ct.name, memc.servers[0], errors, processed)
        except BaseException as err:
            logging.exception('-%s:%s- Unexpected exception [%s] occurred while processing memcache on %s' % (cp.name, ct.name, err, memc.servers[0]))

#@profile
def process_file(options: Values, args: tuple[int, str]) -> tuple[str, int, int]:
    idx, fn = args
    cp = mp.current_process()
    logging.info('Processing %s' % fn)
    processed = errors = 0
    if (f_size := os.path.getsize(fn)):   
        device_memc = {
            b'idfa': options.idfa,
            b'gaid': options.gaid,
            b'adid': options.adid,
            b'dvid': options.dvid,
        }
        tasks = {}
        workers = []
        results = q.Queue()
        buffers = cs.defaultdict(dict)
        for dev_type, memc_addr in device_memc.items():
            memc = memcache.Client([memc_addr], socket_timeout=MEMCACHE_TIMEOUT)
            tasks[dev_type] = q.Queue()
            worker = mt.Thread(
                        target=memc_worker, 
                        args=(memc, tasks[dev_type], results, options.dry), 
                        daemon=False,
                    )
            workers.append(worker)
            worker.start()
        with tqdm(desc=f'{fn: <64}', total=f_size, unit='b', unit_scale=True, unit_divisor=1024, position=idx, ) as pbar:
            with open(fn, 'rb') as f, gzip.open(f, 'rb') as gz: #, io.BufferedReader(gz) as bf:
                for line in gz:
                    pbar.n = f.tell()
                    pbar.update(1)                    
                    line = line.strip()  # processing in bytes
                    if not line:
                        continue
                    appsinstalled = parse_appsinstalled(line)
                    if not appsinstalled:
                        logging.error("-%s- Invalid appsinstalled record: %s" % (cp.name, line))
                        errors += 1
                        continue
                    dev_type =  appsinstalled.dev_type 
                    if dev_type not in device_memc:
                        errors += 1
                        logging.error('-%s- Unknown device type: %s' % (cp.name, dev_type))
                        continue
                    key, value = pack_appsinstalled(appsinstalled)
                    buffers[dev_type][key] = value
                    if len(buffers[dev_type]) >= WORKER_BUFFER_RECORDS:
                        logging.debug('-%s- put %d records of type %s for memcache on %s', cp.name, len(buffers[dev_type]), dev_type, device_memc[dev_type])                        
                        tasks[dev_type].put(buffers[dev_type])
                        buffers[dev_type] = {}
                # flush beffers
                for dev_type, records in buffers.items():
                    if records:
                        tasks[dev_type].put(records)
                # stop workers
                logging.debug('-%s- stopping workers: %d', cp.name, len(tasks))                    
                for task in tasks.values():
                    task.put(WORKER_STOP)
                # join workers
                logging.debug('-%s- joining workers: %d', cp.name, len(workers))
                for worker in workers:
                    worker.join()                  
                # calc results
                logging.debug('-%s- starting results processing: %s', cp.name, results.empty())
                while not results.empty():
                    result = results.get()  # or timeout=0.01
                    logging.debug('get result: %s', result)                    
                    processed += result[0]
                    errors += result[1]
            pbar.refresh()    
    return fn, processed, errors

#@profile
def main(options: Values):
    logging.info("start processing with options: %s", options)
    with mp.Pool(
            processes=options.workers,
            initializer=tqdm.set_lock,
            initargs=(tqdm.get_lock(),)
        ) as ps_pool:
        pf = partial(process_file, options)
        for fn, processed, errors in ps_pool.imap_unordered(pf, enumerate(sorted(glob.iglob(options.pattern)))):
            logging.debug("%s: %d - ok, %d - errs", fn, processed, errors)
            err_rate = float(errors) / processed if processed else 1
            if err_rate < NORMAL_ERR_RATE:
                logging.info("Processed (%d) records. Acceptable error rate (%s). Successfull load %s", processed, err_rate, fn)
            else:
                logging.error("Processed (%d) records. High error rate (%s > %s). Failed load %s", processed, err_rate, NORMAL_ERR_RATE, fn)
            dot_rename(fn)

def prototest():
    sample = b"idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split(b"\t")
        apps = [int(a) for a in raw_apps.split(b",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked

if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("-w", "--workers", type="int", action="store", default=mp.cpu_count())  # dest="workers_count"
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="./data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()

    logging.basicConfig(filename=opts.log, level= logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
