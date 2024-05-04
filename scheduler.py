#!/usr/bin/env python3

import json
from os import getpid
import socket
import subprocess
import time
import re
import docker
from queue import Queue
from threading import Thread

import psutil
from scheduler_logger import SchedulerLogger, Job

prod = False

# hyperparameters
poll_interval = 0.01 # 10ms
schedule_interval = 1 # 1s
ms_interval = 0.1 # 100ms
# number of consecutive abnormal readings required to trigger a reschedule
reading_threshold = 0.4 # 40%
consecutive_threshold_default = 2
qps_threshold = 5000
to_file = False
keep_lqs = 4
threads = 2

ONE_S_IN_NS = 1e+9
pull_images = False

if prod:
    to_file = True
    pull_images = True

# preparation phase
client = docker.from_env()
IMAGES = [
    {'name':'anakli/cca', 'tag': 'parsec_blackscholes'},
    {'name': 'anakli/cca', 'tag' :'parsec_canneal'},
    {'name': 'anakli/cca', 'tag' :'parsec_dedup'},
    {'name': 'anakli/cca', 'tag' :'parsec_ferret'},
    {'name': 'anakli/cca', 'tag' :'parsec_freqmine'},
    {'name': 'anakli/cca', 'tag' :'splash2x_radix'},
    {'name': 'anakli/cca', 'tag' :'parsec_vips'}
]
if pull_images:
    for image in IMAGES:
        print("pulling", image)
        client.images.pull(image['name'], image['tag'])

ta_file = "part4/target_achieved.json"
ta = json.load(open(ta_file))

memcached_pid = int(subprocess.check_output(["sudo", "systemctl", "show", "--property", "MainPID", "--value", "memcached"]))
print(f"memcached_pid = {memcached_pid}")
memcached = psutil.Process(memcached_pid)

# set the affinity of this script
p = psutil.Process(getpid())
p.cpu_affinity([0])

# processing of the target_achieved file
max_qps_by_core = {}
for c in ta:
    measurements = ta[c][f"{threads}"]
    ok_measurements = list(filter(lambda m: m['p95'] < 950, measurements)) # 950ns instead of 1ms just to have some margin
    max_qps = max(map(lambda m: m['achieved'], ok_measurements))
    print(f"max qps by core: t={threads}, c={c}, max={max_qps}")
    max_qps_by_core[int(c)] = max_qps

class Message():
    def __init__(self, action, args):
        self.action = action
        self.args = args

q = Queue()
class Scheduler:
    def __init__(self):
        self.t = Thread(target=self.scheduler_thread)
        self.t.daemon = True
        self.t.start()

    def scheduler_thread(self):
        while True:
            item = q.get()
            if item.action == 'affinity':
                core_list = [0,1] if item.args == 2 else [0]
                memcached.cpu_affinity(core_list)
                logger.update_cores(Job.MEMCACHED, core_list)
            else:
                print('unkown message', item)
            q.task_done()

    def update_scheduling(self, qps: int):
        logger.custom_event(Job.SCHEDULER, f"updating to handle qps = {int(qps)}")
        n_of_cores = 1 if max_qps_by_core[1] > qps else 2
        q.put(Message('affinity', n_of_cores))

    def join(self):
        self.t.join()

scheduler = Scheduler()
logger = SchedulerLogger(to_file)

# taken and modified from:
# https://github.com/dlrust/python-memcached-stats/blob/master/src/memcached_stats.py
class MemcachedClient:
    _client = None
    _buffer = b''
    _stat_regex = re.compile(r"STAT (.*) (.*)\r")

    def __init__(self, host='localhost', port=11211):
        self._host = host
        self._port = port

    @property
    def client(self):
        if self._client is None:
            self._client = socket.create_connection((self._host, self._port))
        return self._client

    def read_until(self, delim):
        while delim not in self._buffer:
            data = self.client.recv(1024)
            if not data: # socket closed
                return None
            self._buffer += data
        line,_,self._buffer = self._buffer.partition(delim)
        return line

    def command(self, cmd):
        self.client.send(("%s\n" % cmd).encode('ascii'))
        return self.read_until(b'END').decode('ascii')

    def stats(self):
        return dict(self._stat_regex.findall(self.command('stats')))

class MemcachedStats:
    get_count = 0
    set_count = 0
    last_readings = []

    def __init__(self) -> None:
        self.client = MemcachedClient()
        get, set = self.read()
        self.last_readings = []
        logger.custom_event(Job.SCHEDULER, f"initial get/set count = {get}/{set}")

    def read(self):
        stats = self.client.stats()
        curr_get_count = int(stats['cmd_get'])
        curr_set_count = int(stats['cmd_set'])
        get_diff = curr_get_count - self.get_count
        set_diff = curr_set_count - self.set_count
        self.get_count = curr_get_count
        self.set_count = curr_set_count
        self.last_readings.append((time.time_ns(), get_diff, set_diff))
        return (get_diff, set_diff)

    def qps(self):
        now = time.time_ns()
        delete_before = -1
        total_get =0
        total_set =0
        for i, (t, g, s) in reversed(list(enumerate(self.last_readings))):
            if now - t > ONE_S_IN_NS:
                delete_before = i
                break
            total_get += g
            total_set += s

        if delete_before > 0:
            del self.last_readings[0:delete_before]
        return total_get + total_set

    # queries received in the last ms
    def split_last_qps(self, split_poit=0.4):
        now = time.time_ns()
        total_get_after = 0
        total_set_after = 0
        total_get_before = 0
        total_set_before = 0
        s_in_ns=int(ONE_S_IN_NS)
        threshold = int(s_in_ns*split_poit)
        for (t, g, s) in reversed(self.last_readings):
            off = now - t
            if off > threshold:
                break
            total_get_after += g
            total_set_after += s

        return (total_get_after+total_set_after) * (1/split_poit)

    def last_reading(self):
        _, a, b = self.last_readings[-1]
        return (a+b)*(1/poll_interval)

stats = MemcachedStats()
inner_loop = int((1/poll_interval))
print(f"inner_loop - {inner_loop}")

stable = True
unstable_t = None
prev = 0
while True:
    thresh = qps_threshold 
    consecutive_threshold = consecutive_threshold_default
    for i in range(inner_loop):
        time.sleep(poll_interval)
        stats.read()
        qps = stats.qps() # used to trim stuff none the less
        lq = stats.last_reading()

        if stable:
            prev = qps
        if abs(prev - lq) > thresh and i % consecutive_threshold == consecutive_threshold-1:
            if stable:
                thresh *= 2
                consecutive_threshold *= 2
                unstable_t = time.time_ns()
            stable = False
            logger.custom_event(Job.SCHEDULER, f"unstable qps = {int(prev)}-{int(lq)} = {abs(prev - lq)}")

        if not stable and abs(prev - lq) < thresh:
            stable = True
            unstable_t = time.time_ns() - unstable_t
            logger.custom_event(Job.SCHEDULER, f"took {int(unstable_t)/ONE_S_IN_NS}s to become stable")
            scheduler.update_scheduling(int(lq))

        if stable and i % 200 == 0:
            # _, qps, _ = stats.last_reading()
            qps = stats.qps()
            logger.custom_event(Job.SCHEDULER, f"stable qps = {int(qps)}")
