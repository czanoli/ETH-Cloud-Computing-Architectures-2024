#!/usr/bin/env python3

from typing import List
import json
from os import getpid
from psutil import cpu_percent
import socket
import subprocess
import time
import re
import docker
from queue import Queue
from threading import Thread

import psutil
from scheduler_logger import SchedulerLogger, Job

prod = True

# hyperparameters
poll_interval = 0.01 # 10ms
schedule_interval = 1 # 1s
ms_interval = 0.1 # 100ms
# number of consecutive abnormal readings required to trigger a reschedule
reading_threshold = 0.4 # 40%
consecutive_threshold_default = 2
qps_threshold = 5000
close_enough = 1000
to_file = False
keep_lqs = 4
threads = 2

ONE_S_IN_NS = 1e+9
pull_images = False

if prod:
    to_file = True

# preparation phase
IMAGES = [
    {'name':'anakli/cca', 'tag': 'parsec_blackscholes'},
    {'name': 'anakli/cca', 'tag' :'parsec_canneal'},
    {'name': 'anakli/cca', 'tag' :'parsec_dedup'},
    {'name': 'anakli/cca', 'tag' :'parsec_ferret'},
    {'name': 'anakli/cca', 'tag' :'parsec_freqmine'},
    {'name': 'anakli/cca', 'tag' :'splash2x_radix'},
    {'name': 'anakli/cca', 'tag' :'parsec_vips'}
]
IMAGE_PER_JOB = {}
IMAGE_PER_JOB[Job.BLACKSCHOLES] = IMAGES[0]
IMAGE_PER_JOB[Job.CANNEAL] = IMAGES[1]
IMAGE_PER_JOB[Job.DEDUP] = IMAGES[2]
IMAGE_PER_JOB[Job.FERRET] = IMAGES[3]
IMAGE_PER_JOB[Job.FREQMINE] = IMAGES[4]
IMAGE_PER_JOB[Job.RADIX] = IMAGES[5]
IMAGE_PER_JOB[Job.VIPS] = IMAGES[6]

ta_file = "target_achieved.json"
ta = json.load(open(ta_file))

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
        self.read()
        self.last_readings = []

    def read(self):
        stats = self.client.stats()
        curr_get_count = int(stats['cmd_get'])
        curr_set_count = int(stats['cmd_set'])
        get_diff = curr_get_count - self.get_count
        set_diff = curr_set_count - self.set_count
        self.get_count = curr_get_count
        self.set_count = curr_set_count
        self.last_readings.append((time.time_ns(), get_diff, set_diff))
        self.cleanup()

    def cleanup(self):
        now = time.time_ns()
        delete_before = -1
        for i, (t, _, _) in reversed(list(enumerate(self.last_readings))):
            if now - t > ONE_S_IN_NS:
                delete_before = i
                break
        if delete_before > 0:
            del self.last_readings[0:delete_before]

    # queries received in the last count*10ms
    def last_measurements(self, count=10):
        total_get_after = 0
        total_set_after = 0
        for i, (_, g, s) in enumerate(reversed(self.last_readings)):
            if i > count:
                continue
            total_get_after += g
            total_set_after += s

        return (total_get_after+total_set_after) / (poll_interval*count)

    def qps(self):
        return self.last_measurements(int(1/poll_interval))

class MemcachedThread():
    qin = Queue()
    qout = Queue()
    stats = MemcachedStats()

    def __init__(self):
        self.t = Thread(target=self.thread)
        self.t.daemon = True
        self.t.start()

    def thread(self):
        i = 0
        while True:
            time.sleep(poll_interval/10)
            if i % 10 == 0:
                i = 0
                self.stats.read()

            if not self.qin.empty():
                (msg, arg) = self.qin.get()
                if msg == 'last_measurements':
                    self.qout.put(self.stats.last_measurements(arg))
                elif msg == 'qps':
                    self.qout.put(self.stats.qps())
                self.qin.task_done()

            i += 1

    def do(self, method, arg=0):
        self.qin.put((method, arg))
        res = self.qout.get()
        self.qout.task_done()
        return res

class CPUThread():
    qin = Queue()
    qout = Queue()
    readings = []

    def __init__(self):
        self.t = Thread(target=self.thread)
        self.t.daemon = True
        self.t.start()

    def read(self):
            cores = cpu_percent(interval=None, percpu=True)
            cores = cores[:2]
            self.readings.append(cores)
            cleanup_point =int(1/poll_interval)
            self.readings = self.readings[:cleanup_point]


    def avg(self, count):
        tot0 = []
        tot1 = []
        for i in range(min(count, len(self.readings))):
            tot0.append(self.readings[i][0])
            tot1.append(self.readings[i][1])

        return [max(tot0), max(tot1)]

    def thread(self):
        i = 0
        while True:
            time.sleep(poll_interval/10)
            if i % 10 == 0:
                i = 0
                self.read()

            if not self.qin.empty():
                count = self.qin.get()
                self.qout.put(self.avg(count))
                self.qin.task_done()

            i += 1

    def get(self, readings=int(1/poll_interval)):
        self.qin.put(readings)
        res = self.qout.get()
        self.qout.task_done()
        return res



# set the affinity of this script
p = psutil.Process(getpid())
p.cpu_affinity([0])

HIGH_CPU_JOBS = [Job.FREQMINE, Job.CANNEAL, Job.FERRET, Job.BLACKSCHOLES, Job.RADIX]
LOW_CPU_JOBS = [Job.DEDUP, Job.VIPS]

MEMCACHED_ONE_CORE = [0]
MEMCACHED_TWO_CORES = [0,1]
HIGH_CPU_CORES = ('2,3', [2,3])
LOW_CPU_CORES = ('1', [1])

N_JOB_ON_CORE23 = 2
N_JOB_ON_CORE1 = 1

class Scheduler:
    msgq = Queue()
    highq = Queue()
    lowq = Queue()
    max_qps_by_core = {}
    def __init__(self):
        self.ct = CPUThread()
        self.client = docker.from_env()
        self.logger = SchedulerLogger(to_file)

        self.compute_max_qps_per_core()
        self.memcached_pid = int(subprocess.check_output(["sudo", "systemctl", "show", "--property", "MainPID", "--value", "memcached"]))
        self.logger.custom_event(Job.SCHEDULER, f"found memcached with pid={self.memcached_pid}")
        self.memcached = psutil.Process(self.memcached_pid)

        for job in HIGH_CPU_JOBS:
            self.highq.put(job)
        self.qcore23 = []
        for job in LOW_CPU_JOBS:
            self.lowq.put(job)
        self.qcore1 = []
        self.unstable()

        self.t = Thread(target=self.thread)
        self.t.daemon = True
        self.t.start()

    def compute_max_qps_per_core(self):
        for c in ta:
            measurements = ta[f"{c}"][f"{threads}"]
            ok_measurements = list(filter(lambda m: m['p95'] < 950, measurements)) # 950ns instead of 1ms just to have some margin
            max_qps = max(map(lambda m: m['achieved'], ok_measurements))
            self.logger.custom_event(Job.SCHEDULER, f"max qps by core: t={threads}, c={c}, max={max_qps}")
            self.max_qps_by_core[int(c)] = max_qps

    def run_job(self, job: Job, high: bool):
        image = IMAGE_PER_JOB[job]
        cores = HIGH_CPU_CORES if high else LOW_CPU_CORES
        threads = len(cores[1])*2
        container = self.client.containers.run(
            f"{image['name']}:{image['tag']}",
            detach=True,
            command=f"./run -a run -S {'parsec' if job != Job.RADIX else 'splash2x'} -p {job.value} -i native -n {threads}",
            cpuset_cpus=cores[0]
        )
        if high:
            self.qcore23.append((job, container.id))
        else:
            self.qcore1.append((job, container.id))
        self.logger.job_start(job, cores[1], threads)

    def collect_done_jobs(self):
        cores = [
            (self.qcore23, min(N_JOB_ON_CORE23, len(self.qcore23))),
            (self.qcore1, min(N_JOB_ON_CORE1, len(self.qcore1))),
        ]
        for (arr, n) in cores:
            for i in reversed(range(n)):
                job, id = arr[i]
                container = self.client.containers.get(id)
                if container.status == 'exited' or container.status == 'dead':
                    self.logger.job_end(job)
                    del arr[i]

    def _pause_helper(self, arr, n: int, pause: bool):
        lower = max(len(arr)-n, 0)
        for i in reversed(range(lower, len(arr))):
            job, id = arr[i]
            container = self.client.containers.get(id)
            if pause and container.status != 'paused':
                container.pause()
                self.logger.job_pause(job)
            elif not pause and container.status == 'paused':
                container.unpause()
                self.logger.job_unpause(job)

    def pause(self, arr, n: int):
        self._pause_helper(arr, n, True)

    def unpause(self, arr, n: int):
        self._pause_helper(arr, n, False)

    def thread(self):
        i = 0
        while True:
            time.sleep(poll_interval/10)
            if not self.msgq.empty():
                action, cores = self.msgq.get()
                if action == 'memcached_affinity':
                    self.memcached_core_list = cores
                    self.memcached.cpu_affinity(self.memcached_core_list)
                    self.logger.update_cores(Job.MEMCACHED, self.memcached_core_list)
                else:
                    print('unkown action', action)
                self.msgq.task_done()

            [core0, core1] = self.ct.get(10)
            if len(self.memcached_core_list) > 1:
                if core1 < 30:
                    self.pause(self.qcore1, 1)
                else:
                    self.pause(self.qcore1, 2)
            else:
                self.unpause(self.qcore1, 2)

            if i % 10 == 0:
                i = 0
                self.collect_done_jobs()
                # TODO: collect done jobs
                if len(self.qcore23) < N_JOB_ON_CORE23 and not self.highq.empty():
                    job = self.highq.get()
                    self.highq.task_done()
                    self.run_job(job, True)
                if len(self.qcore1) < N_JOB_ON_CORE1 and not self.lowq.empty():
                    job = self.lowq.get()
                    self.lowq.task_done()
                    self.run_job(job, False)

                if len(self.qcore1) == 0 and len(self.qcore23) == 0 and self.highq.empty() and self.lowq.empty():
                    self.logger.end()
                    exit(1)

            i += 1

    def stable_qps(self, qps: int):
        self.logger.custom_event(Job.SCHEDULER, f"updating to handle qps = {int(qps)}")
        cores = MEMCACHED_ONE_CORE if self.max_qps_by_core[1] > qps else MEMCACHED_TWO_CORES
        self.msgq.put(('memcached_affinity', cores))

    def unstable(self):
        self.logger.custom_event(Job.SCHEDULER, f"memcached unstable, defaulting to two cores")
        self.msgq.put(('memcached_affinity', MEMCACHED_TWO_CORES))

mt = MemcachedThread()
scheduler = Scheduler()

keep = 20
prev_list = []
stable = False
while True:
    time.sleep(4*poll_interval)
    curr = mt.do('last_measurements', 50)
    if len(prev_list) > 0:
        prev = sum(prev_list)/len(prev_list)
    else:
        prev = 0

    prev_list.append(curr)
    prev_list = prev_list[-keep:]
    if abs(prev - curr) > qps_threshold:
        if stable:
            scheduler.unstable()
            stable = False
        div = 1+int(abs(prev - curr)/max(prev, curr) * 10)
        last_close_enough = all(map(lambda n: curr - n < close_enough, prev_list[-(keep//div):]))
        if last_close_enough:
            prev_list = [curr]
    else:
        if not stable:
            qps = (mt.do('qps') + mt.do('last_measurements', 10)) / 2
            scheduler.stable_qps(qps)
            stable = True
