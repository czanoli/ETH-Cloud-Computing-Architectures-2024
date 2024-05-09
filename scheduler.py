#!/usr/bin/env python3

from typing import List, cast
import json
from os import getpid
from psutil import cpu_percent
import socket
import subprocess
import time
import re
from math import floor
from docker import from_env as docker_connect
from docker.models.containers import Container
from queue import Queue
from threading import Thread
import psutil
from scheduler_logger import SchedulerLogger, Job as JobKind

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
max_qps_one_core = 40_000 # around 39939 

ONE_S_IN_NS = 1e9
pull_images = False

IMAGE_PER_JOB = {}
IMAGE_PER_JOB[JobKind.BLACKSCHOLES] = {'name':'anakli/cca', 'tag': 'parsec_blackscholes'}
IMAGE_PER_JOB[JobKind.CANNEAL] = {'name': 'anakli/cca', 'tag' :'parsec_canneal'}
IMAGE_PER_JOB[JobKind.DEDUP] = {'name': 'anakli/cca', 'tag' :'parsec_dedup'}
IMAGE_PER_JOB[JobKind.FERRET] = {'name': 'anakli/cca', 'tag' :'parsec_ferret'}
IMAGE_PER_JOB[JobKind.FREQMINE] = {'name': 'anakli/cca', 'tag' :'parsec_freqmine'}
IMAGE_PER_JOB[JobKind.RADIX] = {'name': 'anakli/cca', 'tag' :'splash2x_radix'}
IMAGE_PER_JOB[JobKind.VIPS] ={'name': 'anakli/cca', 'tag' :'parsec_vips'} 

ta_file = "part4/target_achieved.json"
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
        buf = self.read_until(b'END')
        assert(buf is not None)
        return buf.decode('ascii')

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
        get_diff = curr_get_count - self.get_count
        self.get_count = curr_get_count
        self.last_readings.append((time.time_ns(), get_diff))
        self.cleanup()

    def cleanup(self):
        now = time.time_ns()
        delete_before = None
        for i, (t, _) in reversed(list(enumerate(self.last_readings))):
            if now - t > ONE_S_IN_NS:
                delete_before = i
                break

        if delete_before is not None:
            del self.last_readings[:delete_before]

    # queries received in the last count*10ms
    def last_measurements(self, count=10):
        total_get_after = 0
        summed = 0
        start = time.time_ns()
        end = 0
        for i, (t, g) in enumerate(reversed(self.last_readings)):
            if i >= count:
                break
            start = min(start, t)
            end = max(end, t)
            total_get_after += g
            summed += 1

        time_diff = abs(end-start)
        if time_diff == 0:
            return 0
        else:
            return int((total_get_after) / ((end-start)/ONE_S_IN_NS))

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
            time.sleep(poll_interval)
            # if i % 10 == 0:
            #     i = 0
            self.stats.read()

            if not self.qin.empty():
                (msg, arg) = self.qin.get()
                if msg == 'last_measurements':
                    self.qout.put(self.stats.last_measurements(arg))
                elif msg == 'qps':
                    self.qout.put(self.stats.qps())
                self.qin.task_done()

            # i += 1

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
            time.sleep(poll_interval)
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

HIGH_QUEUE = [JobKind.FREQMINE, JobKind.CANNEAL, JobKind.FERRET, JobKind.BLACKSCHOLES, JobKind.VIPS]
LOW_QUEUE = [JobKind.DEDUP, JobKind.RADIX]
# HIGH_QUEUE = []
# LOW_QUEUE = [JobKind.RADIX]

MEMCACHED_CORES = [0]

docker_client = docker_connect()

class Job:
    def __init__(self, logger: SchedulerLogger, job: JobKind, cores: int) -> None:
        self.logger = logger
        self.job = job
        self.id = None
        self.cores = cores
        self.update_cores(cores)

    @property
    def container(self) -> Container | None:
        if self.id is not None:
            return cast(Container, docker_client.containers.get(self.id))
        else:
            return None


    @property
    def cores_str(self) -> str:
        if self.cores == 1:
            return '1'
        elif self.cores == 2:
            return '2,3'
        elif self.cores == 3:
            return '1,2,3'
        else:
            raise Exception("unexpected number of required cores: ", self.cores)

    @property
    def cores_list(self) -> List[int]:
        if self.cores == 1:
            return [1]
        elif self.cores == 2:
            return [2,3]
        elif self.cores == 3:
            return [1,2,3]
        else:
            raise Exception("unexpected number of required cores: ", self.cores)

    def update_cores(self, cores: int) -> None:
        prev_cores = self.cores
        self.cores = cores

        if self.container is not None:
            self.container.update(cpuset_cpus=self.cores_str)
            if prev_cores != self.cores:
                self.logger.update_cores(self.job, self.cores_list)

    def run(self) -> None:
        if self.id is None:
            # the container has not been created yet
            image = IMAGE_PER_JOB[self.job]
            assert(image is not None)
            threads = min(3, floor(self.cores * 1.5))
            container = cast(Container, docker_client.containers.run(
                f"{image['name']}:{image['tag']}",
                detach=True,
                command=f"./run -a run -S {'parsec' if self.job != JobKind.RADIX else 'splash2x'} -p {self.job.value} -i native -n {threads}",
                cpuset_cpus=self.cores_str
            ))
            self.id = container.id
            self.logger.job_start(self.job, self.cores_list, threads)

    def done(self) -> None:
        self.logger.job_end(self.job)

    @property
    def status(self) -> str | None:
        container = self.container
        if container is None:
            return None

        if container.status == 'exited' or container.status == 'dead':
            return 'exited'
        elif container.status == 'paused':
            return 'paused'
        else:
            return 'running'

    @property
    def created(self) -> bool:
        return self.status is not None

    @property
    def paused(self) -> bool:
        return self.status == 'paused'

    @property
    def finished(self) -> bool:
        return self.status == 'exited'

    def pause(self) -> None:
        container = self.container
        if container is None:
            raise Exception("tried to pause an unstarted container: ", self.job)

        if not self.finished and not self.paused:
            self.logger.job_pause(self.job)
            container.pause()

    def unpause(self) -> None:
        container = self.container
        if container is None:
            raise Exception("tried to unpause an unstarted container: ", self.job)

        if not self.finished and self.paused:
            self.logger.job_unpause(self.job)
            container.unpause()

class CoreQueue:
    def __init__(self, logger: SchedulerLogger, cores: int, concurrent: int) -> None:
        self.logger = logger
        self.cores = cores
        self.concurrent = concurrent
        self.q = cast(Queue[Job], Queue())
        self.r = cast(List[Job], [])

    def append(self, jobs: List[JobKind]) -> None:
        for job in jobs:
            self.q.put(Job(self.logger, job, self.cores))

    @property
    def current(self) -> List[Job]:
        # sanity check
        for j in self.r:
            if not j.created:
                raise Exception("Job in running list doesn't have an associated container")

        return self.r

    @property
    def empty(self) -> bool:
        return len(self.current) <= 0

    @property
    def running(self) -> List[Job]:
        return [j for j in self.current if not j.paused]

    @property
    def paused(self) -> List[Job]:
        return [j for j in self.current if j.paused]

    @property
    def done(self) -> bool:
        return len(self.r) == 0 and self.q.empty()

    def fill(self) -> List[Job]:
        new = []
        while len(self.current) < self.concurrent and not self.q.empty():
            job = self.q.get()
            self.r.append(job)
            new.append(job)
            if not job.created:
                job.run()
            elif job.paused:
                job.unpause()
            self.q.task_done()
        return new
    
    def reap(self) -> List[Job]:
        done = [j for j in self.current if j.finished]
        for j in reversed(self.current):
            if j in done:
                j.done()
                self.r.remove(j)
        return done

    def disown(self, job: JobKind) -> Job:
        for j in self.current:
            if j.job == job:
                self.r.remove(j)
                return j
        raise Exception("Requested job ", job, " is not currently running")

    def own(self, job: Job) -> None:
        if not job.created:
            raise Exception("Tried to own a non-created job: ", job)

        job.update_cores(self.cores)
        self.q.put(job)

class Scheduler:
    msgq = Queue()
    def __init__(self):
        self.ct = CPUThread()
        self.logger = SchedulerLogger(to_file)

        self.memcached_pid = int(subprocess.check_output(["sudo", "systemctl", "show", "--property", "MainPID", "--value", "memcached"]))
        self.memcached_cores = 2
        self.scheduler_pid = getpid()
        self.logger.custom_event(JobKind.SCHEDULER, f"memcached pid={self.memcached_pid}")
        self.logger.custom_event(JobKind.SCHEDULER, f"scheduler pid={self.memcached_pid}")

        self.highq = CoreQueue(self.logger, 2, 2)
        self.highq.append(HIGH_QUEUE)
        self.lowq = CoreQueue(self.logger, 1, 1)
        self.lowq.append(LOW_QUEUE)
        self.unstable(0, 0)

        self.t = Thread(target=self.thread)
        self.t.daemon = True
        self.t.start()
        self.mt = MemcachedThread()

        # don't run this script alongside memcached
        self.set_affinity(self.scheduler_pid, [2,3])
        self.set_affinity(self.memcached_cores, [0,1])
        self.logger.update_cores(JobKind.MEMCACHED, [0,1])

    def set_affinity(self, pid: int, affinity: List[int]) -> None:
        proc = psutil.Process(pid)
        proc.cpu_affinity(affinity)
        for t in proc.threads():
            psutil.Process(t.id).cpu_affinity(affinity)

    def thread(self):
        i = 0
        while True:
            time.sleep(poll_interval)
            if not self.msgq.empty():
                action, cores = self.msgq.get()
                if action == 'memcached_affinity':
                    cores_list = list(range(cores))
                    self.set_affinity(self.memcached_pid, cores_list)
                    if self.memcached_cores != cores:
                        self.memcached_cores = cores
                        self.logger.update_cores(JobKind.MEMCACHED, cores_list)
                else:
                    print('unkown action', action)
                self.msgq.task_done()

            [core0, core1] = self.ct.get(10)
            unpause_lowq = self.memcached_cores == 1 and core0 < 95
            if unpause_lowq:
                for j in self.lowq.paused:
                    j.unpause()
            else:
                for j in self.lowq.running:
                    j.pause()

            if i % 10 == 0:
                i = 0
                highq_done = self.highq.reap()
                lowq_done = self.lowq.reap()
                if len(highq_done) > 0 or len(lowq_done) > 0:
                    self.logger.custom_event(JobKind.MEMCACHED, f"Some jobs are done: (high, {len(highq_done)}), (low, {len(lowq_done)})")

                self.highq.fill()
                self.lowq.fill()

                if self.highq.done and not self.lowq.done:
                    # move (up to) two jobs from the lowq to the highq
                    for j in self.lowq.current[:2]:
                        jj = self.lowq.disown(j.job)
                        assert(j == jj)
                        self.highq.own(j)

                if self.highq.done and self.lowq.done:
                    self.logger.end()

            i += 1

    @property
    def done(self) -> bool:
        return self.highq.done and self.lowq.done

    def stable(self, qps: int):
        self.logger.custom_event(JobKind.SCHEDULER, f"stable ({qps} QPS)")
        cores = 1 if max_qps_one_core > qps else 2
        self.msgq.put(('memcached_affinity', cores))

    def unstable(self, qps: int, curr: int):
        self.logger.custom_event(JobKind.SCHEDULER, f"unstable (CURR {curr} - {qps} QPS)")
        self.msgq.put(('memcached_affinity', 2))

scheduler = Scheduler()
mt = scheduler.mt

# every 50ms
sleep_interval = 5*poll_interval
stable = False
i = 0
while True:
    time.sleep(sleep_interval)
    curr = mt.do('last_measurements', int(sleep_interval//poll_interval))
    qps = mt.do('qps')
    # if abs(curr - qps) > qps_threshold:
    if abs(curr - qps)/max(1,max(curr, qps)) > 0.3:
        if stable:
            stable = False
            # print('unstable', qps, curr)
            scheduler.unstable(qps, curr)
    else:
        not_stable_before = not stable
        stable = True
        if not_stable_before or i % 10 == 0:
            # print('stable', qps)
            scheduler.stable(qps)

        if i > 10:
            i = 0
    i += 1
# while True:
#     time.sleep(4*poll_interval)
#     curr = mt.do('last_measurements', 20)
#     if len(prev_list) > 0:
#         prev = sum(prev_list)/len(prev_list)
#     else:
#         prev = 0
#
#     if scheduler.done:
#         exit(0)
#
#     prev_list.append(curr)
#     prev_list = prev_list[-keep:]
#     if abs(prev - curr) > qps_threshold:
#         if stable:
#             print('unstable')
#             # scheduler.unstable()
#             stable = False
#         div = 1+int(abs(prev - curr)/max(prev, curr) * 10)
#         last_close_enough = all(map(lambda n: curr - n < close_enough, prev_list[-(keep//div):]))
#         print('last_close_enough', last_close_enough)
#         if last_close_enough:
#             prev_list = [curr]
#     else:
#         if not stable:
#             qps = mt.do('qps')
#             print('stable', qps)
#             # scheduler.stable_qps(qps)
#             stable = True
