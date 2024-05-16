#!/usr/bin/env python3

from typing import List, cast, Tuple, Optional, Dict, Any
from os import getpid
from psutil import cpu_percent
import socket
import traceback
import subprocess
import time
import re
from docker import from_env as docker_connect
from docker.models.containers import Container
from queue import Queue
from threading import Thread
import psutil
from scheduler_logger import SchedulerLogger, Job as JobKind

# hyperparameters
poll_interval = 0.10 # 100ms
stability_sleep = 0.075 # 75ms
max_qps_one_core = 29_000
max_cpu_threshold = 45 # out of 200

ONE_S_IN_NS = 1e9
ONE_S_IN_US = 1e6

IMAGE_PER_JOB = {}
IMAGE_PER_JOB[JobKind.BLACKSCHOLES] = {'name':'anakli/cca', 'tag': 'parsec_blackscholes'}
IMAGE_PER_JOB[JobKind.CANNEAL] = {'name': 'anakli/cca', 'tag' :'parsec_canneal'}
IMAGE_PER_JOB[JobKind.DEDUP] = {'name': 'anakli/cca', 'tag' :'parsec_dedup'}
IMAGE_PER_JOB[JobKind.FERRET] = {'name': 'anakli/cca', 'tag' :'parsec_ferret'}
IMAGE_PER_JOB[JobKind.FREQMINE] = {'name': 'anakli/cca', 'tag' :'parsec_freqmine'}
IMAGE_PER_JOB[JobKind.RADIX] = {'name': 'anakli/cca', 'tag' :'splash2x_radix'}
IMAGE_PER_JOB[JobKind.VIPS] ={'name': 'anakli/cca', 'tag' :'parsec_vips'} 

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
    _stop = False

    def __init__(self):
        self.t = Thread(target=self.thread)
        self.t.daemon = True
        self.t.start()

    def thread(self):
        while not self._stop:
            time.sleep(poll_interval)
            self.stats.read()

            if not self.qin.empty():
                (msg, arg) = self.qin.get()
                if msg == 'last_measurements':
                    self.qout.put(self.stats.last_measurements(arg))
                elif msg == 'qps':
                    self.qout.put(self.stats.qps())
                self.qin.task_done()

    def do(self, method, arg=0):
        self.qin.put((method, arg))
        res = self.qout.get()
        self.qout.task_done()
        return res
   
    def stop(self):
        self._stop = True
        self.t.join()

class CPUThread():
    qin = Queue()
    qout = Queue()
    readings = []
    _stop = False

    def __init__(self):
        self.t = Thread(target=self.thread)
        self.t.daemon = True
        self.t.start()

    def read(self):
            cores = cpu_percent(interval=None, percpu=True)
            cores = cores[:2]
            self.readings.append(cores)
            cleanup_point =int(1/poll_interval)
            if len(self.readings) > cleanup_point:
                del self.readings[:cleanup_point]


    def avg(self, count):
        tot0 = 0
        tot1 = 0
        n = min(count, len(self.readings))
        for i in range(n):
            c0 = self.readings[i][0]
            c1 = self.readings[i][1]
            tot0 += c0 # c0 if c0 < 50 else c0*2
            tot1 += c1 # c1 if c1 < 50 else c1*2

        if n == 0:
            return [0,0]
        else:
            return [tot0/n, tot1/n]

    def thread(self):
        while not self._stop:
            # as recomended by psutil
            time.sleep(0.1)
            self.read()

            if not self.qin.empty():
                count = self.qin.get()
                self.qout.put(self.avg(count))
                self.qin.task_done()

    def get(self, readings=int(1/poll_interval)):
        self.qin.put(readings)
        res = self.qout.get()
        self.qout.task_done()
        return res

    def stop(self):
        self._stop = True
        self.t.join()


# set the affinity of this script
p = psutil.Process(getpid())
p.cpu_affinity([0])

HIGH_QUEUE: List[Tuple[JobKind, int, Optional[float]]] = [
    (JobKind.FERRET, 1, None),
    (JobKind.DEDUP, 1, None),
    (JobKind.VIPS, 1, None),
    (JobKind.BLACKSCHOLES, 1, None),
    (JobKind.CANNEAL, 1, None),
    (JobKind.FREQMINE, 2, None),
]
LOW_QUEUE: List[Tuple[JobKind, int, Optional[float]]] = [
    (JobKind.RADIX, 1, .6),
]

docker_client = docker_connect()

class Job:
    def __init__(self, logger: SchedulerLogger, job: JobKind, threads: int, cpu_percent: Optional[float]) -> None:
        self.logger = logger
        self.job = job
        self.id = None
        self.threads = threads
        self.cpu_percent = cpu_percent

        self.cores = None
        self._done = False

    @property
    def container(self) -> Optional[Container]:
        if self.id is not None:
            return cast(Container, docker_client.containers.get(self.id))
        else:
            return None

    @property
    def cores_str(self) -> Optional[str]:
        return ','.join([str(c) for c in self.cores]) if self.cores is not None else None

    @property
    def cores_list(self) -> Optional[List[int]]:
        return self.cores

    def update_cores(self, cores: List[int]) -> None:
        prev_cores = self.cores
        self.cores = cores

        if self.container is not None:
            self.container.update(cpuset_cpus=self.cores_str)
            if prev_cores != self.cores:
                self.logger.update_cores(self.job, cast(List[int], self.cores_list))

    def run(self, cores: List[int]) -> None:
        if self.id is None:
            # the container has not been created yet
            image = IMAGE_PER_JOB[self.job]
            assert(image is not None)
            self.cores = cores
            ncores = len(cores)
            opts: Dict[str, str | int] = {
                'cpuset_cpus': cast(str, self.cores_str),
            }
            if self.cpu_percent is not None:
                opts['cpu_period'] = int(ONE_S_IN_US*ncores)
                opts['cpu_quota'] = int(ONE_S_IN_US*self.cpu_percent*ncores)
            container = cast(Container, docker_client.containers.run(
                f"{image['name']}:{image['tag']}",
                detach=True,
                command=f"./run -a run -S {'parsec' if self.job != JobKind.RADIX else 'splash2x'} -p {self.job.value} -i native -n {self.threads}",
                **cast(Any, opts)
            ))
            self.id = container.id
            self.logger.job_start(self.job, cast(List[int], self.cores_list), self.threads)

    def done(self) -> None:
        if not self._done:
            self.logger.job_end(self.job)
        self._done = True

    @property
    def status(self) -> Optional[str]:
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
            container.pause()
            self.logger.job_pause(self.job)     

    def unpause(self) -> None:
        container = self.container
        if container is None:
            raise Exception("tried to unpause an unstarted container: ", self.job)

        if not self.finished and self.paused:
            container.unpause()
            self.logger.job_unpause(self.job)
            
class CoreQueue:
    def __init__(self, logger: SchedulerLogger, cores: List[int]) -> None:
        self.logger = logger
        self.cores = cores
        self.q = cast(Queue[Job], Queue())
        self.r = cast(List[Optional[Job]], [None for _ in cores])

    def append(self, jobs: List[Tuple[JobKind, int, Optional[float]]]) -> None:
        for (jk, threads, shares) in jobs:
            self.q.put(Job(self.logger, jk, threads, shares))

    @property
    def current(self) -> List[Tuple[int, Job]]:
        # sanity check
        jobs = [(i,j) for i, j in enumerate(self.r) if j is not None]
        for _, j in jobs:
            if not j.created:
                raise Exception("Job in running list doesn't have an associated container")

        return jobs

    @property
    def empty(self) -> bool:
        return len(self.current) <= 0

    @property
    def running(self) -> List[Job]:
        return [j for _, j in self.current if not j.paused]

    @property
    def paused(self) -> List[Job]:
        return [j for _, j in self.current if j.paused]

    @property
    def done(self) -> bool:
        return len([j for j in self.current if j is not None]) == 0 and self.q.empty()

    @property
    def free_space(self) -> int:
        return len(self.cores) - len(self.current)

    @property
    def has_space(self) -> bool:
        return self.q.empty() and self.free_space > 0

    def cores_per_job(self, job: Job) -> List[int]:
        return [self.cores[i] for i,j in self.current if j == job]

    def next_pos(self) -> Optional[int]:
        available_indexes = [i for i, e in enumerate(self.r) if e is None]
        return available_indexes[0] if len(available_indexes) > 0 else None

    def insert(self, job: Job):
        i = self.next_pos()
        if i is None:
            raise Exception("Tried inserting a job in a queue with no empty space")
        self.r[i] = job
        return i

    def fill(self) -> List[Job]:
        new = []
        while self.free_space > 0 and not self.q.empty():
            job = self.q.get()
            pos = self.next_pos()
            assert pos is not None
            core = self.cores[pos]
            if not job.created:
                job.run([core])
            elif job.paused:
                job.update_cores([core])
                job.unpause()
            elif job.finished:
                job.done()
                self.q.task_done()
                continue
            new.append(job)
            self.insert(job)
            self.q.task_done()
        return new
    
    def reap(self) -> List[Job]:
        done_list = [j for _, j in self.current if j.finished]
        for j in done_list:
            self.remove(j)
            j.done()

        return [j for j in done_list]

    def remove(self, j: Job) -> None:
        ii = [i for i, jj in self.current if jj == j]
        for i in ii:
            self.r[i] = None

    def disown(self) -> Job:
        job = None
        if not self.q.empty():
            job = self.q.get()
            self.q.task_done()
        elif len(self.current) > 0:
            _, job = self.current[0]
            self.remove(job)

        if job is not None:
            self.logger.custom_event(job.job, f"disowned from q: {','.join(map(str, self.cores))}")
            return job
        raise Exception("No jobs to disown")

    def upgade(self, job: Job, cores: int) -> None:
        jobi = [i for i, j in self.current if j == job]
        if len(jobi) <= 0:
            raise Exception("Tried to upgrade a job which is not currently running")

        jobi = jobi[0]
        # also handles if the job is already in this CoreQueue
        max_cores = min(cores, len([j for j in self.r if j is None or j != job]))
        self.logger.custom_event(job.job, f"upgraded by {max_cores} cores")
        for _ in range(max_cores):
            self.insert(job)
        job.update_cores(self.cores_per_job(job))

    def own(self, job: Job) -> None:
        self.q.put(job)

class Scheduler:
    _stop = False

    def __init__(self):
        self.ct = CPUThread()
        self.mt = MemcachedThread()
        self.logger = SchedulerLogger()

        self.memcached_pid = int(subprocess.check_output(["sudo", "systemctl", "show", "--property", "MainPID", "--value", "memcached"]))
        self.memcached_cores_list = None
        self.scheduler_pid = getpid()
        self.logger.custom_event(JobKind.SCHEDULER, f"memcached pid={self.memcached_pid}")
        self.logger.custom_event(JobKind.SCHEDULER, f"scheduler pid={self.memcached_pid}")

        self.highq = CoreQueue(self.logger, [2,3])
        self.highq.append(HIGH_QUEUE)
        self.lowq = CoreQueue(self.logger, [1])
        self.lowq.append(LOW_QUEUE)
        self._stable = False
        self._qps = 100000

        self.t = Thread(target=self.thread)
        self.t.daemon = True
        self.t.start()
        # don't run this script alongside memcached
        self.set_affinity(self.scheduler_pid, [2,3])
        self.memcached_cores([0,1])

    def set_affinity(self, pid: int, affinity: List[int]) -> None:
        proc = psutil.Process(pid)
        proc.cpu_affinity(affinity)
        for t in proc.threads():
            psutil.Process(t.id).cpu_affinity(affinity)

    def memcached_cores(self, cores: List[int]) -> None:
        if self.memcached_cores_list != cores:
            self.memcached_cores_list = cores
            self.set_affinity(self.memcached_pid, self.memcached_cores_list)
            self.logger.update_cores(JobKind.MEMCACHED, self.memcached_cores_list)

    def thread(self):
        i = 0
        while not self._stop:
            try:
                time.sleep(poll_interval)

                [core0, core1] = self.ct.get(1)
                unpause_lowq = self._qps < max_qps_one_core and (core0+core1) < 45 and not self.lowq.done
                if self._stable and unpause_lowq:
                    for j in self.lowq.paused:
                        j.unpause()
                    self.memcached_cores([0])
                else:
                    self.memcached_cores([0,1])
                    for j in self.lowq.running:
                        j.pause()

                # Every 200ms, update the queues
                if i % 2 == 0:
                    i = 0
                    highq_done = self.highq.reap()
                    lowq_done = self.lowq.reap()
                    if len(highq_done) > 0 or len(lowq_done) > 0:
                        self.logger.custom_event(JobKind.SCHEDULER, f"done: (high, {len(highq_done)}), (low, {len(lowq_done)})")

                    self.highq.fill()
                    self.lowq.fill()

                    # shift jobs from the lowq to the highq
                    if self.highq.has_space and not self.lowq.done:
                        self.logger.custom_event(JobKind.SCHEDULER, f"moving {self.highq.free_space} jobs to highq")
                        # move (up to) two jobs from the lowq to the highq
                        for _ in range(self.highq.free_space):
                            job = self.lowq.disown()
                            self.highq.own(job)

                    # give as many cores as possible to the jobs in the highq if
                    # there's only one (self.highq.has_space implies that there
                    # is only one)
                    if self.highq.has_space and not self.highq.done:
                        _, job = self.highq.current[0]
                        self.highq.upgade(job, 2)

                i += 1
            except Exception:
                traceback.print_exc()
                continue

    @property
    def done(self) -> bool:
        return self.highq.done and self.lowq.done

    def stable(self, qps: int):
        print(f"stable ({qps} QPS)")
        self._stable = True
        self._qps = qps

    def unstable(self, qps: int, curr: int):
        print(f"unstable (CURR {curr} - {qps} QPS)")
        self._stable = False
        self._qps = max(qps, curr)

    def stop(self):
        self._stop = True
        self.t.join()
        self.ct.stop()
        self.mt.stop()
        self.logger.end()

scheduler = Scheduler()
mt = scheduler.mt

# every 50ms
stable = False
i = 0
notify_scheduler_every = 2 # * 75ms
while True:
    time.sleep(stability_sleep)
    # take the last 400ms
    curr = mt.stats.last_measurements(4)
    qps = mt.stats.last_measurements(int(1/poll_interval))
    if abs(curr - qps)/max(1,max(curr, qps)) > 0.3:
        if stable:
            stable = False
            scheduler.unstable(qps, curr)
    else:
        not_stable_before = not stable
        stable = True
        if not_stable_before or i % notify_scheduler_every == 0:
            scheduler.stable(qps)

        if i > notify_scheduler_every:
            i = 0

    if scheduler.done:
        scheduler.stop()
        exit(0)

    i += 1
