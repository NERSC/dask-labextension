from distributed.deploy import Cluster, SpecCluster
from distributed.deploy.spec import ProcessInterface
from tornado.ioloop import IOLoop
from dask_jobqueue import JobQueueCluster
import logging
import dask
import asyncio
import subprocess
import os
from dask_jobqueue.core import Job
from distributed.deploy.spec import ProcessInterface, Scheduler, LoopRunner
from distributed.security import Security
from distributed.utils import format_bytes, parse_bytes, tmpfile
from concurrent.futures import ThreadPoolExecutor
from distributed.core import rpc, CommClosedError, Status
from distributed.utils import import_term, suppress
import shlex
import tempfile
import shutil
import json
import os


class SLURMJob(ProcessInterface):
    def __init__(
        self,
        scheduler=None,
        name=None,
        queue=None,
        project=None,
        walltime=None,
        job_cpu=None,
        job_mem=None,
        job_extra=None
    ):
        super().__init__()
        self.config_name = "salloc_job"
        if queue is None:
            queue = dask.config.get("jobqueue.%s.queue" % self.config_name)
        if project is None:
            project = dask.config.get("jobqueue.%s.project" % self.config_name)
        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % self.config_name)
        if job_cpu is None:
            job_cpu = dask.config.get("jobqueue.%s.job-cpu" % self.config_name)
        if job_mem is None:
            job_mem = dask.config.get("jobqueue.%s.job-mem" % self.config_name)
        if job_extra is None:
            job_extra = dask.config.get("jobqueue.%s.job-extra" % self.config_name)
        account_command = None

        self.command_format = ["salloc"]
        if account_command:
            self.command_format.append(str(account_command))
        if job_cpu:
            self.command_format.append(f"--nodes={job_cpu}")
        if walltime:
            self.command_format.append(f"--time={walltime}")
        self.command_format.append(f"--constraint=haswell")
        if job_mem:
            self.command_format.append(f"--mem={job_mem}")

        self.scheduler = scheduler
        self.out, self.err = None, None

    async def start(self):
        """ Submit the process to the resource manager

        For workers this doesn't have to wait until the process actually starts,
        but can return once the resource manager has the request, and will work
        to make the job exist in the future

        For the scheduler we will expect the scheduler's ``.address`` attribute
        to be avaialble after this completes.
        """
        self.status = "running"
        self.command_format.extend("srun", "-u", "dask-mpi", str(self.scheduler))
        self.job_process = await asyncio.create_subprocess_shell(" ".join(self.command_format))

    async def close(self):
        """ Close the process

        This will be called by the Cluster object when we scale down a node,
        but only after we ask the Scheduler to close the worker gracefully.
        This method should kill the process a bit more forcefully and does not
        need to worry about shutting down gracefully
        """
        self.status = "closed"
        self.job_process.terminate()
        self._event_finished.set()

    async def finished(self):
        """ Wait until the server has finished """
        await self.job_process.wait()
        await self._event_finished.wait()

    def __repr__(self):
        return "<%s: status=%s>" % (type(self).__name__, self.status)


class InteractiveSLURMJob(Job):
    submit_command = f"python {os.path.dirname(os.path.realpath(__file__))}/start-dask-mpi.py --dask-mpi "
    cancel_command = "scancel"
    config_name = "slurm"

    def __init__(
        self,
        scheduler=None,
        name=None,
        queue=None,
        project=None,
        walltime=None,
        job_cpu=None,
        job_mem=None,
        job_extra=None,
        config_name=None,
        **base_class_kwargs
    ):
        super().__init__(
            scheduler=scheduler, name=name, config_name=config_name, **base_class_kwargs
        )

        if queue is None:
            queue = dask.config.get("jobqueue.%s.queue" % self.config_name)
        if project is None:
            project = dask.config.get("jobqueue.%s.project" % self.config_name)
        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % self.config_name)
        if job_cpu is None:
            job_cpu = dask.config.get("jobqueue.%s.job-cpu" % self.config_name)
        if job_mem is None:
            job_mem = dask.config.get("jobqueue.%s.job-mem" % self.config_name)
        if job_extra is None:
            job_extra = dask.config.get("jobqueue.%s.job-extra" % self.config_name)

        self.job_header = ""


class InteractiveSLURMCluster(JobQueueCluster):
    job_cls = InteractiveSLURMJob


class SLURMFixedCluster(Cluster):
    def __init__(
        self,
        n_workers=1,
        # Cluster keywords
        loop=None,
        security=None,
        silence_logs="error",
        name=None,
        # Scheduler-only keywords
        scheduler_options=None,
        # Options for both scheduler and workers
        interface=None,
        protocol="tcp://",
        # Job keywords
        config_name=None,
        # Some SLURM only keywords
        scheduler_file: str = None,
        local_scheduler: bool = False,
        timeout: int = 500,
        **kwargs
    ):
        self.status = "created"
        self.name = name

        if scheduler_file is None:
            scheduler_file = "~/scheduler.json"

        scheduler_file = os.path.expanduser(scheduler_file)
        if os.path.exists(scheduler_file):
            raise ValueError("A scheduler file was already found at " + scheduler_file)
        # if interface is None:
        #     interface = dask.config.get("jobqueue.%s.interface" % config_name)
        if scheduler_options is None:
            scheduler_options = {}

        default_scheduler_options = {
            "protocol": protocol,
            "dashboard_address": ":8787",
            "security": security,
        }

        if local_scheduler:
            scheduler_options = dict(default_scheduler_options, **scheduler_options)
            self.scheduler_spec = {
                "cls": Scheduler,  # Use local scheduler for now
                "options": scheduler_options,
            }
        self.scheduler_file = scheduler_file

        kwargs["config_name"] = config_name
        kwargs["interface"] = interface

        self.protocol = protocol
        self.security = security or Security()
        self._kwargs = kwargs
        self.worker = None
        self.local = local_scheduler

        default_worker_options = {
            "cores": 1,
            "memory": "1GB",
            "python": shutil.which("python"),
            "dask_path": shutil.which("dask-mpi"),
            "threads_per_cpu": 4,
            "time": None,
            "constraint": None,
            "qos": None,
            "memory_limit": "0.5"
        }

        self.n_workers = 0

        self.worker_fields = default_worker_options.keys()
        self.worker_options = default_worker_options
        if kwargs is not None:
            self.worker_options.update({key: value for key, value in kwargs.items() if value is not None})
        print(kwargs)
        self._loop_runner = LoopRunner(asynchronous=True)
        self._loop = self._loop_runner.loop

        self._lock = asyncio.Lock()
        self._loop_runner.start()
        self._supports_scaling = False

        self.worker_count = int(n_workers)
        self.nodes = int(self.worker_options["cores"])
        self.memory = self.worker_options["memory"]
        self.threads_per_cpu = int(self.worker_options["threads_per_cpu"])
        self.time = self.worker_options["time"]
        self.constraint = self.worker_options["constraint"]
        self.qos = self.worker_options["qos"]
        self.dask_path = self.worker_options["dask_path"]
        self.memory_limit = self.worker_options["memory_limit"]
        
        # A really dirty hack to ensure that mpirun is in the desired path
        self.mpirun_path = os.path.join(os.path.dirname(self.dask_path), "mpirun")

        self.timeout = int(timeout)
        print(self.dask_path)
        if self.worker_options["python"] is None:
            raise ValueError("Python path was unspecified and was not found in the environment path")
        if self.dask_path is None:
            raise ValueError("Dask MPI path was unspecified and was not found in the environment path")

        super().__init__(
            asynchronous=True
        )

    async def _start(self):
        async with self._lock:
            while self.status == "starting":
                await asyncio.sleep(0.01)
            if self.status == "running":
                return
            if self.status == "closed":
                raise ValueError("Cluster is closed")
            self.status = "starting"

        if self.local:
            if self.scheduler_spec is None:
                raise ValueError("No scheduler was set")

            scheduler_clz = self.scheduler_spec["cls"]
            if isinstance(scheduler_clz, str):
                scheduler_clz = import_term(scheduler_clz)
            self.scheduler = scheduler_clz(**self.scheduler_spec.get("options", {}))
            self.scheduler = await self.scheduler
            self.scheduler_comm = rpc(
                getattr(self.scheduler, "external_address", None) or self.scheduler.address,
                connection_args=self.security.get_connection_args("client"),
            )
            with open(self.scheduler_file, 'w+') as f:
                json.dump({
                    'address': self.scheduler.address
                }, f)
            await self._spawn_workers()
        else:
            await self._spawn_workers()
            counter = 1
            while not os.path.exists(self.scheduler_file):
                await asyncio.sleep(5.0)
                counter += 5
                if self.worker.returncode is not None or counter > int(self.timeout):
                    _, err = await self.worker.communicate()
                    self.worker.terminate()
                    raise ValueError("Scheduler failed to spawn. The request for a job returned: " + err)
            with open(self.scheduler_file, "r") as scheduler_file:
                obj = json.load(scheduler_file)
                self.scheduler_comm = rpc(obj["address"])

        await super()._start()

    async def _spawn_workers(self):
        if self.status == "running":
            return
        account_option = ""
        command = [
            "salloc"
        ]
        if account_option:
            command.append(account_option)
        command.append(f"--nodes={self.nodes}")
        command.append(f"--ntasks={self.nodes}")
        if self.time:
            command.append(f"--time={self.time}")
        if self.constraint:
            command.append(f"--constraint={self.constraint}")
        if self.qos:
            command.append(f"--qos={self.qos}")
        if self.memory:
            command.append(f"--mem={self.memory}")
        # "srun", "-u", self.worker_options["python"], "-u", 
        command.extend([self.mpirun_path, "-np", str(self.nodes), self.dask_path])

        if not self.local:
            command.append(f"--scheduler-file={self.scheduler_file}")
            command.append("--no-nanny")
        else:
            command.append(f"--scheduler-file={self.scheduler_file}")
            command.append("--no-scheduler")
        command.extend([f"--memory-limit={self.memory_limit}",
                        f"--nthreads={self.threads_per_cpu}",
                        "--local-directory=/tmp"])
        command = " ".join(command)
        logging.getLogger("SLURMFixedCluster").info(command)
        for _ in range(int(self.worker_count)):
            self.worker = await asyncio.create_subprocess_shell(command,
                                                                stdout=asyncio.subprocess.PIPE,
                                                                stderr=asyncio.subprocess.PIPE)
        # let's wait for a new line to ensure that the scheduler file is potentially created

    async def _close(self):
        async with self._lock:
            while self.status == Status.closing:
                await asyncio.sleep(0.1)
            if self.status == Status.closed:
                return
            self.status = Status.closing
            with suppress(CommClosedError):
                if self.scheduler_comm:
                    await self.scheduler_comm.close(close_workers=True)
            self.worker.terminate()
            self.worker = None
        if self.scheduler:
            await self.scheduler.close()

        await super()._close()

    def __await__(self):
        async def _():
            await self._start()
            assert self.status == Status.running
            return self

        return _().__await__()
