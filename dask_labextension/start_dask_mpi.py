import argparse
import asyncio
import os
import shlex
import shutil
import signal
import subprocess

class MyError(Exception):
    pass

def main():
    run(parse_args())

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", "-A",
            help="Account if not default")
    parser.add_argument("--nodes", "-N", 
            help="Number of nodes [%(default)d]",
            default=1)
    parser.add_argument("--ntasks", "-n", 
            help="Number of tasks to launch [%(default)d]",
            default=32)
    parser.add_argument("--cpus-per-task", "-c", 
            help="CPUs per task [%(default)d]", 
            default=2)
    parser.add_argument("--time", "-t", 
            help="Time limit in minutes [%(default)d]",
            default=30)
    parser.add_argument("--constraint", "-C", 
            help="Constraint [%(default)s]",
            choices=["haswell", "knl"], 
            default="haswell")
    parser.add_argument("--qos", "-q", 
            help="QOS to submit to [%(default)s]",
            default="interactive")
    parser.add_argument("--image",
            help="Shifter image. If set, requires also --python and --dask-mpi")
    parser.add_argument("--python",
            help="Path to python executable, can be discovered if not using Shifter")
    parser.add_argument("--dask-mpi",
            help="Path to dask-mpi executable, can be discovered if not using Shifter")
    parser.add_argument("--scheduler-file", 
            help="Scheduler file name [%(default)s]",
            default="scheduler.json")
    parser.add_argument("--verbose", 
            help="Print out command to be submitted",
            action="store_true")
    parser.add_argument("--test", 
            help="Print command but don't submit",
            action="store_true")
    parser.add_argument("--confirm", 
            help="Confirm before submission",
            action="store_true")
    parser.add_argument("--scheduler-location",
            help="Specifies the URL that an existing scheduler is located at")
    args = parser.parse_args()

    if args.image and not (args.python and args.dask_mpi):
        raise ValueError("Shifter image specified but python or dask-mpi executable not set")

    args.python = args.python or shutil.which("python")
    args.dask_mpi = args.dask_mpi or shutil.which("dask-mpi")

    return args

def run(args):
    set_omp_num_threads(args)
    clear_old_scheduler_file(args)
    command = format_command(args)
    if args.verbose or args.test or args.confirm:
        print(command)
        print()
    if args.test:
        print("Test only, exiting")
        exit()
    if args.confirm and input("Are you sure? (y/n) ") != "y":
        exit()
    command = " ".join(shlex.split(command))
    print(command)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_process(command))
    loop.close()

async def run_process(command):
    process = await asyncio.create_subprocess_shell(command)
    await process.communicate()

def set_omp_num_threads(args):
    if os.environ.get("OMP_NUM_THREADS"):
        return
    os.environ["OMP_NUM_THREADS"] = str(omp_num_threads(args))

def omp_num_threads(args):
    if args.constraint == "haswell":
        return args.cpus_per_task // 2
    if args.constraint == "knl":
        return args.cpus_per_task // 4

def clear_old_scheduler_file(args):
    try:
        os.remove(args.scheduler_file)
    except:
        pass

def format_command(args):
    if args.account:
        account_option = f"--account={args.account}"
    else:
        account_option = ""
    return f"""
    salloc
        {account_option}
        --nodes={args.nodes}
        --ntasks={args.ntasks}
        --cpus-per-task={args.cpus_per_task}
        --time={args.time}
        --constraint={args.constraint}
        --qos={args.qos}
        srun -u 
            {args.python} -u {args.dask_mpi} 
                --scheduler-file={args.scheduler_file}
                --dashboard-address=0
                --nthreads=1
                --memory-limit=0
                --no-nanny
                --local-directory=/tmp"""

if __name__ == "__main__":
    main()
