#!/usr/bin/env python

"""
Start a Spark cluster on top of an SGE cluster.
"""


from __future__ import print_function

import os
import re
import time
import pickle
import argparse
import subprocess
from os import path


def main(args):
    # Getting the binary path
    bin_path = path.abspath(path.dirname(__file__))

    # The job IDs (the first one is always the worker
    job_ids = []

    # The master command
    master_command = ["qsub", "-terse"]
    if args.master_host is not None:
        master_command.extend(["-q", args.master_host])
    master_command.append(path.join(bin_path, "_start_master.sh"))

    # Launching the command
    p = subprocess.Popen(master_command, stdout=subprocess.PIPE)

    # Remember the process ID.
    job_ids.append(_strip_pid(p.stdout.read()))

    # Getting the host on which the job is running
    master_hostname = get_hostname_of_job_id(job_ids[0])
    monitor_hostname = get_monitor_hostname(master_hostname)
    print("Master running at", master_hostname)
    print("Spark server monitor at", monitor_hostname)

    processes = []
    for i in range(args.nb_workers):
        options = ["SPARK_SLAVES_NB_CORES={}".format(args.nb_cpus),
                   "SPARK_MASTER_HOSTNAME={}".format(master_hostname)]
        command = [
            "qsub", "-terse", "-pe", "multiprocess", str(args.nb_cpus),
            "-v", ",".join(options), path.join(bin_path, "_start_slave.sh"),
        ]
        p = subprocess.Popen(command, stdout=subprocess.PIPE)
        processes.append(p)

    # I do this in two stages so that we don't have to wait for a task to start
    # submit the next one.
    for p in processes:
        job_ids.append(_strip_pid(p.stdout.read()))

    filename = "spark_cluster.pkl"
    print("Started the Spark cluster, saving the task IDs to '{}'."
          "".format(filename))

    with open(filename, "wb") as f:
        pickle.dump(job_ids, f)


def get_hostname_of_job_id(job_id):
    # Checking if the job was launched
    command = ["qstat", "-j", job_id]

    # Waiting that the job is launched
    while True:
        time.sleep(1)
        p = subprocess.Popen(command, stdout=subprocess.PIPE)
        job_info = p.stdout.read().decode("utf-8")
        if u"usage" in job_info:
            break

    # Waiting that the log file appears (NFS, network issue, etc)
    log_file = "_spark_master.e{}".format(job_id)
    while not path.isfile(log_file):
        time.sleep(1)
    while os.stat(log_file).st_size < 1:
        time.sleep(1)

    # Reading the log
    hostname = None
    while hostname is None:
        with open(log_file) as f:
            for line in f:
                if "INFO Master: Starting Spark master at" in line:
                    hostname = re.search(r"spark://\S+", line).group()
        time.sleep(1)

    return hostname


def get_monitor_hostname(hostname):
    return re.sub("[0-9]+$", "8080", re.sub("^spark", "http", hostname))


def _strip_pid(s):
    return s.decode("utf-8").strip()


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--master-host", metavar="HOST",
        help="The host of the master (e.g. all.q@srapl-sg-cnc14).",
    )

    parser.add_argument(
        "--nb-workers", metavar="INT", default=2, type=int,
        help="The number of workers to spawn. [%(default)d]",
    )
    parser.add_argument(
        "--nb-cpus", metavar="INT", default=10, type=int,
        help="The number of CPUs for each worker. [%(default)d]",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args)
