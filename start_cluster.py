#!/usr/bin/env python

"""
Start a Spark cluster on top of an SGE cluster.
"""

import pickle
import argparse
import subprocess


SERVERS = [
    ("srapl-sg-cn001", 48),
    ("srapl-sg-cn002", 48),
    ("srapl-sg-cn003", 48),
    ("srapl-sg-cn004", 48),
    ("srapl-sg-cn005", 48),
    ("srapl-sg-cnc06", 16),
    ("srapl-sg-cnc07", 16),
    ("srapl-sg-cnc08", 16),
    ("srapl-sg-cnc09", 16),
    ("srapl-sg-cnc10", 16),
    ("srapl-sg-cnc11", 16),
    ("srapl-sg-cnc12", 16),
    ("srapl-sg-cnc13", 16),
    ("srapl-sg-cnc14", 16),
]


def main(args):
    process_ids = []

    small_nodes_queue = []

    # Create the queues as arguments for qsub.
    for server, n_cpus in SERVERS:
        if n_cpus == 16:
            small_nodes_queue.extend([
                "-q", "all.q@" + server,
            ])

    command = ["qsub"]
    command.extend(small_nodes_queue)
    command.extend(["-terse", "./_start_master.sh"])

    print(command)
    p = subprocess.Popen(command, stdout=subprocess.PIPE)

    # Function to parse the process ID.
    def _strip_pid(s):
        return s.decode("utf-8").strip()

    # Remember the process ID.
    process_ids.append(_strip_pid(p.stdout.read()))

    processes = []
    for i in range(args.n_workers):
        command = [
            "qsub", "-terse", "-pe", "multiprocess", "16", "./_start_slave.sh"
        ]
        print(command)
        p = subprocess.Popen(command, stdout=subprocess.PIPE)
        processes.append(p)

    # I do this in two stages so that we don't have to wait for a task to start
    # submit the next one.
    for p in processes:
        process_ids.append(_strip_pid(p.stdout.read()))

    filename = "spark_cluster.pkl"
    print("Started the Spark cluster, saving the task IDs to '{}'."
          "".format(filename))

    with open(filename, "wb") as f:
        pickle.dump(process_ids, f)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--master-host",
        default=None
    )

    parser.add_argument(
        "--n-workers",
        default=2,
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args)
