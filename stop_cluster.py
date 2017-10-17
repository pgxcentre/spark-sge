#!/usr/bin/env python

"""
Stop a Spark cluster given a spark_cluster.pkl file.
"""

from __future__ import print_function

import sys
import time
import pickle
import subprocess


def main():
    if len(sys.argv) != 2:
        print("USAGE: stop_cluster.py spark_cluster.pkl")

    with open(sys.argv[1], "rb") as f:
        task_ids = pickle.load(f)

    # Deleting the tasks in reverse order (master is the first)
    for task_id in task_ids[::-1]:
        print("Deleting task", task_id)
        subprocess.Popen([
            "qdel", task_id,
        ])
        time.sleep(1)

    print("Deleted all the registered jobs.")


if __name__ == "__main__":
    main()
