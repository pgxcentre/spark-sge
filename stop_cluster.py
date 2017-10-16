#!/usr/bin/env python

"""
Stop a Spark cluster given a spark_cluster.pkl file.
"""

import sys
import pickle
import subprocess


def main():
    if len(sys.argv) != 2:
        print("USAGE: stop_cluster.py spark_cluster.pkl")

    with open(sys.argv[1], "rb") as f:
        task_ids = pickle.load(f)

    for task_id in task_ids:
        subprocess.Popen([
            "qdel", task_id,
        ])

    print("Deleted all the registered jobs.")


if __name__ == "__main__":
    main()
