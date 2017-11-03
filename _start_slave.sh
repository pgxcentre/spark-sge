#!/bin/bash

#$ -V
#$ -cwd
#$ -S /bin/bash
#$ -N _spark_slave


# Loading the module
module load spark/$SPARK_VERSION

# Making sure Spark is not daemonize (running in foreground)
export SPARK_NO_DAEMONIZE=yes

# Creating the worker directory
mkdir -p $SPARK_HOME/work/worker_$SPARK_SLAVE_NB

echo "Starting spark slave"
$SPARK_HOME/sbin/start-slave.sh \
    -c $SPARK_SLAVES_NB_CORES \
    -d $SPARK_HOME/work/worker_$SPARK_SLAVE_NB \
    $SPARK_MASTER_HOSTNAME
