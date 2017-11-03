#!/bin/bash

#$ -V
#$ -cwd
#$ -S /bin/bash
#$ -N _spark_master


# Loading the module
module load spark/$SPARK_VERSION

# Making sure Spark is not daemonize (running in foreground)
export SPARK_NO_DAEMONIZE=yes

# Launching the master
echo "Starting master on $HOSTNAME"
$SPARK_HOME/sbin/start-master.sh
