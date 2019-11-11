#!/bin/bash
# This file could be used to prepare the environment during the cluster initialization.
# It is not used in the end because the parameters are set as properties to the job.
# This is an example that could be used for more complex use cases.

echo export SFTP_USER=XXX >>/etc/environment
echo export SFTP_HOST=XXX >>/etc/environment
echo export SFTP_PASS=XXX >>/etc/environment

echo "spark.executorEnv.SFTP_USER=XXX" >> /etc/spark/conf.dist/spark-defaults.conf
echo "spark.executorEnv.SFTP_HOST=XXX" >>  /etc/spark/conf.dist/spark-defaults.conf
echo "spark.executorEnv.SFTP_PASS=xXX" >>  /etc/spark/conf.dist/spark-defaults.conf

echo "spark.yarn.appMasterEnv.SFTP_USER=XXX" >> /etc/spark/conf.dist/spark-defaults.conf
echo "spark.yarn.appMasterEnv.SFTP_HOST=XXX" >>  /etc/spark/conf.dist/spark-defaults.conf
echo "spark.yarn.appMasterEnv.SFTP_PASS=XXX" >>  /etc/spark/conf.dist/spark-defaults.conf
