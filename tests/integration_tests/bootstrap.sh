#!/usr/bin/env bash
set -euxo pipefail
shopt -s extglob

python setup.py bdist_rpm
yum install -y dist/supremm-+([0-9.])*.x86_64.rpm
~/bin/services start
rm -rf /var/lib/mongo/*
mongod -f /etc/mongod.conf
~/bin/importmongo.sh

mkdir -p /data/{phillips,pozidriv,frearson,mortorq,robertson}/{pcp-logs,jobscripts}
mkdir -p "/data/mortorq/pcp-logs/hostname/2016/12/30"

python tests/integration_tests/supremm_setup_expect.py

cp tests/integration_tests/pcp_logs_extracted/* /data/mortorq/pcp-logs/hostname/2016/12/30

# Create files containing 'job scripts' for 'start' jobs
jspath=/data/phillips/jobscripts/20170101
mkdir $jspath
for jobid in 197155 197182 197186 197199 1234234[21] 123424[]
do
    echo "Job script for job $jobid" > $jspath/$jobid.savescript
done

# Create job scripts for a submit jobs
jspath=/data/robertson/jobscripts/20161212
mkdir $jspath
for jobid in 6066098
do
    echo "Job script for job $jobid" > $jspath/$jobid.savescript
done

# Create job script for end jobs
jspath=/data/pozidriv/jobscripts/20161230
mkdir $jspath
for jobid in 983936
do
    echo "Job script for job $jobid" > $jspath/$jobid.savescript
done
