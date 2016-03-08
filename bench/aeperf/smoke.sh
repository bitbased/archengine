#! /bin/sh

# Smoke-test aeperf as part of running "make check".
./aeperf -O `dirname $0`/runners/small-lsm.aeperf -o "run_time=20"
