#!/bin/sh
cd /data
echo $PWD
ls
ls /etc/secrets
ls -l /data/generators
/data/generators -config /etc/secrets/stompserverconfig4fwjr.json -infile wmarchive_sample_2.json -nrec 2 -producer wmarchive
