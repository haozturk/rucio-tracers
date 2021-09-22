#!/bin/sh
cd /data
echo $PWD
ls
ls /etc/secrets
ls -l /data/RucioTracer
/data/RucioTracer -help
/data/RucioTracer -config /etc/secrets/stompserverconfig4fwjr.json -sitemap /data/etc/ruciositemap.json -domainmap /data/etc/domainsitemap.txt
