#!/bin/sh

timekey=$1
scriptName=$2

echo "Start: $(date)" > ${scriptName}_${timekey}_presto_stat.txt

aws s3 cp s3://mcodemo/mdscripts/$scriptName.hql $scriptName.hql
presto-cli --catalog hive --schema default -f $scriptName.hql > ${scriptName}_presto_out.csv
aws s3 cp ${scriptName}_presto_out.csv s3://mcodemo/md/$timekey/presto/${scriptName}_presto_out.csv

echo "End: $(date)" >> ${scriptName}_${timekey}_presto_stat.txt

aws s3 cp ${scriptName}_${timekey}_presto_stat.txt s3://mcodemo/md/$timekey/stats/${scriptName}_${timekey}_presto_stat.txt

