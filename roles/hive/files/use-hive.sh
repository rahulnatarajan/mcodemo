#!/bin/sh

timekey=$1
scriptName=$2

echo "Start: $(date)" > ${scriptName}_${timekey}_hive_stat.txt

aws s3 cp s3://mcodemo/mdscripts/$scriptName.hql $scriptName.hql
hive -f $scriptName.hql > ${scriptName}_hive_out.csv
aws s3 cp ${scriptName}_hive_out.csv s3://mcodemo/md/$timekey/hive/$scriptName_hive_out.csv

echo "End: $(date)" >> ${scriptName}_${timekey}_hive_stat.txt

aws s3 cp ${scriptName}_${timekey}_hive_stat.txt s3://mcodemo/md/$timekey/stats/${scriptName}_${timekey}_hive_stat.txt
