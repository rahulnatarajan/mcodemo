#!/bin/sh

timekey=$1
scriptName=$2

echo "Start: $(date)" > ${scriptName}_${timekey}_sparksql.stat

aws s3 cp s3://mcodemo/mdscripts/$scriptName.hql $scriptName.hql
spark-sql -f $scriptName.hql > ${scriptName}_sparksql_out.csv
aws s3 cp ${scriptName}_sparksql_out.csv s3://mcodemo/md/$timekey/sparksql/${scriptName}_sparksql_out.csv

echo "End: $(date)" >> ${scriptName}_${timekey}_sparksql.stat

aws s3 cp ${scriptName}_${timekey}_stat.out s3://mcodemo/md/$timekey/stats/${scriptName}_${timekey}_sparksql.stat
