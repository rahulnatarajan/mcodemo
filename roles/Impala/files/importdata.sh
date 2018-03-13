#!/bin/bash

timekey=2017-10-31
tablename=mv_mco_account

datadir=/mnt/mcodemo/data/$1
primary_table_script_dir=/mnt/mcodemo/data/hive/primary
staging_table_script_dir=/mnt/mcodemo/data/hive/staging
hadoop_datasets_dir=/mcodemo/datasets

# Check if data directory exists
if [ ! -d "$datadir" ]; then
  mkdir -p $datadir
fi

# Download from s3
echo "Downloading dataset from s3..."
aws s3 cp s3://mcodemo/datasets/$1/$2.csv $datadir/$1/$2.csv

# Check if datasets directory exists
echo "Ensuring data directory exists in hdfs..."
hadoop fs -test -d $hadoop_datasets_dir
if [ $? != 0 ]; then
  hadoop fs -mkdir -p $hadoop_datasets_dir
fi

# Ensure that directory for the timekey is created
hadoop fs -mkdir -p $hadoop_datasets_dir/$1

# Import to hdfs
echo "Importing data set to hdfs..."
hadoop fs -put $datadir/$1/$2.csv $hadoop_datasets_dir/$1/$2.csv


# Load data to table
if [ "$tablename" == "mco_entity_bookcode_mapping" ]; then
  # Special handling for tables not needing staging
  echo "Downloading create table script from s3..."
  aws s3 cp s3://mcodemo/hive/primary/$tablename.sql $primary_table_script_dir/$tablename.sql

  echo "Dropping table $tablename.."
  hive -e "drop table $tablename;"

  echo "Recreating table $tablename.."
  hive -f $primary_table_script_dir/$tablename.sql

  echo "Loading CSV for $tablename"
  hive -e "load data inpath '$hadoop_datasets_dir/$1/$2.csv' INTO TABLE $tablename"
else
  echo "Downloading create table staging ${tablename}_stg from s3..."
  aws s3 cp s3://mcodemo/hive/staging/${tablename}_stg.sql $staging_table_script_dir/${tablename}_stg.sql

  echo "Dropping table ${tablename}_staging.."
  hive -e "drop table ${tablename}_staging;"

  echo "Recreating table ${tablename}_staging.."
  hive -f $staging_table_script_dir/${tablename}_stg.sql

  echo "Loading CSV for ${tablename}_staging"
  hive -e "load data inpath '$hadoop_datasets_dir/$1/$2.csv' INTO TABLE ${tablename}_staging"

  echo "Downloading create table script from s3..."
  aws s3 cp s3://mcodemo/hive/primary/$tablename.sql $primary_table_script_dir/$tablename.sql

  echo "Dropping table $tablename.."
  hive -e "drop table $tablename;"

  echo "Recreating table $tablename.."
  hive -f $primary_table_script_dir/$tablename.sql

  echo "Downloading staging to primary migration script"
  aws s3 cp s3://mcodemo/hive/stg2primary/${tablename}_stg2prim.hql $stg2prim/${tablename}.hql

  hive -f $stg2prim/${tablename}.hql
fi
