#!/bin/bash

one=2017-10-31
two=mv_mco_account

datadir=/mnt/mcodemo/data/$one
primary_table_script_dir=/mnt/mcodemo/data/hive/primary
staging_table_script_dir=/mnt/mcodemo/data/hive/staging
hadoop_datasets_dir=/mcodemo/datasets

# Check if data directory exists
if [ ! -d "$datadir" ]; then
  mkdir -p $datadir
fi

# Download from s3
echo "Downloading dataset from s3..."
aws s3 cp s3://mcodemo/datasets/$one/$two.csv $datadir/$two.csv

# Check if datasets directory exists
echo "Ensuring data directory exists in hdfs..."
hadoop fs -test -d $hadoop_datasets_dir
if [ $? != 0 ]; then
  hadoop fs -mkdir -p $hadoop_datasets_dir
fi

# Ensure that directory for the timekey is created
hadoop fs -mkdir -p $hadoop_datasets_dir/$one

# Import to hdfs
echo "Importing data set to hdfs..."
hadoop fs -put $datadir/$two.csv $hadoop_datasets_dir/$one/$two.csv


# Load data to table
if [ "$two" == "mco_entity_bookcode_mapping" ]; then
  # Special handling for tables not needing staging
  echo "Downloading create table script from s3..."
  aws s3 cp s3://mcodemo/hive/primary/$two.sql $primary_table_script_dir/$two.sql

  echo "Dropping table $two.."
  hive -e "drop table $two;"

  echo "Recreating table $two.."
  hive -f $primary_table_script_dir/$two.sql

  echo "Loading CSV for $two"
  hive -e "load data inpath '$hadoop_datasets_dir/$one/$two.csv' INTO TABLE $two"
else
  echo "Downloading create table staging ${two}_stg from s3..."
  aws s3 cp s3://mcodemo/hive/staging/${two}_stg.sql $staging_table_script_dir/${two}_stg.sql

  echo "Dropping table ${two}_staging.."
  hive -e "drop table ${two}_staging;"

  echo "Recreating table ${two}_staging.."
  hive -f $staging_table_script_dir/${two}_stg.sql

  echo "Loading CSV for ${two}_staging"
  hive -e "load data inpath '$hadoop_datasets_dir/$one/$two.csv' INTO TABLE ${two}_staging"

  echo "Downloading create table script from s3..."
  aws s3 cp s3://mcodemo/hive/primary/$two.sql $primary_table_script_dir/$two.sql

  echo "Dropping table $two.."
  hive -e "drop table $two;"

  echo "Recreating table $two.."
  hive -f $primary_table_script_dir/$two.sql

  echo "Downloading staging to primary migration script"
  aws s3 cp s3://mcodemo/hive/stg2primary/${two}_stg2prim.hql $stg2prim/${two}.hql

  hive -f $stg2prim/${two}.hql
fi
