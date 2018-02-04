# Download Impala Dataset jar
wget http://elasticmapreduce.s3.amazonaws.com/samples/impala/dbgen-1.0-jar-with-dependencies.jar

#Launch the program to create the test data 
java -cp dbgen-1.0-jar-with-dependencies.jar DBGen -p /mnt/dbgen -b 1 -c 1 -t 1

#Create a new folder in the cluster's HDFS file system
hadoop fs -mkdir /data/ 

#copy the test data from the master node's local file system to HDFS
hadoop fs -put /mnt/dbgen/* /data/ 
hadoop fs -ls -h -R /data/

