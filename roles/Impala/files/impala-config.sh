# Create a folder named Test
mkdir test

# Download Impala Dataset jar
cd test && wget http://elasticmapreduce.s3.amazonaws.com/samples/impala/dbgen-1.0-jar-with-dependencies.jar && java -cp dbgen-1.0-jar-with-dependencies.jar DBGen -p /mnt/dbgen -b 1 -c 1 -t 1 && hadoop fs -mkdir /data/ && hadoop fs -put /mnt/dbgen/* /data/ && hadoop fs -ls -h -R /data/

#Create Books Table
impala-shell -q create EXTERNAL TABLE books( id BIGINT, isbn STRING, category STRING, publish_date TIMESTAMP, publisher STRING, price FLOAT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '/data/books/';

#Create Books Customers
impala-shell -q create EXTERNAL TABLE customers( id BIGINT, name STRING, date_of_birth TIMESTAMP, gender STRING, state STRING, email STRING, phone STRING ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '/data/customers/'; 

#Create Books Transcations
impala-shell -q create EXTERNAL TABLE transactions( id BIGINT, customer_id BIGINT, book_id BIGINT, quantity INT, transaction_date TIMESTAMP ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '/data/transactions/';
