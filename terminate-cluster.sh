#!/bin/bash
for ids in $(aws emr list-clusters --cluster-states TERMINATED | grep Id | cut -f 2 -d ":" | tr -d '"' | tr -d ' '| tr -d ',')
do
aws emr terminate-clusters --cluster-ids $ids
done
