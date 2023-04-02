#!/bin/bash
sudo /usr/hdp/current/spark2-client/bin/spark-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 consumer.py