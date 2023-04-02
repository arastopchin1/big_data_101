#!/bin/bash
sudo /usr/hdp/current/spark2-client/bin/spark-submit --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 --driver-class-path=/home/andrey/consumer/elasticsearch-hadoop-7.7.1.jar consumer.py
