#!/bin/bash

/home/andrey/venv/bin/python convert.py -f /user/andrey/destinations/destinations.csv -t /user/andrey/destinations/destinations.parquet
/home/andrey/venv/bin/python convert.py -f /user/andrey/train/train.csv -t /user/andrey/train/train.parquet