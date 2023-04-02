import argparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from hdfs import InsecureClient


def read_config():
    global config
    with open("config.yaml", "r") as yamlfile:
        config = yaml.safe_load(yamlfile)


def convert_csv_to_parquet(path_to_csv, path_to_parquet):
    read_config()
    # establish connection with HDFS
    client = InsecureClient(f"{config['hdfs']['url']}:{config['hdfs']['port']}", config['user'])
    chunk_size = config['chunk_size']
    # open reading from HDFS and writing to HDFS streams
    with client.read(path_to_csv, encoding='utf-8') as reader, client.write(path_to_parquet,
                                                                            overwrite=True) as write_stream:
        csv = pd.read_csv(reader, chunksize=chunk_size)
        for i, chunk in enumerate(csv):
            if i == 0:
                # schema of csv using first
                parquet_schema = pa.Table.from_pandas(df=chunk).schema
                # open a parquet file for writing
                writer = pq.ParquetWriter(write_stream, parquet_schema)
            # write chunk to the parquet file
            table = pa.Table.from_pandas(chunk)
            writer.write_table(table)
        writer.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", dest='path_to_csv', type=str, help="Path to CSV file in HDFS", required=True)
    parser.add_argument("-t", dest='path_to_parquet', type=str, help="Path for saving Parquet file in HDFS",
                        required=True)
    cmd_args = parser.parse_args()
    convert_csv_to_parquet(cmd_args.path_to_csv, cmd_args.path_to_parquet)


if __name__ == '__main__':
    main()
