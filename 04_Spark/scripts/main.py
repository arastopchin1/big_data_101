import argparse

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField, StringType

SCHEMA = StructType([
    StructField("date_time", StringType(), True),
    StructField("site_name", IntegerType(), True),
    StructField("posa_continent", IntegerType(), True),
    StructField("user_location_country", IntegerType(), True),
    StructField("user_location_region", IntegerType(), True),
    StructField("user_location_city", IntegerType(), True),
    StructField("orig_destination_distance", DoubleType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("is_mobile", IntegerType(), True),
    StructField("is_package", IntegerType(), True),
    StructField("channel", IntegerType(), True),
    StructField("srch_ci", StringType(), True),
    StructField("srch_co", StringType(), True),
    StructField("srch_adults_cnt", IntegerType(), True),
    StructField("srch_children_cnt", IntegerType(), True),
    StructField("srch_rm_cnt", IntegerType(), True),
    StructField("srch_destination_id", IntegerType(), True),
    StructField("srch_destination_type_id", IntegerType(), True),
    StructField("is_booking", IntegerType(), True),
    StructField("cnt", IntegerType(), True),
    StructField("hotel_continent", IntegerType(), True),
    StructField("hotel_country", IntegerType(), True),
    StructField("hotel_market", IntegerType(), True),
    StructField("hotel_cluster", IntegerType(), True)
])


def read_parser_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--path", dest='path_to_csv', type=str, help="Path to CSV file in HDFS", required=True)
    parser.add_argument("-t", "--task", dest='task_id', type=int, help="Task to perform", required=True)
    cmd_args = parser.parse_args()
    return cmd_args


def task_1_perform_query(data_frame: DataFrame) -> DataFrame:
    # function finding top 3 most popular hotels between couples.
    return (
        data_frame.select('hotel_continent', 'hotel_country', 'hotel_market', 'srch_adults_cnt')
        .filter(data_frame.srch_adults_cnt == 2)
        .groupBy('hotel_continent', 'hotel_country', 'hotel_market')
        .count()
        .sort(desc('count'))
        .limit(3)
    )


def task_2_perform_query(data_frame: DataFrame) -> DataFrame:
    # function finding the most popular country where hotels are booked and searched from the same country.
    return (
        data_frame.select('hotel_country', 'user_location_country', 'is_booking')
        .filter((data_frame.user_location_country == data_frame.hotel_country) & (data_frame.is_booking == 1))
        .groupBy('hotel_country')
        .count()
        .sort(desc('count'))
        .limit(1)
    )


def task_3_perform_query(data_frame: DataFrame) -> DataFrame:
    # function finding 3 hotels where people with children are interested but not booked in the end.
    return (
        data_frame.select('hotel_country', 'hotel_continent', 'hotel_market', 'srch_children_cnt', 'is_booking')
        .filter((data_frame.srch_children_cnt > 0) & (data_frame.is_booking == 0))
        .groupBy('hotel_continent', 'hotel_country', 'hotel_market')
        .count()
        .sort(desc('count'))
        .limit(3)
    )


def main():
    cmd_args = read_parser_args()
    if cmd_args.task_id not in (1, 2, 3):
        print("You've typed incorrect task id. Please, choose task 1 to 3.")
        raise SystemExit

    # reading config, defining data source URL
    try:
        with open("config.yaml", "r") as yaml_file:
            config = yaml.safe_load(yaml_file)
        spark = SparkSession.builder.appName("BigData_101_task_05").getOrCreate()
        data = spark.read.csv(f"{config['url']}{cmd_args.path_to_csv}", schema=SCHEMA, header=True)
    except:
        config = input('Data source URL was not defined, please, type URL manually (hdfs://xxx.xx.x.x): ')
        spark = SparkSession.builder.appName("BigData_101_task_05").getOrCreate()
        data = spark.read.csv(f"{config}{cmd_args.path_to_csv}", schema=SCHEMA, header=True)

    if cmd_args.task_id == 1:
        print("Performing task #1: Find top 3 most popular hotels between couples.")
        task_1_perform_query(data).show()
    elif cmd_args.task_id == 2:
        print("Performing task #2: Find the most popular country where hotels are booked and searched from the same "
              "country.")
        task_2_perform_query(data).show()
    elif cmd_args.task_id == 3:
        print("Performing task #3: Find top 3 hotels where people with children are interested but not booked in the "
              "end.")
        task_3_perform_query(data).show()


if __name__ == '__main__':
    main()
