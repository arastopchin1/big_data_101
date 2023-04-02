import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + '/../')

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField
from pyspark.sql.types import Row

from scripts.main import task_1_perform_query
from scripts.main import task_2_perform_query
from scripts.main import task_3_perform_query


@pytest.fixture(scope='session')
def spark_fixture():
    return SparkSession.builder.appName("Test_BigData_101_task_05").getOrCreate()


@pytest.fixture(scope='session')
def testing_data_fixture(spark_fixture):
    schema = StructType([
        StructField("hotel_continent", IntegerType(), True),
        StructField("hotel_country", IntegerType(), True),
        StructField("hotel_market", IntegerType(), True),
        StructField("srch_adults_cnt", IntegerType(), True),
        StructField("user_location_country", IntegerType(), True),
        StructField("is_booking", IntegerType(), True),
        StructField("srch_children_cnt", IntegerType(), True)
    ])
    correct_data = [
        Row(hotel_continent=1, hotel_country=6, hotel_market=35, srch_adults_cnt=2, user_location_country=6,
            is_booking=1,
            srch_children_cnt=1),
        Row(hotel_continent=1, hotel_country=10, hotel_market=50, srch_adults_cnt=1, user_location_country=10,
            is_booking=0,
            srch_children_cnt=2),
        Row(hotel_continent=1, hotel_country=10, hotel_market=50, srch_adults_cnt=1, user_location_country=10,
            is_booking=1,
            srch_children_cnt=1),
        Row(hotel_continent=1, hotel_country=10, hotel_market=50, srch_adults_cnt=2, user_location_country=10,
            is_booking=0,
            srch_children_cnt=2),
        Row(hotel_continent=1, hotel_country=10, hotel_market=50, srch_adults_cnt=2, user_location_country=10,
            is_booking=0,
            srch_children_cnt=2),
        Row(hotel_continent=1, hotel_country=10, hotel_market=50, srch_adults_cnt=2, user_location_country=10,
            is_booking=1,
            srch_children_cnt=0),
        Row(hotel_continent=2, hotel_country=15, hotel_market=40, srch_adults_cnt=2, user_location_country=100,
            is_booking=0, srch_children_cnt=1),
        Row(hotel_continent=2, hotel_country=18, hotel_market=50, srch_adults_cnt=2, user_location_country=18,
            is_booking=1,
            srch_children_cnt=0),
        Row(hotel_continent=2, hotel_country=50, hotel_market=900, srch_adults_cnt=1, user_location_country=60,
            is_booking=1, srch_children_cnt=0),
        Row(hotel_continent=2, hotel_country=50, hotel_market=900, srch_adults_cnt=1, user_location_country=60,
            is_booking=1, srch_children_cnt=1),
        Row(hotel_continent=2, hotel_country=50, hotel_market=900, srch_adults_cnt=1, user_location_country=60,
            is_booking=1, srch_children_cnt=1),
        Row(hotel_continent=2, hotel_country=50, hotel_market=900, srch_adults_cnt=2, user_location_country=50,
            is_booking=1, srch_children_cnt=0),
        Row(hotel_continent=2, hotel_country=50, hotel_market=900, srch_adults_cnt=2, user_location_country=50,
            is_booking=1, srch_children_cnt=0),
        Row(hotel_continent=2, hotel_country=50, hotel_market=900, srch_adults_cnt=2, user_location_country=50,
            is_booking=1, srch_children_cnt=0),
        Row(hotel_continent=2, hotel_country=50, hotel_market=900, srch_adults_cnt=2, user_location_country=50,
            is_booking=1, srch_children_cnt=0),
        Row(hotel_continent=2, hotel_country=50, hotel_market=900, srch_adults_cnt=2, user_location_country=50,
            is_booking=1, srch_children_cnt=1),
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, srch_adults_cnt=1, user_location_country=60,
            is_booking=1, srch_children_cnt=1),
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, srch_adults_cnt=1, user_location_country=70,
            is_booking=0, srch_children_cnt=2),
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, srch_adults_cnt=1, user_location_country=70,
            is_booking=1, srch_children_cnt=1),
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, srch_adults_cnt=2, user_location_country=15,
            is_booking=0, srch_children_cnt=2),
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, srch_adults_cnt=2, user_location_country=15,
            is_booking=0, srch_children_cnt=2),
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, srch_adults_cnt=2, user_location_country=15,
            is_booking=0, srch_children_cnt=2),
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, srch_adults_cnt=2, user_location_country=15,
            is_booking=1, srch_children_cnt=0),
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, srch_adults_cnt=2, user_location_country=35,
            is_booking=0, srch_children_cnt=2),
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, srch_adults_cnt=2, user_location_country=35,
            is_booking=1, srch_children_cnt=0),
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, srch_adults_cnt=2, user_location_country=35,
            is_booking=1, srch_children_cnt=1),
        Row(hotel_continent=3, hotel_country=80, hotel_market=30, srch_adults_cnt=2, user_location_country=100,
            is_booking=0, srch_children_cnt=1),
        Row(hotel_continent=3, hotel_country=90, hotel_market=70, srch_adults_cnt=2, user_location_country=90,
            is_booking=0,
            srch_children_cnt=2),
        Row(hotel_continent=3, hotel_country=90, hotel_market=70, srch_adults_cnt=2, user_location_country=90,
            is_booking=0,
            srch_children_cnt=2),
        Row(hotel_continent=4, hotel_country=5, hotel_market=10, srch_adults_cnt=2, user_location_country=100,
            is_booking=1,
            srch_children_cnt=1),
        Row(hotel_continent=4, hotel_country=5, hotel_market=35, srch_adults_cnt=2, user_location_country=5,
            is_booking=1,
            srch_children_cnt=0),
        Row(hotel_continent=5, hotel_country=10, hotel_market=20, srch_adults_cnt=2, user_location_country=100,
            is_booking=1, srch_children_cnt=1)
    ]
    return spark_fixture.createDataFrame(correct_data, schema=schema)


def test_task_1_perform_query(spark_fixture, testing_data_fixture):
    schema_correct_task_1 = StructType([
        StructField("hotel_continent", IntegerType(), True),
        StructField("hotel_country", IntegerType(), True),
        StructField("hotel_market", IntegerType(), True),
        StructField("count", IntegerType(), False)
    ])

    correct_data_task_1 = [
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, count=7),
        Row(hotel_continent=2, hotel_country=50, hotel_market=900, count=5),
        Row(hotel_continent=1, hotel_country=10, hotel_market=50, count=3)
    ]

    correct_result_task_1 = spark_fixture.createDataFrame(correct_data_task_1, schema=schema_correct_task_1)
    result_task_1 = task_1_perform_query(testing_data_fixture)
    assert correct_result_task_1.exceptAll(result_task_1).count() == 0


def test_task_2_perform_query(spark_fixture, testing_data_fixture):
    schema_correct_task_2 = StructType([
        StructField("hotel_country", IntegerType(), True),
        StructField("count", IntegerType(), False)
    ])

    correct_data_task_2 = [
        Row(hotel_country=50, count=5)
    ]
    correct_result_task_2 = spark_fixture.createDataFrame(correct_data_task_2, schema=schema_correct_task_2)
    result_task_2 = task_2_perform_query(testing_data_fixture)
    assert correct_result_task_2.exceptAll(result_task_2).count() == 0


def test_task_3_perform_query(spark_fixture, testing_data_fixture):
    schema_correct_task_3 = StructType([
        StructField("hotel_continent", IntegerType(), True),
        StructField("hotel_country", IntegerType(), True),
        StructField("hotel_market", IntegerType(), True),
        StructField("count", IntegerType(), False)
    ])

    correct_data_task_3 = [
        Row(hotel_continent=3, hotel_country=70, hotel_market=450, count=5),
        Row(hotel_continent=1, hotel_country=10, hotel_market=50, count=3),
        Row(hotel_continent=3, hotel_country=90, hotel_market=70, count=2)
    ]

    correct_result_task_3 = spark_fixture.createDataFrame(correct_data_task_3, schema=schema_correct_task_3)
    result_task_3 = task_3_perform_query(testing_data_fixture)
    assert correct_result_task_3.exceptAll(result_task_3).count() == 0
