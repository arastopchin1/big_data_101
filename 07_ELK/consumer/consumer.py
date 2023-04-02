import yaml
from pyspark.sql import SparkSession


def read_config():
    with open("config.yaml", "r") as yaml_file:
        return yaml.safe_load(yaml_file)


def main():
    config = read_config()
    spark = SparkSession.builder.master("local[2]").appName("Big_Data_ELK_Homework").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    topic = 'test'
    server = f"{config['bootstrap_servers']['host']}:{config['bootstrap_servers']['port']}"
    checkpoint = f"{config['path']}{config['checkpointLocation']}"

    # read stream for getting data from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", server)
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST(value AS STRING)", "CAST(timestamp as TIMESTAMP)")
    )

    # write stream for writing data to Elasticsearch
    (
        df.writeStream
        .outputMode('append')
        .format("org.elasticsearch.spark.sql")
        .option("header", "true")
        .option("path", "events/index")
        .option("checkpointLocation", checkpoint)
        .start()
        .awaitTermination()
    )


if __name__ == '__main__':
    main()
