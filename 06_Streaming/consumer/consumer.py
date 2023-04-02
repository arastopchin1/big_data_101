import yaml
from pyspark.sql import SparkSession


def read_config():
    with open("config.yaml", "r") as yaml_file:
        return yaml.safe_load(yaml_file)


def main():
    config = read_config()
    spark = SparkSession.builder.master("yarn").appName("Big_Data_Streaming_Homework").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    topic = 'test'
    file = f"{config['path']}{config['file']}"
    server = f"{config['bootstrap_servers']['host']}:{config['bootstrap_servers']['port']}"
    checkpoint = f"{config['path']}{config['checkpointLocation']}"

    # read stream for getting data from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", server)
        .option("subscribe", topic)
        .load()
        .selectExpr("CAST(value AS STRING)")
    )

    # write stream for writing data to HDFS
    (
        df.writeStream
        .outputMode('append')
        .format('csv')
        .option("path", file)
        .option("checkpointLocation", checkpoint)
        .option("header", "true")
        .trigger(processingTime='1 second')
        .start()
        .awaitTermination()
    )


if __name__ == '__main__':
    main()
