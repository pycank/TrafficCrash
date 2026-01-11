import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

BOOTSTRAP = os.getenv('BOOTSTRAP_SERVERS', 'kafka-1:9092,kafka-2:9093,kafka-3:9094')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'TrafficAccident')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'cassandra.bigdata-pipeline.svc.cluster.local')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'traffic')
CASSANDRA_TABLE = "traffic_stream"

spark = SparkSession.builder \
    .appName('TrafficSpeedLayer') \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', BOOTSTRAP) \
    .option('subscribe', KAFKA_TOPIC) \
    .option('startingOffsets', 'latest') \
    .load()

schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Severity", IntegerType(), True),
    StructField("Start_Time", StringType(), True),
    StructField("Weather_Condition", StringType(), True),
])

parsed_df = df.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select(
        col("data.ID").alias("id"),
        col("data.Severity").alias("severity"),
        to_timestamp(col("data.Start_Time")).alias("start_time"),
        col("data.Weather_Condition").alias("weather_condition")
    )

query = parsed_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "/app/checkpoints") \
    .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE) \
    .outputMode("append") \
    .start()

query.awaitTermination()
