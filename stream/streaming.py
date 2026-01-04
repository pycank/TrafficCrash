import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.streaming import DataStreamWriter
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

BOOTSTRAP = os.getenv('BOOTSTRAP_SERVERS', 'kafka-1:9092,kafka-2:9093,kafka-3:9094')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'TrafficAccident')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', '127.0.0.1')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'traffic')

spark = SparkSession.builder \
    .appName('TrafficSpeedLayer') \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', BOOTSTRAP) \
    .option('subscribe', KAFKA_TOPIC) \
    .option('startingOffsets', 'latest') \
    .load()

schema = StructType([
    StructField('ID', StringType()),
    StructField('Severity', IntegerType()),
    StructField('Start_Time', StringType()),
    StructField('Weather_Condition', StringType()),
])

parsed = df.selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ts", to_timestamp(col("Start_Time")))

cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect()
session.execute(f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} "
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
session.set_keyspace(CASSANDRA_KEYSPACE)
session.execute("""
    CREATE TABLE IF NOT EXISTS traffic_stream (
        id text PRIMARY KEY,
        severity int,
        start_time timestamp,
        weather_condition text
    )
""")

def write_to_cassandra(batch_df, batch_id):
    rows = batch_df.collect()
    for r in rows:
        stmt = SimpleStatement(
            "INSERT INTO traffic_stream (id, severity, start_time, weather_condition) VALUES (%s, %s, %s, %s)"
        )
        session.execute(stmt, (r['ID'], r['Severity'], r['ts'], r['Weather_Condition']))
    print(f"Batch {batch_id} written to Cassandra ({len(rows)} rows)")

query = parsed.writeStream.foreachBatch(write_to_cassandra).start()
query.awaitTermination()
