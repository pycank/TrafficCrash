import pyspark
import hdfs
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

base_path = "/user/pdt/raw_rows/"

client = hdfs.Client("http://namenode:9870")
files_list = client.list(base_path)
print(files_list)
full_df = None

spark = pyspark.sql.SparkSession.builder.appName("collect-to-new-batch").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Connect to Cassandra
print("Connecting to Cassandra...")
cassandra_host = "cassandra-0.cassandra.bigdata-pipeline.svc.cluster.local"
cluster = Cluster([cassandra_host])
session = cluster.connect()

# Create keyspace if not exists
session.execute(
    "CREATE KEYSPACE IF NOT EXISTS trafficaccidentshub "
    "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
)
session.set_keyspace("trafficaccidentshub")

# Create table for batch results
session.execute("""
    CREATE TABLE IF NOT EXISTS batch_summary (
        id text PRIMARY KEY,
        severity text,
        start_time text,
        weather_condition text,
        processed_at timestamp
    )
""")
print("Cassandra connected and tables created")

processed_list = []
try:
    processed_list = client.list("/user/pdt/processed")
except:
    pass


for file_name in files_list:
    print('hieu')
    try:
        if file_name in processed_list:
            continue
        if full_df is None:
            full_df = spark.read \
                .option("delimiter","|||") \
                .option("header", "true") \
                .csv("hdfs://namenode:9000" + base_path + file_name)
            full_df.write \
                .option("delimiter","|||") \
                .csv(f"hdfs://namenode:9000/user/pdt/processed/{file_name}", header=True)
        else:
            df = spark.read \
                .option("delimiter","|||") \
                .option("header", "true") \
                .csv("hdfs://namenode:9000" + base_path + file_name)
            full_df = full_df.union(df)
            full_df.write \
                .option("delimiter","|||") \
                .csv(f"hdfs://namenode:9000/user/pdt/processed/{file_name}", header=True)
    except:
        pass

try:
    full_df.write \
        .option("delimiter", "|||") \
        .csv("hdfs://namenode:9000/user/pdt/news/new_batch.csv", header=True)
    print("Batch written to HDFS")
except Exception as e:
    print(f"Error writing to HDFS: {e}")

# Write batch results to Cassandra
if full_df is not None:
    print("Writing batch results to Cassandra...")
    try:
        import datetime
        rows = full_df.collect()
        count = 0
        for row in rows:
            try:
                stmt = SimpleStatement(
                    "INSERT INTO batch_summary (id, severity, start_time, weather_condition, processed_at) "
                    "VALUES (%s, %s, %s, %s, %s)"
                )
                session.execute(stmt, (
                    str(row['ID']),
                    str(row['Severity']),
                    str(row['Start_Time']),
                    str(row['Weather_Condition']),
                    datetime.datetime.now()
                ))
                count += 1
            except Exception as e:
                print(f"Error inserting row: {e}")
                continue
        print(f"Successfully wrote {count} rows to Cassandra")
    except Exception as e:
        print(f"Error writing to Cassandra: {e}")
else:
    print("No data to write to Cassandra")

# Close Cassandra connection
cluster.shutdown()
print("Batch processing completed")

