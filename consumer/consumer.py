from kafka import KafkaConsumer
import datetime
import json
import pyspark
from pyspark.sql.types import *
import os
import time
from pyspark.sql import SparkSession

# Set the PYTHONIOENCODING environment variable to UTF-8
os.environ["PYTHONIOENCODING"] = "UTF-8"

# Create SparkSession once and reuse it (more efficient)
spark = SparkSession.builder \
    .appName("consumer") \
    .config("spark.driver.memory", "512m") \
    .config("spark.driver.maxResultSize", "256m") \
    .config("spark.executor.memory", "512m") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Batch configuration
BATCH_SIZE = 100  # Number of messages to batch before writing
BATCH_TIMEOUT = 30  # Maximum seconds to wait before flushing batch
MAX_RETRIES = 3  # Maximum retry attempts for HDFS writes
RETRY_DELAY = 5  # Seconds to wait between retries

def write_batch_to_hdfs(batch_data, retry_count=0):
    """
    Write a batch of data to HDFS with retry logic
    """
    if not batch_data:
        return True
    
    try:
        # Convert batch to dataframe
        df = spark.createDataFrame(batch_data)
        
        # Create path with timestamp (one file per batch, not per message)
        now = datetime.datetime.now()
        path = 'hdfs://namenode:9000/user/pdt/raw_rows/batch_{}_{}_{}_{}_{}_{}_{}.csv'.format(
            now.year, now.month, now.day, now.hour, now.minute, now.second, now.microsecond
        )
        
        # Write to HDFS with error handling
        df.write \
            .option("header", "true") \
            .option("delimiter", "|||") \
            .csv(path)
        
        print(f'[kafka consumer saved batch of {len(batch_data)} rows]')
        return True
        
    except Exception as e:
        print(f'[ERROR] Failed to write batch to HDFS: {str(e)}')
        if retry_count < MAX_RETRIES:
            print(f'[RETRY] Retrying in {RETRY_DELAY} seconds... (attempt {retry_count + 1}/{MAX_RETRIES})')
            time.sleep(RETRY_DELAY)
            return write_batch_to_hdfs(batch_data, retry_count + 1)
        else:
            print(f'[ERROR] Max retries reached. Dropping batch of {len(batch_data)} rows.')
            return False

print('hello')

# Kafka consumer configuration
# Using all Kafka brokers in Kubernetes cluster for better reliability
consumer_config = {
    'bootstrap_servers': 'kafka-1:9092,kafka-2:9093,kafka-3:9094',  # All brokers in Kubernetes cluster
    'group_id': 'my_consumer_group',
    'auto_offset_reset': 'latest',
    'enable_auto_commit': True,
    'auto_commit_interval_ms': 100,
}

print("configed")

# Create Kafka consumer instance
consumer = KafkaConsumer(**consumer_config)

print("created")

# Subscribe to a Kafka topic
kafka_topic = 'TrafficAccident'  # Replace with your Kafka topic
consumer.subscribe([kafka_topic])

print("subscribed")

# Batch buffer
batch_buffer = []
last_batch_time = time.time()

try:
    while True:
        records = consumer.poll(1000)  # timeout in milliseconds
        # Process received records
        for tp, consumer_records in records.items():
            for consumer_record in consumer_records:
                try:
                    # Convert from byte -> dict (json format)
                    decoded_message = consumer_record.value.decode('utf-8')
                    data_dict = json.loads(decoded_message)
                    
                    # Add to batch buffer
                    batch_buffer.append(data_dict)
                    print(f'[kafka consumer received row] (batch size: {len(batch_buffer)})')
                    
                except Exception as e:
                    print(f'[ERROR] Failed to process message: {str(e)}')
                    continue
        
        # Check if we should flush the batch
        current_time = time.time()
        should_flush = (
            len(batch_buffer) >= BATCH_SIZE or 
            (len(batch_buffer) > 0 and (current_time - last_batch_time) >= BATCH_TIMEOUT)
        )
        
        if should_flush and batch_buffer:
            success = write_batch_to_hdfs(batch_buffer)
            if success:
                batch_buffer = []  # Clear buffer on success
                last_batch_time = current_time
            # If failed, keep buffer for next retry (but don't block forever)

except KeyboardInterrupt:
    print('\n[INFO] Shutting down gracefully...')
    # Write remaining batch before exit
    if batch_buffer:
        print(f'[INFO] Flushing remaining {len(batch_buffer)} rows...')
        write_batch_to_hdfs(batch_buffer)
except Exception as e:
    print(f'[FATAL ERROR] Unexpected error: {str(e)}')
    # Try to save remaining batch
    if batch_buffer:
        print(f'[INFO] Attempting to save remaining {len(batch_buffer)} rows...')
        write_batch_to_hdfs(batch_buffer)
finally:
    # Write any remaining batch before closing
    if batch_buffer:
        print(f'[INFO] Final flush of {len(batch_buffer)} rows...')
        write_batch_to_hdfs(batch_buffer)
    
    # Close down consumer to commit final offsets
    consumer.close()
    spark.stop()
    print('[INFO] Consumer closed')

