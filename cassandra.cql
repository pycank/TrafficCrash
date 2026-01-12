-- 1. Tạo Database (Keyspace)
CREATE KEYSPACE IF NOT EXISTS trafficaccidentshub
WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE trafficaccidentshub;

-- 2. Tạo bảng Realtime (Stream Layer)
CREATE TABLE IF NOT EXISTS realtime_traffic_stats (
    window_start TIMESTAMP,
    severity TEXT,
    weather_condition TEXT,
    accident_count INT,
    last_updated TIMESTAMP,
    PRIMARY KEY ((severity), window_start, weather_condition)
) WITH CLUSTERING ORDER BY (window_start DESC, weather_condition ASC);

-- 3. Tạo bảng Batch (Batch Layer)
CREATE TABLE IF NOT EXISTS batch_traffic_stats (
    accident_date DATE,
    state TEXT,
    severity TEXT,
    total_accidents INT,
    avg_distance FLOAT,
    PRIMARY KEY ((state), accident_date, severity)
) WITH CLUSTERING ORDER BY (accident_date DESC, severity ASC);

-- 4. Insert dữ liệu giả
INSERT INTO realtime_traffic_stats (window_start, severity, weather_condition, accident_count, last_updated)
VALUES ('2025-01-01 10:00:00', '2', 'Rain', 10, toTimestamp(now()));