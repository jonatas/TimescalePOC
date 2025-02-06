import psycopg2
import os
from datetime import datetime
import random

try:
    device_count = 300  # Fixed number of devices
    history_years = 3
    tag_count = 50
except Exception as e:
    print(f"Parameters error: {e}")

today = datetime.today()
creation_date = today.replace(year=today.year - history_years).strftime('%Y-%m-%d %H:%M:%S')


conn_string = os.environ.get('CONN_STRING', "postgresql://postgres:5432/TEST?user=postgres&password=TEST!timescale")

try:
    db_connection = psycopg2.connect(conn_string)
    cursor = db_connection.cursor()
    print("Successfully connected to DB", flush=True)
except Exception as e:
    print(f"Error during connection to db {e}")

def execute_query(query):
    try:
        cursor.execute(query)
        db_connection.commit()
    except Exception as e:
        print(f"Error query: {e}")
        exit(0)

# Execute query that must be outside any transaction
def execute_without_transaction(query):
    try:
        # Temporarily enable autocommit
        old_autocommit = db_connection.autocommit
        db_connection.autocommit = True
        
        cursor.execute(query)
        
        # Restore previous autocommit setting
        db_connection.autocommit = old_autocommit
    except Exception as e:
        print(f"Error query: {e}")
        exit(0)

# Add drop commands before create table
create_table_query = f'''
DROP TABLE IF EXISTS public."Values" CASCADE;
DROP TABLE IF EXISTS public.values_daily_rollup CASCADE;
DROP TABLE IF EXISTS public.values_monthly_rollup CASCADE;

CREATE TABLE IF NOT EXISTS public."Values"
(
    "Date" timestamptz NOT NULL,
    "Tag" TEXT NOT NULL,
    "Device" TEXT NOT NULL,
    "Value" double precision NOT NULL,
    CONSTRAINT "PK_Values" PRIMARY KEY ("Tag", "Device", "Date")
);

 select create_hypertable('"Values"', by_range('Date', INTERVAL '1 month'));
 select add_dimension('"Values"', 'Device', number_partitions => 4);
'''
execute_query(create_table_query)


start_year = (today.year - history_years)
print(f"ADDING VALUES", flush=True)
for device in range(device_count):
    query = ''
    for tag in range(tag_count):
        query += f'''
            INSERT INTO "Values" ("Date", "Tag", "Device", "Value")
            SELECT
                generate_series AS "Data",
                'TAG_{tag}' AS "Tag",
                'DEVICE_{device}' AS "Device",
                random()*1 AS "Valore" from generate_series('{start_year}-01-01', '{today.year}-01-01', INTERVAL '1 hour');
        '''
    execute_query(query)


# Create continuous aggregates outside of any transaction
daily_rollup_query = '''
CREATE MATERIALIZED VIEW IF NOT EXISTS values_daily_rollup
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', "Date") AS bucket,
       "Tag",
       "Device",
       SUM("Value") as sum_value
FROM "Values"
GROUP BY bucket, "Tag", "Device"
WITH DATA;
'''

monthly_rollup_query = '''
CREATE MATERIALIZED VIEW IF NOT EXISTS values_monthly_rollup
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 month', "Date") AS bucket,
       "Tag",
       "Device",
       SUM("Value") as sum_value
FROM "Values"
GROUP BY bucket, "Tag", "Device" 
WITH DATA;
'''
# Execute each query separately outside of any transaction
print("Creating daily   continuous aggregates", flush=True)
execute_without_transaction(daily_rollup_query)
print("Creating monthly continuous aggregates", flush=True)
execute_without_transaction(monthly_rollup_query)



def benchmark_query(query, description, iterations=3):
    """Run a query multiple times and measure its performance"""
    times = []
    for i in range(iterations):
        start_time = datetime.now()
        cursor.execute(query)
        results = cursor.fetchall()
        end_time = datetime.now()
        # Convert to milliseconds
        times.append((end_time - start_time).total_seconds() * 1000)
    
    avg_time = sum(times) / len(times)
    print(f"\n{description}")
    print(f"Average execution time: {avg_time:.2f} ms")
    print(f"Number of results: {len(results)}")
    return avg_time, results

# Example devices for testing
test_devices = [f"'DEVICE_{i}'" for i in range(1, 11)]  # Test with 10 devices
devices_array = f"ARRAY[{','.join(test_devices)}]"

# Generate one random tag to use across all queries
test_tag = f"TAG_{random.randint(0, tag_count)}"

# Test queries
queries = {
    "Raw Query (Daily)": f"""
    SELECT
        DATE_TRUNC('day', t."Date"::timestamp) AS "Time",
        COALESCE(SUM(t."Value"), 0.0) AS "Value"
    FROM "Values" AS t
    WHERE t."Device" = ANY ({devices_array})
    AND t."Tag" = '{test_tag}'
    AND t."Date"::timestamp >= '2023-01-01 00:00:00'
    AND t."Date"::timestamp <= '2023-12-31 23:59:59'
    GROUP BY DATE_TRUNC('day', t."Date"::timestamp)
    ORDER BY "Time";
    """,
    
    "Using time_bucket (Daily)": f"""
    SELECT
        time_bucket('1 day', "Date") AS "Time",
        COALESCE(SUM("Value"), 0.0) AS "Value"
    FROM "Values"
    WHERE "Device" = ANY ({devices_array})
    AND "Tag" = '{test_tag}'
    AND "Date" >= '2023-01-01 00:00:00'
    AND "Date" <= '2023-12-31 23:59:59'
    GROUP BY "Time"
    ORDER BY "Time";
    """,
    
    "Using Daily Rollup": f"""
    SELECT
        bucket as "Time",
        COALESCE(SUM(sum_value), 0.0) AS "Value"
    FROM values_daily_rollup
    WHERE "Device" = ANY ({devices_array})
    AND "Tag" = '{test_tag}'
    AND bucket >= '2023-01-01'
    AND bucket <= '2023-12-31'
    GROUP BY bucket
    ORDER BY bucket;
    """,
    
    "Using Monthly Rollup": f"""
    SELECT
        bucket as "Time",
        COALESCE(SUM(sum_value), 0.0) AS "Value"
    FROM values_monthly_rollup
    WHERE "Device" = ANY ({devices_array})
    AND "Tag" = '{test_tag}'
    AND bucket >= '2023-01-01'
    AND bucket <= '2023-12-31'
    GROUP BY bucket
    ORDER BY bucket;
    """
}

print("\nRunning benchmark tests...")
print("=" * 50)

# Run EXPLAIN ANALYZE for each query
print("\nQuery execution plans:")
print("=" * 50)
for description, query in queries.items():
    print(f"\n{description}")
    print("-" * 50)
    cursor.execute(f"EXPLAIN ANALYZE {query}")
    plan = cursor.fetchall()
    for line in plan:
        print(line[0])

# Run actual benchmarks
print("\nBenchmark results:")
print("=" * 50)
results = {}
for description, query in queries.items():
    avg_time, data = benchmark_query(query, description)
    results[description] = avg_time

# Add row counts before closing the connection
count_queries = {
    '"Values" table': 'SELECT COUNT(*) FROM "Values"',
    'Daily rollup': 'SELECT COUNT(*) FROM values_daily_rollup',
    'Monthly rollup': 'SELECT COUNT(*) FROM values_monthly_rollup'
}

print("\nRow Counts:")
print("=" * 50)
for table_name, count_query in count_queries.items():
    cursor.execute(count_query)
    count = cursor.fetchone()[0]
    print(f"{table_name}: {count:,} rows")

# Print summary
print("\nPerformance Summary:")
print("=" * 50)
for description, avg_time in sorted(results.items(), key=lambda x: x[1]):
    print(f"{description}: {avg_time:.2f} ms")

# Close database connection
cursor.close()
db_connection.close()



