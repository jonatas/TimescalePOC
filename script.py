import psycopg2
import os
from datetime import datetime, timedelta
import random
import argparse
import json
from typing import Dict, List, Tuple
import time
from tabulate import tabulate
import sys


class TimescaleBenchmark:
    def __init__(self, conn_string: str, use_dimension: bool = True):
        self.conn_string = conn_string
        self.use_dimension = use_dimension
        self.db_connection = None
        self.cursor = None
        self.connect_to_db()

    def connect_to_db(self):
        try:
            self.db_connection = psycopg2.connect(self.conn_string)
            self.cursor = self.db_connection.cursor()
            print("Successfully connected to DB", flush=True)
        except Exception as e:
            print(f"Error during connection to db: {e}")
            sys.exit(1)

    def execute_query(self, query: str):
        try:
            self.cursor.execute(query)
            self.db_connection.commit()
        except Exception as e:
            print(f"Error executing query: {e}")
            sys.exit(1)

    def execute_without_transaction(self, query: str):
        try:
            old_autocommit = self.db_connection.autocommit
            self.db_connection.autocommit = True
            self.cursor.execute(query)
            self.db_connection.autocommit = old_autocommit
        except Exception as e:
            print(f"Error executing query: {e}")
            sys.exit(1)

    def create_schema(self):
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
        '''
        
        if self.use_dimension:
            create_table_query += 'select add_dimension(\'"Values"\', \'Device\', number_partitions => 4);'
        
        self.execute_query(create_table_query)

    def generate_data(self, device_count: int, tag_count: int, start_date: datetime, end_date: datetime):
        print(f"Generating data for {device_count} devices and {tag_count} tags...", flush=True)
        total_devices = device_count
        batch_size = 10  # Process 10 devices at a time
        
        for batch_start in range(0, total_devices, batch_size):
            batch_end = min(batch_start + batch_size, total_devices)
            query = ''
            
            for device in range(batch_start, batch_end):
                for tag in range(tag_count):
                    query += f'''
                        INSERT INTO "Values" ("Date", "Tag", "Device", "Value")
                        SELECT
                            generate_series AS "Date",
                            'TAG_{tag}' AS "Tag",
                            'DEVICE_{device}' AS "Device",
                            random()*1 AS "Value" 
                        FROM generate_series(
                            '{start_date.strftime('%Y-%m-%d')}', 
                            '{end_date.strftime('%Y-%m-%d')}', 
                            INTERVAL '1 hour'
                        );
                    '''
            self.execute_query(query)
            print(f"Processed devices {batch_start} to {batch_end-1}", flush=True)

    def create_continuous_aggregates(self):
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

        print("Creating daily continuous aggregates", flush=True)
        self.execute_without_transaction(daily_rollup_query)
        print("Creating monthly continuous aggregates", flush=True)
        self.execute_without_transaction(monthly_rollup_query)

    def benchmark_query(self, query: str, description: str, iterations: int = 3) -> Tuple[float, List, str]:
        times = []
        results = None
        explain_plan = ""

        # Get query plan
        self.cursor.execute(f"EXPLAIN ANALYZE {query}")
        explain_plan = "\n".join([line[0] for line in self.cursor.fetchall()])

        # Run benchmark
        for i in range(iterations):
            start_time = time.time()
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            end_time = time.time()
            times.append((end_time - start_time) * 1000)  # Convert to milliseconds

        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        
        return {
            'avg_time': avg_time,
            'min_time': min_time,
            'max_time': max_time,
            'result_count': len(results),
            'explain_plan': explain_plan
        }

    def run_benchmarks(self, test_devices: List[str], test_tag: str, start_date: str, end_date: str) -> Dict:
        devices_array = f"ARRAY[{','.join(test_devices)}]"
        
        queries = {
            "Raw Query (Daily)": f"""
            SELECT
                DATE_TRUNC('day', t."Date"::timestamp) AS "Time",
                COALESCE(SUM(t."Value"), 0.0) AS "Value"
            FROM "Values" AS t
            WHERE t."Device" = ANY ({devices_array})
            AND t."Tag" = '{test_tag}'
            AND t."Date"::timestamp >= '{start_date}'
            AND t."Date"::timestamp <= '{end_date}'
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
            AND "Date" >= '{start_date}'
            AND "Date" <= '{end_date}'
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
            AND bucket >= '{start_date}'
            AND bucket <= '{end_date}'
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
            AND bucket >= '{start_date}'
            AND bucket <= '{end_date}'
            GROUP BY bucket
            ORDER BY bucket;
            """
        }

        results = {}
        for description, query in queries.items():
            print(f"\nRunning benchmark for: {description}")
            results[description] = self.benchmark_query(query, description)

        return results

    def get_row_counts(self) -> Dict[str, int]:
        count_queries = {
            '"Values" table': 'SELECT COUNT(*) FROM "Values"',
            'Daily rollup': 'SELECT COUNT(*) FROM values_daily_rollup',
            'Monthly rollup': 'SELECT COUNT(*) FROM values_monthly_rollup'
        }

        counts = {}
        for table_name, query in count_queries.items():
            self.cursor.execute(query)
            counts[table_name] = self.cursor.fetchone()[0]
        
        return counts

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.db_connection:
            self.db_connection.close()


def format_results(benchmark_results: Dict, row_counts: Dict, use_dimension: bool) -> str:
    output = []
    
    # Add configuration header
    output.append("=" * 80)
    output.append(f"Benchmark Results {'with' if use_dimension else 'without'} Dimension")
    output.append("=" * 80)
    
    # Add row counts
    output.append("\nRow Counts:")
    output.append("-" * 40)
    count_table = [[table, f"{count:,}"] for table, count in row_counts.items()]
    output.append(tabulate(count_table, headers=['Table', 'Count'], tablefmt='grid'))
    
    # Add benchmark results
    output.append("\nQuery Performance:")
    output.append("-" * 40)
    
    perf_table = []
    headers = ['Query Type', 'Avg (ms)', 'Min (ms)', 'Max (ms)', 'Results']
    
    for query_type, results in benchmark_results.items():
        perf_table.append([
            query_type,
            f"{results['avg_time']:.2f}",
            f"{results['min_time']:.2f}",
            f"{results['max_time']:.2f}",
            results['result_count']
        ])
    
    output.append(tabulate(perf_table, headers=headers, tablefmt='grid'))
    
    # Add query plans
    output.append("\nQuery Execution Plans:")
    output.append("-" * 40)
    for query_type, results in benchmark_results.items():
        output.append(f"\n{query_type}:")
        output.append("-" * len(query_type))
        output.append(results['explain_plan'])
        output.append("")
    
    return "\n".join(output)


def main():
    parser = argparse.ArgumentParser(description='TimescaleDB Benchmark Tool')
    parser.add_argument('--devices', type=int, default=300, help='Number of devices to simulate')
    parser.add_argument('--tags', type=int, default=50, help='Number of tags per device')
    parser.add_argument('--years', type=int, default=3, help='Number of years of historical data')
    parser.add_argument('--conn-string', type=str, 
                       default=os.environ.get('CONN_STRING', "postgresql://postgres:5432/TEST?user=postgres&password=TEST!timescale"),
                       help='Database connection string')
    parser.add_argument('--output', type=str, default='benchmark_results.json',
                       help='Output file for benchmark results')
    parser.add_argument('--test-devices', type=int, default=10,
                       help='Number of devices to use in benchmark queries')
    
    args = parser.parse_args()

    # Run benchmarks with and without dimension
    all_results = {}
    
    for use_dimension in [True, False]:
        print(f"\nRunning benchmark {'with' if use_dimension else 'without'} dimension...")
        
        benchmark = TimescaleBenchmark(args.conn_string, use_dimension)
        
        # Setup
        print("Creating schema...")
        benchmark.create_schema()
        
        # Generate data
        end_date = datetime.now()
        start_date = end_date.replace(year=end_date.year - args.years)
        
        print("Generating data...")
        benchmark.generate_data(args.devices, args.tags, start_date, end_date)
        
        print("Creating continuous aggregates...")
        benchmark.create_continuous_aggregates()
        
        # Run benchmarks
        test_devices = [f"'DEVICE_{i}'" for i in range(args.test_devices)]
        test_tag = f"TAG_{random.randint(0, args.tags-1)}"
        
        print("Running benchmarks...")
        benchmark_results = benchmark.run_benchmarks(
            test_devices,
            test_tag,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        row_counts = benchmark.get_row_counts()
        
        # Format and store results
        formatted_output = format_results(benchmark_results, row_counts, use_dimension)
        print(formatted_output)
        
        all_results[f"{'with' if use_dimension else 'without'}_dimension"] = {
            'benchmark_results': benchmark_results,
            'row_counts': row_counts
        }
        
        benchmark.close()
    
    # Save results to file
    with open(args.output, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"\nDetailed results saved to {args.output}")


if __name__ == "__main__":
    main()



