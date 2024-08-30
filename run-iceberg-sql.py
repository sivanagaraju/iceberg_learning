import argparse
from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys
import os
import time

def create_spark_session(config_file=None):
    conf = SparkConf()
    
    if config_file:
        with open(config_file, 'r') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    conf.set(key.strip(), value.strip())
    
    return (SparkSession.builder
            .config(conf=conf)
            .appName("Iceberg SQL Runner")
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.14.0")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
            .config("spark.sql.catalog.hadoop_catalog.warehouse", "s3://your-bucket/warehouse")
            .config("spark.sql.defaultCatalog", "hadoop_catalog")
            .getOrCreate())

def run_sql_file(spark, file_path, verbose=False):
    with open(file_path, 'r') as file:
        sql_commands = file.read().split(';')
    
    results = []
    for command in sql_commands:
        command = command.strip()
        if command:
            if verbose:
                print(f"Executing: {command}")
            start_time = time.time()
            try:
                result = spark.sql(command)
                if command.lower().startswith(('select', 'show')):
                    result_data = result.collect()
                    results.append((command, result_data))
                execution_time = time.time() - start_time
                print(f"Command executed successfully in {execution_time:.2f} seconds")
            except Exception as e:
                print(f"Error executing command: {command}")
                print(f"Error message: {str(e)}")
    return results

def display_results(results):
    for command, data in results:
        print(f"\nResults for command: {command}")
        for row in data:
            print(row)

def main():
    parser = argparse.ArgumentParser(description="Run Iceberg SQL commands from a file.")
    parser.add_argument("sql_file", help="Path to the SQL file")
    parser.add_argument("--config", help="Path to Spark configuration file")
    parser.add_argument("--verbose", action="store_true", help="Print each SQL command before execution")
    parser.add_argument("--output", help="Path to save query results")
    args = parser.parse_args()

    if not os.path.exists(args.sql_file):
        print(f"SQL file not found: {args.sql_file}")
        sys.exit(1)

    spark = create_spark_session(args.config)

    try:
        results = run_sql_file(spark, args.sql_file, args.verbose)
        display_results(results)

        if args.output:
            with open(args.output, 'w') as f:
                for command, data in results:
                    f.write(f"Results for command: {command}\n")
                    for row in data:
                        f.write(f"{row}\n")
                    f.write("\n")
            print(f"Results saved to {args.output}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()