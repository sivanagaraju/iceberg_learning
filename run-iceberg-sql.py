import argparse
from pyspark.sql import SparkSession
from pyspark import SparkConf
import sys
import os
import time
import re
import configparser

def create_spark_session():
    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('spark-config.conf')
    
    # Create a SparkConf object
    conf = SparkConf()
    
    # Add all configurations from the 'spark' section
    if 'spark' in config:
        for key, value in config['spark'].items():
            conf.set(f"spark.{key}", value)
    
    # Handle the 'jars' section separately
    if 'jars' in config:
        jar_dir = config['jars'].get('jarDirectory', '')
        jars = config['jars'].get('jars', '').split(',')
        jar_paths = [f"{jar_dir}/{jar.strip()}" for jar in jars if jar.strip()]
        conf.set("spark.jars", ",".join(jar_paths))
    
    # Create the SparkSession
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    return spark

def validate_sql_commands(commands):
    valid_commands = []
    errors = []
    
    ddl_pattern = re.compile(r'\b(CREATE|ALTER|DROP|TRUNCATE|COMMENT|RENAME)\b', re.IGNORECASE)
    dml_pattern = re.compile(r'\b(SELECT|INSERT|UPDATE|DELETE|MERGE|CALL)\b', re.IGNORECASE)
    
    for i, command in enumerate(commands, 1):
        command = command.strip()
        if not command:
            continue
        
        if ddl_pattern.search(command) or dml_pattern.search(command):
            valid_commands.append(command)
        else:
            errors.append(f"Line {i}: Invalid or unsupported SQL command: {command}")
    
    return valid_commands, errors

def run_sql_file(spark, file_path, verbose=False, dry_run=False):
    with open(file_path, 'r') as file:
        sql_commands = file.read().split(';')
    
    valid_commands, validation_errors = validate_sql_commands(sql_commands)
    
    if validation_errors:
        print("Validation errors found:")
        for error in validation_errors:
            print(error)
        if dry_run or not valid_commands:
            return []
        print("Proceeding with valid commands...")
    
    if dry_run:
        print("Dry run completed. No commands were executed.")
        return []
    
    results = []
    for command in valid_commands:
        if verbose:
            print(f"Executing: {command}")
        start_time = time.time()
        try:
            result = spark.sql(command)
            if command.lower().strip().startswith(('select', 'show')):
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
    parser.add_argument("--dry-run", action="store_true", help="Validate the SQL file without executing commands")
    args = parser.parse_args()

    if not os.path.exists(args.sql_file):
        print(f"SQL file not found: {args.sql_file}")
        sys.exit(1)

    spark = create_spark_session(args.config)

    try:
        results = run_sql_file(spark, args.sql_file, args.verbose, args.dry_run)
        if not args.dry_run:
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