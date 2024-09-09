import argparse
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException, ParseException
import sys
import os
import time
import re
import logging
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime


def setup_logging():
    # Create logs directory if it doesn't exist
    log_dir = Path.cwd() / 'logs'
    log_dir.mkdir(exist_ok=True)

    # Create a timestamped log file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"iceberg_sql_run_{timestamp}.log"

    # Set up logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

logger = setup_logging()
# Set up logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

@contextmanager
def timer(operation):
    start_time = time.time()
    yield
    end_time = time.time()
    logger.info(f"{operation} completed in {end_time - start_time:.2f} seconds")

def create_spark_session(config_file=None):
    logger.info("Creating Spark session")
    builder = SparkSession.builder.appName("Iceberg SQL Runner")

    # Default configurations
    default_configs = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.spark_catalog.type": "hive",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": "s3://your-bucket/warehouse",
        "spark.sql.defaultCatalog": "local"
    }

    # Load configurations from file if provided
    if config_file:
        logger.info(f"Loading configuration from {config_file}")
        with open(config_file, 'r') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    default_configs[key.strip()] = value.strip()

    # Set configurations
    for key, value in default_configs.items():
        builder = builder.config(key, value)

    # Create and return the SparkSession
    spark = builder.getOrCreate()
    
    logger.info("Spark Configuration:")
    for item in spark.sparkContext.getConf().getAll():
        logger.info(f"{item[0]}: {item[1]}")
    
    return spark

def validate_sql_commands(spark, commands):
    valid_commands = []
    errors = []
    
    ddl_pattern = re.compile(r'\b(CREATE|ALTER|DROP|TRUNCATE|COMMENT|RENAME)\b', re.IGNORECASE)
    dml_pattern = re.compile(r'\b(SELECT|INSERT|UPDATE|DELETE|MERGE|CALL)\b', re.IGNORECASE)
    
    for i, command in enumerate(commands, 1):
        command = command.strip()
        if not command:
            continue
        
        try:
            # Use Spark's SQL parser to catch syntax errors
            spark.sql(command)
            
            if ddl_pattern.search(command) or dml_pattern.search(command):
                valid_commands.append(command)
            else:
                errors.append(f"Line {i}: Unknown command type: {command}")
        except AnalysisException as e:
            errors.append(f"Line {i}: Analysis Error: {str(e)}")
        except ParseException as e:
            errors.append(f"Line {i}: Parse Error: {str(e)}")
        except Exception as e:
            errors.append(f"Line {i}: Unexpected error: {str(e)}")
    
    return valid_commands, errors

def validate_iceberg_specific(spark, commands):
    errors = []
    
    for i, command in enumerate(commands, 1):
        # Check for Iceberg-specific syntax
        if 'CREATE TABLE' in command.upper() and 'USING iceberg' not in command.lower():
            errors.append(f"Line {i}: Iceberg table format not specified in CREATE TABLE statement")
        
        # Check for table existence in ALTER, DROP, INSERT, UPDATE, DELETE, MERGE statements
        if any(keyword in command.upper() for keyword in ['ALTER TABLE', 'DROP TABLE', 'INSERT INTO', 'UPDATE', 'DELETE FROM', 'MERGE INTO']):
            table_name = re.search(r'\b(ALTER TABLE|DROP TABLE|INSERT INTO|UPDATE|DELETE FROM|MERGE INTO)\s+(\S+)', command, re.IGNORECASE)
            if table_name:
                table_name = table_name.group(2)
                try:
                    spark.table(table_name)
                except AnalysisException:
                    errors.append(f"Line {i}: Table {table_name} does not exist")
        
        # Check for valid partitioning in CREATE TABLE statements
        if 'CREATE TABLE' in command.upper() and 'PARTITIONED BY' in command.upper():
            partition_cols = re.search(r'PARTITIONED BY \(([^)]+)\)', command, re.IGNORECASE)
            if partition_cols:
                partition_cols = [col.strip() for col in partition_cols.group(1).split(',')]
                table_cols = re.search(r'CREATE TABLE \S+ \(([^)]+)\)', command, re.IGNORECASE)
                if table_cols:
                    table_cols = [col.split()[0].strip() for col in table_cols.group(1).split(',')]
                    invalid_partition_cols = [col for col in partition_cols if col not in table_cols]
                    if invalid_partition_cols:
                        errors.append(f"Line {i}: Invalid partition columns: {', '.join(invalid_partition_cols)}")
    
    
    return errors

def execute_command(spark, command, verbose=False):
    if verbose:
        logger.info(f"Executing: {command}")
    
    with timer("Command execution"):
        try:
            result = spark.sql(command)
            if command.lower().strip().startswith(('select', 'show')):
                result_data = result.collect()
                logger.info(f"Query result: {result_data}")
                return result_data
        except Exception as e:
            logger.error(f"Error executing command: {command}")
            logger.error(f"Error message: {str(e)}")
            raise

def run_sql_file(spark, file_path, verbose=False, dry_run=False):
    logger.info(f"Processing SQL file: {file_path}")
    
    with open(file_path, 'r') as file:
        sql_commands = file.read().split(';')
    
    valid_commands, validation_errors = validate_sql_commands(spark, sql_commands)
    iceberg_errors = validate_iceberg_specific(spark, valid_commands)
    
    all_errors = validation_errors + iceberg_errors
    
    if all_errors:
        logger.warning("Validation errors found:")
        for error in all_errors:
            logger.warning(error)
        if dry_run or not valid_commands:
            return []
        logger.info("Proceeding with valid commands...")
    
    if dry_run:
        logger.info("Dry run completed. No commands were executed.")
        return []
    
    results = []
    for command in valid_commands:
        try:
            result = execute_command(spark, command, verbose)
            if result:
                results.append((command, result))
        except Exception as e:
            logger.error(f"Command execution failed: {str(e)}")
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Run Iceberg SQL commands from a file.")
    parser.add_argument("sql_file", help="Path to the SQL file")
    parser.add_argument("--config", help="Path to Spark configuration file")
    parser.add_argument("--verbose", action="store_true", help="Print each SQL command before execution")
    parser.add_argument("--dry-run", action="store_true", help="Validate the SQL file without executing commands")
    args = parser.parse_args()

    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)

    if not os.path.exists(args.sql_file):
        logger.error(f"SQL file not found: {args.sql_file}")
        sys.exit(1)

    spark = create_spark_session(args.config)

    try:
        results = run_sql_file(spark, args.sql_file, args.verbose, args.dry_run)
        
        if not args.dry_run:
            logger.info("Execution Results:")
            for command, result in results:
                logger.info(f"Command: {command}")
                logger.info(f"Result: {result}")
                logger.info("-" * 50)
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        spark.stop()


# python run_iceberg_sql.py path_to_your_sql_file.sql --config spark_config.conf --verbose 
if __name__ == "__main__":
    main()