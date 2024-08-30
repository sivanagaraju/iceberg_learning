from pyspark.sql import SparkSession
import sys

def create_spark_session():
    return (SparkSession.builder
            .appName("Iceberg SQL Runner")
            .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.14.0")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
            .config("spark.sql.catalog.hadoop_catalog.warehouse", "s3://your-bucket/warehouse")
            .config("spark.sql.defaultCatalog", "hadoop_catalog")
            .getOrCreate())

def run_sql_file(spark, file_path):
    with open(file_path, 'r') as file:
        sql_commands = file.read().split(';')
    
    for command in sql_commands:
        if command.strip():
            print(f"Executing: {command}")
            spark.sql(command)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_iceberg_sql.py <path_to_sql_file>")
        sys.exit(1)

    sql_file_path = sys.argv[1]
    spark = create_spark_session()

    try:
        run_sql_file(spark, sql_file_path)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        spark.stop()
