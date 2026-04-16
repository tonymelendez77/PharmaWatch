import os
from pyspark.sql import SparkSession

aws_access_key = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
aws_region = os.environ["AWS_REGION"]

spark = (
    SparkSession.builder
    .appName("PharmaWatch Iceberg Setup")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://pharmawatch-data-lake/iceberg/")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    .config("spark.hadoop.fs.s3a.endpoint.region", aws_region)
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.pharmawatch")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.pharmawatch.faers_events (
    report_id STRING,
    drug_name STRING,
    reaction STRING,
    severity STRING,
    age INT,
    weight DOUBLE,
    report_date DATE,
    ingestion_ts TIMESTAMP,
    hospitalization INTEGER,
    death INTEGER,
    disability INTEGER
)
USING iceberg
PARTITIONED BY (drug_name, month(report_date))
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.pharmawatch.pubmed_articles (
    article_id STRING,
    title STRING,
    abstract STRING,
    publish_date DATE,
    drug_name STRING,
    ingestion_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (drug_name)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.pharmawatch.reddit_mentions (
    post_id STRING,
    subreddit STRING,
    title STRING,
    body STRING,
    score INT,
    drug_mentions STRING,
    created_utc TIMESTAMP,
    ingestion_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (drug_mentions)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.pharmawatch.drug_labels (
    drug_id STRING,
    brand_name STRING,
    generic_name STRING,
    warnings STRING,
    interactions STRING,
    ingestion_ts TIMESTAMP
)
USING iceberg
""")

print("iceberg tables ready")

spark.stop()
