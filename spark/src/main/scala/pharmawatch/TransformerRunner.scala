package pharmawatch

import org.apache.spark.sql.SparkSession

object TransformerRunner extends App {

  val awsRegion = sys.env.getOrElse("AWS_REGION", "us-east-1")

  val spark = SparkSession.builder()
    .appName("PharmaWatch Transformers")
    .config(
      "spark.jars.packages",
      "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8"
    )
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://pharmawatch-data-lake/iceberg/")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.hadoop.fs.s3a.endpoint.region", awsRegion)
    .getOrCreate()

  spark.sql("CREATE DATABASE IF NOT EXISTS glue_catalog.pharmawatch_clean")

  FaersTransformer.run(spark)
  println("faers done")

  PubmedTransformer.run(spark)
  println("pubmed done")

  RedditTransformer.run(spark)
  println("reddit done")

  DrugLabelsTransformer.run(spark)
  println("labels done")

  println("all transforms finished")

  spark.stop()
}
