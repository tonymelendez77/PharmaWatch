package pharmawatch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object PubmedTransformer {

  val SourceTable = "glue_catalog.pharmawatch.pubmed_articles"
  val TargetTable = "glue_catalog.pharmawatch_clean.pubmed_articles"

  def transform(spark: SparkSession): DataFrame = {
    val raw = spark.read.format("iceberg").table(SourceTable)

    val cast = raw.withColumn("publish_date", col("publish_date").cast(DateType))

    val filtered = cast
      .filter(col("abstract").isNotNull && length(col("abstract")) >= 10)

    val normalized = filtered
      .withColumn("drug_name", upper(trim(col("drug_name"))))

    val enriched = normalized
      .withColumn("abstract_length", length(col("abstract")))
      .withColumn("publish_year", year(col("publish_date")))

    enriched.dropDuplicates("article_id")
  }

  def run(spark: SparkSession): Unit = {
    val df = transform(spark)
    df.writeTo(TargetTable).using("iceberg").createOrReplace()
  }
}
