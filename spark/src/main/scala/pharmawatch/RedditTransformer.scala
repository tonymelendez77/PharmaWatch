package pharmawatch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

object RedditTransformer {

  val SourceTable = "glue_catalog.pharmawatch.reddit_mentions"
  val TargetTable = "glue_catalog.pharmawatch_clean.reddit_mentions"

  def transform(spark: SparkSession): DataFrame = {
    val raw = spark.read.format("iceberg").table(SourceTable)

    val cast = raw.withColumn("created_utc", col("created_utc").cast(TimestampType))

    val filtered = cast
      .filter(col("body").isNotNull && length(col("body")) > 0)

    val enriched = filtered
      .withColumn("body_length", length(col("body")))
      .withColumn("hour_of_day", hour(col("created_utc")))
      .withColumn("drug_list", split(col("drug_mentions"), ","))

    enriched.dropDuplicates("post_id")
  }

  def run(spark: SparkSession): Unit = {
    val df = transform(spark)
    df.writeTo(TargetTable).using("iceberg").createOrReplace()
  }
}
