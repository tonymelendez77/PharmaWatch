package pharmawatch

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DrugLabelsTransformer {

  val SourceTable = "glue_catalog.pharmawatch.drug_labels"
  val TargetTable = "glue_catalog.pharmawatch_clean.drug_labels"

  def transform(spark: SparkSession): DataFrame = {
    val raw = spark.read.format("iceberg").table(SourceTable)

    val filtered = raw.filter(
      !(col("brand_name") === lit("unknown") && col("generic_name") === lit("unknown"))
    )

    val normalized = filtered
      .withColumn("brand_name", upper(trim(col("brand_name"))))
      .withColumn("generic_name", upper(trim(col("generic_name"))))

    val enriched = normalized
      .withColumn(
        "has_interactions",
        col("interactions").isNotNull && length(col("interactions")) > 10
      )
      .withColumn("warnings_length", length(col("warnings")))

    enriched.dropDuplicates("drug_id")
  }

  def run(spark: SparkSession): Unit = {
    val df = transform(spark)
    df.writeTo(TargetTable).using("iceberg").createOrReplace()
  }
}
