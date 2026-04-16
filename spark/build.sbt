ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "pharmawatch"
ThisBuild / version := "0.1.0"

name := "pharmawatch-spark"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" % Provided,
  "org.apache.spark" %% "spark-sql" % "3.5.1" % Provided,
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % "1.5.0"
)

assembly / mainClass := Some("pharmawatch.TransformerRunner")

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF")          => MergeStrategy.discard
  case PathList("META-INF", "services", xs @ _*)    => MergeStrategy.concat
  case PathList("META-INF", xs @ _*)                => MergeStrategy.discard
  case "reference.conf"                             => MergeStrategy.concat
  case "application.conf"                           => MergeStrategy.concat
  case x                                            =>
    val old = (assembly / assemblyMergeStrategy).value
    old(x)
}
