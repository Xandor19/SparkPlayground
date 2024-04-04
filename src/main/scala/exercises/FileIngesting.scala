package exercises

import core.Exercise
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.{IntegerType, DateType, StringType}
import javax.xml.crypto.Data

object FileIngesting extends Exercise {
  def main(args: Array[String]): Unit = {
    val spark = createExahustiveLocalSession("File ingesting")
    val csvPath = "data/complex-format-books.csv"
    val simpleJsonPath = "data/durham-nc-foreclosure-2006-2016.json"
    val multilineJsonPath = "data/countrytravelinfo.json"
    val xmlPath = "data/nasa-patents.xml"
    val xmlRowTag = "row"
    val avroPath = "data/weather.avro"
    val orcPath = "data/demo-11-zlib.orc"
    val parquetPath = "data/alltypes_plain.parquet"

    dataframePreview(parquet(spark, parquetPath))
  }

  def csvInferredSchema(spark: SparkSession, path: String): DataFrame = spark.read
    .options(Map(
      "header" -> "true",
      "multiline" -> "true",
      "sep" -> ";",
      "quote" -> "*",
      "dateFormat" -> "M/d/y",
      "inferSchema" -> "true"
    ))
    .csv(path)

  def csvOwnSchema(spark: SparkSession, path: String): DataFrame = {
    val schema = StructType(Array(
      StructField("id", IntegerType, false),
      StructField("authorId", IntegerType, false),
      StructField("bookTitle", StringType, false),
      StructField("releaseDate", DateType, false),
      StructField("url", StringType, false)
    ))
    
    spark.read
      .options(Map(
        "header" -> "true",
        "multiline" -> "true",
        "sep" -> ";",
        "quote" -> "*",
        "dateFormat" -> "M/d/y"
      ))
      .schema(schema)
      .csv(path)
  }

  def simpleJson(spark: SparkSession, path: String): DataFrame = spark
    .read
    .json(path)

  def multilineJson(spark: SparkSession, path: String): DataFrame = spark.read
    .option("multiline", true)
    .json(path)

  // Requires Databricks' XML library, which was added but I don't get it to work
  def xml(spark: SparkSession, path: String, rowTag: String): DataFrame = spark.read
    .format("com.databricks.spark.xml")
    .option("rowTag", rowTag)
    .load(path)

  // Requires Avro community library
  def avro(spark: SparkSession, path: String): DataFrame = spark.read
    .format("avro")
    .load(path)

  // Prior to Spark 2.4 it is required that a SparkSession with explicit config of
  // "spark.sql.orc.impl" as "native"
  def orc(spark: SparkSession, path: String): DataFrame = spark.read
    .format("orc")
    .load(path)

  def parquet(spark: SparkSession, path: String): DataFrame = spark.read
    .format("parquet")
    .load(path)
}