package exercises

import core.Exercise
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object RestaurantsUnion extends Exercise {
  def main(args: Array[String]): Unit = {
    val spark = createExahustiveLocalSession("Restaurants Union")

    val wakeDf = formatWake(readCsv(
      spark, 
      "data/Restaurants_in_Wake_County_NC.csv", 
      true
    ))
    val durhamDf = formatDurham(readJson(
      spark, 
      "data/Restaurants_in_Durham_County_NC.json"
    ))
    val df = wakeDf.unionByName(durhamDf)

    df.printSchema()
  }

  def formatWake(df: DataFrame): DataFrame = df.withColumn("county", lit("Wake"))
    .withColumnRenamed("HSISID", "datasetId")
    .withColumnRenamed("NAME", "name")
    .withColumnRenamed("ADDRESS1", "address1")
    .withColumnRenamed("ADDRESS2", "address2")
    .withColumnRenamed("CITY", "city")
    .withColumnRenamed("STATE", "state")
    .withColumnRenamed("POSTALCODE", "zip")
    .withColumnRenamed("PHONENUMBER", "tel")
    .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
    .withColumn("dateEnd", lit(null))
    .withColumnRenamed("FACILITYTYPE", "type")
    .withColumnRenamed("X", "geoX")
    .withColumnRenamed("Y", "geoY")
    .drop("OBJECTID")
    .drop("PERMITID")
    .drop("GEOCODESTATUS")
    .withColumn("id", concat(
      col("state"), lit("_"),
      col("county"), lit("_"),
      col("datasetId")
    ))

  def formatDurham(df: DataFrame): DataFrame = df.withColumn("county", lit("Durham"))
    .withColumn("datasetId", col("fields.id"))
    .withColumn("name", col("fields.premise_name"))
    .withColumn("address1", col("fields.premise_address1"))
    .withColumn("address2", col("fields.premise_address2"))
    .withColumn("city", col("fields.premise_city"))
    .withColumn("state", col("fields.premise_state"))
    .withColumn("zip", col("fields.premise_zip"))
    .withColumn("tel", col("fields.premise_phone"))
    .withColumn("dateStart", col("fields.opening_date"))
    .withColumn("dateEnd", col("fields.closing_date"))
    .withColumn("type", 
      split(col("fields.type_description"), " - ").getItem(1)
    )
    .withColumn("geoX", col("fields.geolocation").getItem(0))
    .withColumn("geoY", col("fields.geolocation").getItem(1))
    .withColumn("id", concat(
      col("state"), lit("_"),
      col("county"), lit("_"),
      col("datasetId")
    ))
    .drop("fields")
    .drop("geometry")
    .drop("record_timestamp")
    .drop("recordid")
}