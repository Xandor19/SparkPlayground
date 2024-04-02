package exercises

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.util.Try
import core.Exercise

object DelimiterSplit extends Exercise {    
  def main(args: Array[String]): Unit = {
    val spark = createExahustiveLocalSession("Delimiter Split")
    import spark.implicits._
    
    val df = readCsv(spark, "data/sep_split.csv", false).toDF("original_value", "separator")
    
    val sol = df.withColumn("split_values", expr("""split(original_value, concat('\\', separator))"""))
    
    sol.show(truncate = false)
    sol.printSchema()
  }
}