package exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import javax.xml.crypto.Data
import org.apache.spark.sql.Column
import org.apache.spark.sql.SaveMode
import core.Exercise

object StockMarket extends Exercise {
  def main(args: Array[String]): Unit = {
    val spark = createExahustiveLocalSession("first-project")

    val df = readCsv(spark, "data/AAPL.csv", true)

    val renamedCols = camelizeCols(df.columns)

    val stockData = df
      .select(renamedCols: _*)
      .withColumn("diff", col("close") - col("open"))

    import spark.implicits._

    //val onTheRise = stockData.where($"close" > $"open" * 1.15)
    //val onTheLow = stockData.where($"diff" < 0)

    val yearlyAvg = stockData
      .groupBy(year($"date").as("year"))
      .agg(
        max($"close"),
        avg($"close"),
        max($"diff"),
        avg($"diff")
      )
      .sort($"year")

    //highestClosingDayPerYear(stockData).show()

    /* diffWithMonthlyAvg(stockData).select(
      $"date", 
      $"close", 
      $"close_monthly_avg",
      $"deviation", 
      $"diff_max"
    ).show() */

    saveToDb(monthsCloseDiff(df), "month_close_diff")
    // yearlyAvg.rdd.map(_.mkString(",")).saveAsTextFile("results/yearly_data")

    //saveToDb(df, "month_close_diff")
  }  

  def highestClosingDayPerYear(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val window = Window.partitionBy(year($"date").as("year")).orderBy($"close".desc)

    df.withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .drop($"rank")
      .sort(year($"date"), $"rank")
  }

  def monthlyRank(df: DataFrame): DataFrame = {
    val window = monthsWindows(df).orderBy(col("close"))

    df.withColumn("rank", rank().over(window))
  }

  def diffWithMonthlyAvg(df: DataFrame): DataFrame = {
    val window = monthsWindows(df)

    df.withColumn("close_monthly_avg", avg("close").over(window))
      .withColumn("deviation", col("close") - col("monthly_avg"))
      .withColumn("diff_max", col("close") - max("close").over(window))
  }

  def monthsCloseDiff(df: DataFrame): DataFrame = {
    val aggregated = df.groupBy(col("date").substr(0,7).as("month"))
      .agg(avg("close").as("close_avg"), max("close").as("max_close"))

    val window = Window.partitionBy(col("month").substr(0, 3)).orderBy("month")

    aggregated.withColumn("prev_avg_diff", col("close_avg") - lag(col("close_avg"), 1, 0).over(window))
      .withColumn("post_avg_diff", col("close_avg") - lead(col("close_avg"), 1, 0).over(window))
      .withColumn("prev_max_diff", col("max_close") - lag(col("max_close"), 1, 0).over(window))
      .withColumn("post_max_diff", col("max_close") - lead(col("max_close"), 1, 0).over(window))
      .select(
        col("month"),
        col("close_avg"),
        col("max_close"),
        col("prev_avg_diff"),
        col("post_avg_diff"),
        col("prev_max_diff"),
        col("post_max_diff")
      )
  }

  def monthsWindows(df: DataFrame) = Window.partitionBy(col("date").substr(0,7).as("date"))
} 
