package core

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import javax.xml.crypto.Data
import java.util.Properties
import org.apache.spark.sql.SaveMode

trait Exercise {
  val conStr = "jdbc:postgresql://localhost/spark_playground"
  val deff = new Properties()
  deff.setProperty("driver", "org.postgresql.Driver")
  deff.setProperty("user", "postgres")
  deff.setProperty("password", "admin")

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def createLocalSession(name: String, threads: String): SparkSession = SparkSession
    .builder()
    .appName(name)
    .master(f"local[$threads]")
    .getOrCreate()

  def createExahustiveLocalSession(name: String): SparkSession = createLocalSession(
    name,
    "*"
  )

  def createRemoteSession(name: String, master: String, memory: String): SparkSession = SparkSession.
    builder()
    .appName(name)
    .master(master)
    .config("spark.executor.memory", memory)
    .getOrCreate()

  def readCsv(spark: SparkSession, path: String, header: Boolean): DataFrame = spark.read
    .option("header", value = header)
    .option("inferSchema", value = true)
    .csv(path)

  def readJson(spark: SparkSession, path: String): DataFrame = spark.read
    .option("inferSchema", value = true)
    .json(path)

  def camelizeCols(cols: Array[String]): Array[Column] = cols.map {n => 
    val r = "[-_\\s]+([a-z])".r
    col(n).as(r.replaceAllIn(n.toLowerCase(), _.group(1).toUpperCase()))
  }

  def dataframePreview(df: DataFrame): Unit = {
    df.printSchema()
    df.show()
  }

  def saveToDb(
    df: DataFrame, 
    table: String, 
    props: Properties = null
  ): Unit = {
    val current = deff.clone().asInstanceOf[Properties]

    if (props != null) current.putAll(props)

    df.write
      .mode(SaveMode.Overwrite)
      .jdbc(conStr, table, current)
  }
}