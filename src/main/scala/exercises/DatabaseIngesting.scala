package exercises

import core.Exercise
import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object DatabaseIngesting extends Exercise {
  val db = "sakila"

  def main(args: Array[String]): Unit = {
    val spark = createExahustiveLocalSession("Database Ingesting")
    
    ingestingTroughQueries(spark)
  }

  /**
    * Reads from a relational database with out-of-the-box support (Postgres
    * in this case)
    * 
    * Has examples for the main connection methods
    *
    * @param spark
    */
  def supportedRelational(spark: SparkSession): Unit = {
    // Using "option" methods, such as reading from files
    val optionRead = spark.read
      .option("url", conStr + db)
      .option("dbtable", "actor")
      .option("user", "postgres")
      .option("password", "admin")
      .option("useSSL", "false")
      .option("serverTimezone", "EST")
      .format("jdbc")
      .load()
      .orderBy(col("last_name"))

    // Using an extended connection URL to specify options
    val urlRead = spark.read
      .jdbc(conStr + db
        + "?user=postgres"
        + "&password=admin"
        + "&useSSL=false"
        + "&serverTimezone=EST",
        "film",
        new Properties()
      )
      .orderBy(col("title"))

    // Using regular java Properties to specify options, this is the default
    // used in Exercises.readFromDb, left here for the example
    val propsRead = spark.read
      .jdbc(conStr + db, "language", deff)
      .orderBy(col("name"))

    println("*******Reading using \"option\" methods for configuration*******")
    dataframePreview(optionRead)

    println("*******Reading using long connection URL*******")
    dataframePreview(urlRead)

    println("*******Reading using Properties object for configuration*******")
    dataframePreview(propsRead)
  }

  /**
    * Reads from a relational database which is not supported and requires
    * an user-defined dialect (Informix in this case)
    * 
    * It uses utils.InformixDialect to handle the dialect
    *
    * @param spark
    */
  def requiresDialect(spark: SparkSession): Unit = {

  }

  def ingestingTroughQueries(spark: SparkSession): Unit = {
    val singleTable = "(SELECT * " +
      "FROM film " +
      "WHERE rental_rate > 1 AND (rating = 'G' OR rating = 'PG')" +
    ") film_alias"

    val singleTableDf = readFromDb(spark, db, singleTable)

    val tableJoin = "(" +
      "SELECT " +
      "   actor.first_name, " +
      "   actor.last_name, " +
      "   COUNT(DISTINCT(film.title)) AS \"films\", " +
      "   COUNT(DISTINCT(language.name)) AS \"languages\"" +
      "FROM actor, film_actor, film, language " +
      "WHERE actor.actor_id = film_actor.actor_id " +
      "   AND film_actor.film_id = film.film_id " +
      "   AND film.language_id = language.language_id " +
      "GROUP BY actor.first_name, actor.last_name " +
      "ORDER BY languages DESC, films DESC" +
    ") actors_films"

    val joinTablesDf = readFromDb(spark, db, tableJoin)

    println("*******Pre-filtered single table query*******")
    dataframePreview(singleTableDf)

    println("*******Pre-filtered query joining tables*******")
    dataframePreview(joinTablesDf)
  }
}