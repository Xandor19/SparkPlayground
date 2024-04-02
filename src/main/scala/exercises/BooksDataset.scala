package exercises

import core.Exercise
import java.text.SimpleDateFormat
import org.apache.spark.sql.Encoders
import java.sql

object BooksDataset extends Exercise {
  def main(args: Array[String]): Unit = {
    val spark = createExahustiveLocalSession("Books Transform")
    val df = readCsv(spark, "data/books.csv", true)

    df.printSchema()

    import spark.implicits._

    val ds = df.map(b => {
      Book(
        b.getAs[Int]("id"),
        b.getAs[Int]("authorId"),
        b.getAs[String]("title"),
        b.getAs[String]("link"),
        {
          val date = b.getAs[String]("releaseDate")

          if (date != null) FormatDate.format(date)
          else null
        }
      )
    })

    ds.printSchema()
  }

  case class Book (
    id: Int,
    authorId: Int,
    title: String,
    link: String,
    releaseDate: sql.Date
  )

  object FormatDate {
    val formatter = new SimpleDateFormat("M/d/yy")

    def format(date: String): sql.Date = new sql.Date(
      formatter.parse(date).getTime()
    )
  }
}