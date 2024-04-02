package exercises

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.catalyst.StructFilters
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.Row
import java.sql.Date
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.matchers.must.Matchers.contain

class StockTest extends AnyFunSuite {
  val session = StockMarket.createExahustiveLocalSession("first-test")

  val schema = StructType(
    Seq(
      StructField("date", DateType, true),
      StructField("open", DoubleType, true),
      StructField("close", DoubleType, true)
    )
  )

  test("High closing prices per year obtention") {
    val testRows = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2023-03-01"), 1.0, 2.0),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0)
    )
    val expected = Seq(
      Row(Date.valueOf("2022-01-12"), 1.0, 2.0),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0)
    )
    implicit val encoder: Encoder[Row] = Encoders.row(schema)
    val testDf = session.createDataset(testRows)
    val result = StockMarket.highestClosingDayPerYear(testDf).collect()

    result should contain theSameElementsAs expected
  }

}
