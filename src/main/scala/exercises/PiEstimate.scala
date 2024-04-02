package exercises

import core.Exercise
import scala.util.Random
import org.apache.spark.sql.Encoders

object PiEstimate extends Exercise {
  def main(args: Array[String]): Unit = {
    val spark = createExahustiveLocalSession("Pi Estimation")
    val slices = 5
    val totalDarts = 1000000 * slices

    import spark.implicits._

    val df = spark.sparkContext.parallelize(1 to totalDarts).map { x =>
      val x = math.random() * 2 - 1
      val y = math.random() * 2 - 1

      if (x * x + y * y <= 1) 1 else 0
    }
    val hits = df.reduce(_+_)

    println("Estimate: " + 4.0 * hits / totalDarts)
  }
}