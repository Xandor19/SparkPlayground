package exercises

import core.Exercise
import scala.util.{Try, Success, Failure}
import java.util.InputMismatchException
import org.apache.spark.sql.functions._

object LazynessEval extends Exercise {
  def main(args: Array[String]): Unit = {
    try {
      if(args.nonEmpty) {
        val mode = getParam("--mode", args)
        val file = getParam("--path", args)
        val header = !args.contains("--no-header")
        val replicas = getParam("--replica", args)
        val export = getParam("--export", args)

        start(
          mode.getOrElse("noop"),
          file.getOrElse(throw new InputMismatchException("Source file is required (arg --path)")),
          header,
          replicas.getOrElse("1").toInt,
          export.getOrElse("")
        )
        exit("Task finished", false)
      }
      else throw new InputMismatchException("At least the source file must be provided (arg --path)")
    } catch {
      case e: InputMismatchException => exit(e.getMessage(), true)
    }
  }

  def start(
    mode: String, 
    file: String, 
    fileHeader: Boolean, 
    replicas: Int, 
    export: String
  ): Unit = {
    val spark = createExahustiveLocalSession("Lazzyness Test")
    val t1 = System.currentTimeMillis()
    val initial = readCsv(
      spark, 
      file,
      fileHeader
    )
    val t2 = System.currentTimeMillis()
    val replicated = List.fill(replicas)(initial).reduce((a, b) => a.union(b))
    val t3 = System.currentTimeMillis()
    val renamed = replicated.withColumnRenamed("Lower Confidence Limit", "lcl")
      .withColumnRenamed("Upper Confidence Limit", "ucl")
    val t4 = System.currentTimeMillis()

    val df = 
      if (!mode.equalsIgnoreCase("noop")) {
        val withCols = renamed.withColumn("avg", expr("(lcl + ucl) / 2"))
          .withColumn("lcl2", col("lcl"))
          .withColumn("ucl2", col("ucl"))
        
          if (mode.equalsIgnoreCase("full")) {
            withCols.drop("avg")
              .drop("lcl2")
              .drop("ucl2")
          }
          else withCols
      }
      else renamed
    val t5 = System.currentTimeMillis()
    df.collect()
    val t6 = System.currentTimeMillis()
  }

  def getParam(name: String, args: Array[String]): Option[String] = {
    val pos = args.indexOf(name)

    if(pos > -1) Some(args(pos + 1)) 
    else None
  }

  def exit(message: String, err: Boolean): Unit = {
    if (err) {
      System.err.println(message)
      System.exit(-1)
    }
    else {
      println(message)
      System.exit(0)
    }
  }
}