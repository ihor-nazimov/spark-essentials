package part3datatypes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{array_contains, coalesce, col, expr, split, struct, to_date}

object ComplexTypes extends App {
  val spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  println("moviesDF")
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  println(s"Overall row count: ${moviesDF.count}")

  val moviesActDateDF = moviesDF.select(
      col("Title"),
      col("Release_Date")
    )
    .withColumn("Actual_Release",
      coalesce(
        to_date(moviesDF.col("Release_Date"), "d-MMM-yy"),
        to_date(moviesDF.col("Release_Date"), "yyyy-MM-dd"),
        to_date(moviesDF.col("Release_Date"), "MMMM, yyyy"))
    )
//    .where(col("Title") === "Among Giants" or col("Title") === "The Apartment")
  moviesActDateDF.show

  moviesActDateDF.where(col("Actual_Release").isNull).show
  println(s"Incorrect date row count: ${moviesActDateDF.where(col("Actual_Release").isNull).count}")

//stocks.csv
  println("******* stocksDF *******")

  val stocksDF = spark.read
    .option("sep", ",")
    .option("header", "true")
//    .option("dateFormat", )
    .csv("src/main/resources/data/stocks.csv")

  println(s"Overall row count: ${stocksDF.count}")
  stocksDF.show

  val stocksDateDF = stocksDF //.select("*")
    .withColumn("date_converted", to_date(stocksDF.col("date"), "MMM d yyyy"))

  println(s"Incorrect date row count: ${stocksDateDF.where(col("date_converted").isNull).count}")

  stocksDateDF.where(col("date_converted").isNull).show

  //Structures
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit.US_Gross").as("US_Profit"))
    .show

  moviesDF
    .selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross as US_Profit")
    .show

  //Arrays
  moviesDF
    .select(col("Title"), split(col("Title"), " |,").as("Title_Words"))
    .withColumn("First_Word", expr("Title_Words[0]"))
    .select(
      col("*"),
      functions.size(col("Title_Words")),
      array_contains(col("Title_Words"), "Love")
    )
    .show
}
