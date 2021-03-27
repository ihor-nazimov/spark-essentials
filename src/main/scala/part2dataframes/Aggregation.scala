package part2dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{approx_count_distinct, asc, avg, col, column, count, countDistinct, desc, mean, stddev, sum}

//def quiet_logs(sc):
//  logger = sc._jvm.org.apache.log4j
//  logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
//  logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

object Aggregation extends App {
  val spark = SparkSession.builder()
    .appName("Aggregation App")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  val genresCount = moviesDF.select(count(col("Major_Genre")))
  genresCount.show()
  moviesDF.selectExpr("count(Major_Genre)").show()
  moviesDF.select(count("*")).show()

  moviesDF.select(countDistinct("Major_Genre")).show()

  moviesDF.select(approx_count_distinct(column("Major_Genre"))).show()

  val minIMDB = moviesDF.select(functions.min(column("IMDB_Rating"))).show()
  val maxIMDB = moviesDF.selectExpr("max(IMDB_Rating)").show()
//  println(maxIMDB())

  moviesDF.select(functions.sum(col("US_Gross"))).show()
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

  moviesDF.selectExpr(
    "mean(Rotten_Tomatoes_Rating)",
    "stddev(Rotten_Tomatoes_Rating)"
  ).show()

  //Grouping
  val grouped = moviesDF
    .groupBy(col("Major_Genre"))
    .count()
  grouped.show()

  val avgIMDBRating = moviesDF
    .groupBy(col("Major_Genre"))
    .avg()
  avgIMDBRating.show()

  val meanStdIMDB = moviesDF
    .selectExpr("Major_Genre", "IMDB_Rating")
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("Count"),
      avg("IMDB_Rating").as("IMDB_Avg"),
      mean("IMDB_Rating").as("IMDB_Mean"),
      stddev("IMDB_Rating").as("IMDB_Stddev")
    )

    meanStdIMDB.orderBy(asc("IMDB_Mean")).show()
    meanStdIMDB.orderBy(desc("IMDB_Mean")).show()
  /**
    * Exercises
    */
  //Movie profits
  val profitDF = moviesDF.selectExpr(
    "Title", "Worldwide_Gross", "Production_Budget", "Worldwide_Gross - Production_Budget as Profit"
  )
  .orderBy("Profit")
  profitDF.show()

  profitDF.select(sum(col("Profit"))).show()

  //Count of distinct directors
  val directorsDF = moviesDF.select(countDistinct(col("Director")))
  directorsDF.show()

  //Mean and std.dev of US gross
  val meanStdUSGross = moviesDF.select(
    mean(col("US_Gross")).as("Mean_US_Gross"),
    stddev(col("US_Gross")).as("Stddev_US_Gross")
  )
  meanStdUSGross.show()

  //avg IMDB rating and US gross per director
  val avgsPerDirector = moviesDF.select("Director", "US_Gross", "IMDB_Rating")
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      avg("US_Gross").as("Avg_US_Gross"),
      sum("US_Gross").as("Sum_IMDB_Rating"),
      count("*")
    )
  avgsPerDirector.orderBy(desc("Avg_IMDB_Rating")).show()
  avgsPerDirector.orderBy(desc("Avg_US_Gross")).show()
  avgsPerDirector.orderBy(col("Sum_IMDB_Rating").desc_nulls_last).show()


}
