package part3datatypes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

object ManagingNulls extends App {
  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF
    .select(
      col("Title"),
      col("IMDB_Rating"),
      col("Rotten_Tomatoes_Rating"),
      coalesce(col("IMDB_Rating") * 10, col("Rotten_Tomatoes_Rating"))
    )
    .show

  moviesDF.where(col("IMDB_Rating").isNull).show

  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last).show

  moviesDF.select(col("Title"), col("IMDB_Rating")).na.drop.show

  moviesDF.select(
      col("Title"),
      col("IMDB_Rating")
    )
    .na.fill(0)
    .show
  moviesDF.select(
      col("Title"),
      col("IMDB_Rating"),
      col("Rotten_Tomatoes_Rating")
    )
    .na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
    .show
  moviesDF.select(
      col("Title"),
      col("IMDB_Rating"),
      col("Director")
    )
    .na.fill(Map(
      "IMDB_Rating" -> 0,
      "Director" -> "unknown"
    ))
    .show

  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as I_R_Rating", //coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as I_R_Rating_nvl", //coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as Null_If", // if (first == second) null else first
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 100) as Nvl2", // if (first != null) second else third
  )
  .show
}
