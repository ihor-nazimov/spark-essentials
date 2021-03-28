package part3datatypes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, lit, regexp_extract, regexp_replace}

object DataTypes extends App {
  val spark = SparkSession.builder()
    .appName("DataTypes")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.select(col("Title"), lit(47).as("plain_value")).show

  val preferredFilter = col("Major_Genre") equalTo "Comedy"

  moviesDF
    .select(col("Title"), col("Major_Genre"), preferredFilter.as("is_preferred"))
    .show

  moviesDF
    .select(col("Title"), col("IMDB_Rating") / 2 + col("Rotten_Tomatoes_Rating") / 10)
    .show

  val ratingCorr = moviesDF.stat.corr("IMDB_Rating", "Rotten_Tomatoes_Rating")
  println(s"Correlation is $ratingCorr")

  //********************
  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  println("volkswagen")
  carsDF.select(initcap(col("Name"))).show
  carsDF.select("*").where(col("Name") contains "volkswagen").show

  println("vw|volkswagen")
  val regexpString = "vw|volkswagen"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexpString, 0).as("regexp_extract")
  )
    .where(col("regexp_extract") =!= "")
    .drop("regexp_extract")
  vwDF.show

  println("People's Car")
  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexpString, "People's Car").as("people_cars")
  )
  .show

  /**
    * Exercise
    */

  def getCarNames: List[String] =
    List("ford", "fiat")

  def listToRegexp(list: List[String]): String =
    list.map(_.toLowerCase).reduceLeft(_ + "|" + _)

  println(listToRegexp(getCarNames))

  carsDF.select(
      col("Name"),
      regexp_extract(col("Name"), listToRegexp(getCarNames), 0).as("reg_extr")
    )
    .where(col("reg_extr") =!= "")
    .drop("reg_extr")
    .show

  val carFilter = getCarNames
        .map(_.toLowerCase)
        .map(name => col("Name").contains(name))
        .fold(lit(false))(_ or _)
  carsDF.filter(carFilter).show


}
