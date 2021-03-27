package part3datatypes.ScalaTypes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object DataTypes extends App {
  val spark = SparkSession.builder()
    .appName("DataTypes")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.select(col("Title"), lit(47))
    .show
}
