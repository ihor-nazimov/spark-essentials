package part3datatypes

import org.apache.spark.sql.functions.{array_contains, avg, col, to_date}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

import java.sql.Date

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.show

  numbersDF.printSchema()
  //simple dataset
  implicit val encoderInt: Encoder[Int] = Encoders.scalaInt
  val numbersDS = numbersDF.as[Int]
  numbersDS.show

  def readDF(name: String): DataFrame =
    spark.read.option("inferSchema", "true").json("src/main/resources/data/" + name + ".json")

  //complex dataset
  case class Car(
                Name: String,
                Miles_Per_Gallon: Option[Double],
                Cylinders: Long,
                Displacement: Double,
                Horsepower: Option[Long],
                Weight_in_lbs: Long,
                Acceleration: Double,
                Corrected_Year: Date,
                Origin: String
                )

//  import.spark.implicits._
  val carsDF = readDF("cars")
  carsDF.show

  val correctedYearCarsDF = carsDF
    .withColumn("Corrected_Year", to_date(col("Year"), "yyyy-MM-dd"))
  correctedYearCarsDF.show

  import spark.implicits._
//  implicit val carEncoder = Encoders.product[Car]
  val carsDS = correctedYearCarsDF.as[Car]

  carsDS.show

  println(carsDS.count)
  println(carsDS.filter(car => car.Horsepower.getOrElse(0L) > 140).count)
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsDS.count)
  //or
  println(carsDS.select(avg("Horsepower")).show) //more precise

  //join
  case class Guitar (id: Long, model: String, make: String, guitarType: String)
  val guitarsDS = readDF("guitars")
    .withColumnRenamed("type", "guitarType")
    .as[Guitar]

  case class GuitarPlayer (id: Long, name: String, guitars: List[Long], band: Long)
  val guitarPlayersDS = readDF("guitarPlayers").as[GuitarPlayer]

  case class Band (id: Long, name: String, hometown: String, year: String)
  val bandsDS = readDF("bands")
//    .withColumn("year_to_date", to_date(col("year"), "yyyy"))
    .as[Band]

  guitarsDS.show
  guitarPlayersDS.show
  bandsDS.show

  guitarPlayersDS.joinWith(bandsDS, bandsDS.col("id") === guitarPlayersDS.col("band")).show
  guitarPlayersDS.joinWith(bandsDS, bandsDS.col("id") === guitarPlayersDS.col("band"), "left_outer").show

  guitarPlayersDS.join(bandsDS, bandsDS.col("id") === guitarPlayersDS.col("band"), "left_outer").show

  guitarPlayersDS
    .joinWith(
      guitarsDS,
      array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")),
      "left_outer"
    )
    .select(col("_1.name"))
    .show

  carsDS
    .groupByKey(_.Origin)
    .count
    .show


}
