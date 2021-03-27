package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()

  /*
   Writing DFs
   - format
   - save mode = overwrite, append, ignore, errorIfExists
   - path
   - zero or more options
  */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))
  val stocks = spark.read
    .schema(stockSchema)
    .options(Map(
      "header" -> "true",
      "sep" -> ",",
      "nullValue" -> "",
      "dateFormat" -> "MMM d yyyy"
    ))
    .csv("src/main/resources/data/stocks.csv")

  stocks.show()


  val employees = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employees.show()

  /**
    * Exercise
    */

  val movieSchema = StructType(Array(
    StructField("Title", StringType, true),
    StructField("US_Gross", LongType, true),
    StructField("Worldwide_Gross", LongType, true),
    StructField("US_DVD_Sales", LongType, true),
    StructField("Production_Budget", LongType, true),
    StructField("Release_Date", StringType, true),
    StructField("MPAA_Rating", StringType, true),
    StructField("Running_Time_min", LongType, true),
    StructField("Distributor", StringType, true),
    StructField("Source", StringType, true),
    StructField("Major_Genre", StringType, true),
    StructField("Creative_Type", StringType, true),
    StructField("Director", StringType, true),
    StructField("Rotten_Tomatoes_Rating", LongType, true),
    StructField("IMDB_Rating", DoubleType, true),
    StructField("IMDB_Votes", LongType, true),
  ))

  val movie = spark.read
//    .format("jdbc")
    .schema(movieSchema)
    .options(Map(
//      "inferSchema" -> "true",
//      "mode" -> "failFast", //permissive by default
      "allowSingleQuotas" -> "true",
      "compression" -> "uncompressed"//,
//      "dateFormat" -> "yy-MMM-d"
    ))
    .json("src/main/resources/data/movies.json")

  println(movie.schema)
  movie.show()

  movie.write
    .format("csv")
    .mode(SaveMode.Overwrite)
//    .options(Map(
//      "sep" -> "\t",
//      "header" -> true,
//      "compression" -> "uncompressed"
//    ))
    .save("src/main/resources/data/movies.csv")

  movie.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")

  movie.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .options(Map(
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
      "dbtable" -> "public.movies",
      "user" -> "docker",
      "password" -> "docker"
    ))
    .save()

  val genres = movie.selectExpr("Major_Genre").distinct()
  genres.show()

  val advents = movie.selectExpr(
      "Title",
      "Release_Date",
      "Source",
      "Rotten_Tomatoes_Rating",
      "MPAA_Rating",
      "IMDB_Rating",
      "Production_Budget")
    .where("IMDB_Rating > 8")
  advents.show()

  val adventsRated = advents.withColumn("Prod_Budget_MLNS", advents.col("Production_Budget") / 1000000)
    .drop("Production_Budget")
  adventsRated.show()
}

