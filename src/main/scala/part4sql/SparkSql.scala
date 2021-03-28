package part4sql

//import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SaveNamespaceRequestProto
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object SparkSql extends App {
  val spark = SparkSession.builder()
    .appName("Spark SQL")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "file:/home/ihor/IdeaProjects/spark-essentials/src/main/resources/warehouse")
//    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") //Spark <3
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

//  val carsDF = spark.read
//    .option("inferSchema", "true")
//    .json("src/main/resources/data/cars.json")
//
//  carsDF.select(col("Name"))
//  .where(col("Origin") === "USA").show
//
//  carsDF.createOrReplaceTempView("cars")
//  val americanCars = spark.sql(
//    """
//      |select Name from cars where Origin = 'USA'
//      |""".stripMargin
//  )

//  americanCars.show

//  spark.sql("create database rtjvm")

  spark.sql("show databases").show
  spark.sql("use rtjvm").show
//  spark.sql("use default").show
  //transfer
  def readTable(name: String) =
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("user", "docker")
      .option("password", "docker")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
      .option("dbtable", "public." + name)
      .load()

  def transferTable(tableName: String) = {
    val tableDF = readTable(tableName)
//    tableDF.createOrReplaceTempView(tableName)
    tableDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName + "_wh")
  }

//  List(
//    "employees",
//    "departments",
//    "titles", "dept_emp", "dept_manager", "salaries")
//    .foreach( tableName =>
//      try
//        transferTable(tableName)
//      catch {
//        case e: AnalysisException => println(s"Table $tableName already exists")
//      }
//    )

  spark.catalog.listDatabases.foreach(db => println(db))
  spark.catalog.listTables.show

  def tryShow(tableName: String) =
    try
      spark.read.table(tableName).show
    catch {
      case e: AnalysisException => {
        println(s"Table or view $tableName not found")
      }
    }

  tryShow("employees")
  tryShow("employees_wh")
}
