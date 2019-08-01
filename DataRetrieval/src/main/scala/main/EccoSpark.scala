package main

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import preprocessing.IndexFile.Date

object EccoSpark {

  // Use environment variables for authentication

  private val hadoopPassword = "kd23.S.W"
  private val hadoopUser = "ecco"
  // val hadoopPassword = sys.env("HTW_MONGO_PWD")
  private val hadoopDB = "ecco"
  private val hadoopCollection = "buoyTest"
  private val dateCollection = "latestProgress"
  //val hadoopPort = sys.env.getOrElse("HTW_MONGO_PORT", "27020")
  //val hadoopHost = sys.env.getOrElse("HTW_MONGO_HOST", "hadoop05.f4.htw-berlin.de")
  //private val hadoopPort = sys.env.getOrElse("HTW_MONGO_PORT", "27017")
  private val hadoopPort = sys.env.getOrElse("HTW_MONGO_PORT", "27020")
  private val hadoopHost = sys.env.getOrElse("HTW_MONGO_HOST", "localhost")
  private val dateURI = s"mongodb://$hadoopUser:$hadoopPassword@$hadoopHost:$hadoopPort/$hadoopDB.$dateCollection"


  // Basic Spark configuration. Use 'buoy' as mongodb collection.
  private val sparkConfig = new SparkConf()
    .setMaster("local[8]")
    .setAppName("HTW-Argo")
    //.set("spark.executor.memory", "471m")
    .set("spark.ui.port", "4050")
    .set("spark.mongodb.output.uri", s"mongodb://$hadoopUser:$hadoopPassword@$hadoopHost:$hadoopPort/$hadoopDB.$hadoopCollection")
    .set("spark.mongodb.input.uri", s"mongodb://$hadoopUser:$hadoopPassword@$hadoopHost:$hadoopPort/$hadoopDB.$hadoopCollection?readPreference=primaryPreferred")
  val sparkContext = new SparkContext(sparkConfig)
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark SQL for Argo Data")
    .config(sparkConfig)
    .getOrCreate()

  def saveEccoData(rows: RDD[Row], schema: StructType): Unit = {
    val dataFrame = spark.sqlContext.createDataFrame(rows, schema)
    dataFrame.write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode("append")
      .save()
  }

  def saveDate(date: Date): Unit = {

    val schema = StructField("date", StringType)
    val schemaType = StructType(List(schema))
    val rdd: RDD[Row] = sparkContext.parallelize(List(Row(date.date)))
    val df = spark.sqlContext.createDataFrame(rdd, schemaType)
    val writeConfig = WriteConfig(Map("uri" -> dateURI))
    MongoSpark.save(df, writeConfig)
  }

  def loadLastUpdateDate(): Date = {
    val readConfig = ReadConfig(Map("uri" -> dateURI))
    val x = MongoSpark.load(sparkContext, readConfig)
    x.map(x => Date(x.getString("date"))).sortBy(_.date, ascending = false).first()
  }

  //MongoSpark.load(spark, new ReadConfig(hadoopDB, "buoyMeta"))
}
