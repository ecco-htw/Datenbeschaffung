package dataretrieval

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import dataretrieval.preprocessing.IndexFile.Date
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object EccoSpark {

  private val mongoHost: String = sys.env.getOrElse("MONGO_HOST", throw new IllegalStateException("The environment variable MONGO_HOST is not set."))
  private val mongoPort: String = sys.env.getOrElse("MONGO_PORT", throw new IllegalStateException("The environment variable MONGO_PORT is not set."))
  private val mongoUser: String = sys.env.getOrElse("MONGO_USER", throw new IllegalStateException("The environment variable MONGO_USER is not set."))
  private val mongoPassword: String = sys.env.getOrElse("MONGO_PASSWORD", throw new IllegalStateException("The environment variable MONGO_PASSWORD is not set."))
  private val mongoDB: String = sys.env.getOrElse("MONGO_DB", throw new IllegalStateException("The environment variable MONGO_DB is not set."))
  private val dateURI = s"mongodb://$mongoUser:$mongoPassword@$mongoHost:$mongoPort/$mongoDB"


  // Basic Spark configuration. Use 'buoy' as mongodb collection.
  private val sparkConfig = new SparkConf()
    .setMaster("local[8]")
    .setAppName("HTW-Argo")
    .set("spark.ui.port", "4051")
    .set("spark.mongodb.output.uri", dateURI)
    .set("spark.mongodb.input.uri", dateURI)
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
    val rdd: RDD[Row] = sparkContext.parallelize(List(Row(0, date.str)))
    val df = spark.sqlContext.createDataFrame(rdd, StructType(List(StructField("_id", IntegerType), StructField("date", StringType))))
    val writeConfig = WriteConfig(Map("uri" -> dateURI))
    MongoSpark.save(df, writeConfig)
  }

  def loadLastUpdateDate(): Date = {
    val readConfig = ReadConfig(Map("uri" -> dateURI))
    val date = MongoSpark.load(sparkContext, readConfig)
    if (date.count() == 0) Date("00000000000000")
    else Date(date.map(_.getString("date")).first())
  }
}
