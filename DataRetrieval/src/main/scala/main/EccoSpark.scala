package main

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object EccoSpark {

  // Use environment variables for authentication

  private val hadoopPassword = "kd23.S.W"
  private val hadoopUser = "ecco"
  // val hadoopPassword = sys.env("HTW_MONGO_PWD")
  private val hadoopDB = "ecco"
  private val hadoopCollection = "buoyTest"
  //val hadoopPort = sys.env.getOrElse("HTW_MONGO_PORT", "27020")
  //val hadoopHost = sys.env.getOrElse("HTW_MONGO_HOST", "hadoop05.f4.htw-berlin.de")
  private val hadoopPort = sys.env.getOrElse("HTW_MONGO_PORT", "27017")
  private val hadoopHost = sys.env.getOrElse("HTW_MONGO_HOST", "localhost")

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

  def loadLastUpdateDate(): String = ???
    //MongoSpark.load(spark, new ReadConfig(hadoopDB, "buoyMeta"))
}
