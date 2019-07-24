package db

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

class MongoDBManager {

  // Use environment variables for authentication
  val hadoopPassword = "kd23.S.W"
  val hadoopUser = "ecco"
  // val hadoopPassword = sys.env("HTW_MONGO_PWD")
  val hadoopDB = "ecco"
  //val hadoopPort = sys.env.getOrElse("HTW_MONGO_PORT", "27020")
  //val hadoopHost = sys.env.getOrElse("HTW_MONGO_HOST", "hadoop05.f4.htw-berlin.de")
  val hadoopPort = sys.env.getOrElse("HTW_MONGO_PORT", "27017")
  val hadoopHost = sys.env.getOrElse("HTW_MONGO_HOST", "localhost")

  // Basic Spark configuration. Use 'buoy' as mongodb collection.
  val conf = new SparkConf()
    .setMaster("local[8]")
    .setAppName("HTW-Argo")
    //.set("spark.executor.memory", "471m")
    .set("spark.ui.port", "4050")
    .set("spark.mongodb.output.uri", s"mongodb://$hadoopUser:$hadoopPassword@$hadoopHost:$hadoopPort/$hadoopDB.buoy")
    .set("spark.mongodb.input.uri", s"mongodb://$hadoopUser:$hadoopPassword@$hadoopHost:$hadoopPort/$hadoopDB.buoy?readPreference=primaryPreferred")
  val sc = new SparkContext(conf)
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark SQL for Argo Data")
    .config(conf)
    .getOrCreate()

  def saveRDD(rows: RDD[Row], schema: StructType): Unit = {
    val dataFrame = spark.sqlContext.createDataFrame(rows, schema)
    dataFrame.write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode("append")
      .save()
  }

}
