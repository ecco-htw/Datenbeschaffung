package main

import java.net.URI

import org.apache.log4j.{Level, Logger}
import akka.actor.{Actor, ActorSystem, Props}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import preprocessing.{GlobalList, ThisWeekList}
import netcdfhandling.{BuoyData, NetCDFConverter}
import observer.FtpObserver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, StringType, StructField}
import ucar.nc2.NetcdfFile

import collection.JavaConverters._

/** Main object to run argo-data retrieval.
  *
  * @author Raimi Solorzano Niederhausen - s0557978@htw-berlin.de
  */
object RunProcedure {

  // Omit INFO log in console
  val rootLogger = Logger.getLogger("org").setLevel(Level.WARN)
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
  val spark = SparkSession
    .builder()
    .appName("Spark SQL for Argo Data")
    .config(conf)
    .getOrCreate()


  import NetCDFConverter.{extractVariable, extractFirstProfile, extractFirstProfile2}

  // TODO: probably better to move this to NetCDFConverter instead of passing through the constructor (separation of concerns)
  val netCDFConverter = NetCDFConverter(
    (extractFirstProfile[Double]("JULD"), StructField("juld", DoubleType)),
    (extractFirstProfile[Int]("CYCLE_NUMBER"), StructField("cycleNumber", IntegerType)),
    (extractFirstProfile[Array[Char]]("FLOAT_SERIAL_NO", _.mkString.trim), StructField("floatSerialNo", StringType)),
    (extractFirstProfile[Array[Float]]("PRES", _.map(_.toDouble)), StructField("PRES", ArrayType(DoubleType))),
    (extractFirstProfile[Array[Float]]("TEMP", _.map(_.toDouble)), StructField("TEMP", ArrayType(DoubleType))),
    (extractFirstProfile[Array[Float]]("PSAL", _.map(_.toDouble)), StructField("PSAL", ArrayType(DoubleType))),
    (extractFirstProfile[Double]("LONGITUDE"), StructField("longitude", DoubleType)),
    (extractFirstProfile[Double]("LATITUDE"), StructField("latitude", DoubleType))
  )

  def main(args: Array[String]) {
    val start_time = System.currentTimeMillis()

    val buoyList = new GlobalList(sc, spark.sqlContext)

    //    val buoyList = GlobalList(sc, spark.sqlContext).contentRDD

    //saveAllFromThisWeekList
    //val actorSystem = ActorSystem("eccoActorSystem")

    //val ftpObserver = actorSystem.actorOf(FtpObserver.props(saveAllFromThisWeekList), "ftpObserver")
    //val text = GlobalReader(sc, spark.sqlContext).getTextRdd
    //val buoy_list = GlobalList(sc, spark.sqlContext, text)
    /*
    val firsthundred = buoy_list.toRDD((0, 100))
    val rootFTP = buoy_list.rootFTP
    val fhl = firsthundred.map(row => rootFTP + "/" + row.getString(0)).collect().toList
    saveDataMongoDB(fhl.head)
    val nexthundred = buoy_list.toRDD((100, 200))
    val nh = nexthundred.map(row => rootFTP + "/" + row.getString(0)).collect().toList
    saveDataMongoDB(nh.head)
    //sc.stop()
    //spark.stop()
    */
    println(buoyList.nLines)
    saveAll(0, buoyList.nLines.toInt, buoyList, buoyList.rootFTP)
    val end_time = System.currentTimeMillis()
    println("time: " + (end_time-start_time))
    // 68709
  }

  def saveAll(start: Int, count: Int, global_list: GlobalList, rootFTP: String): Unit = {
    // if (count < global_list.lines) {

    println(start)
    //val rdd = global_list.toRDD((start, start + count))
    val rdd = global_list.getSubRDD((start, start + count))
    //val fhl = rdd.map(row => rootFTP + "/" + row.getString(0)).collect().toList
    val fhl = rdd.map(row => rootFTP + "/" + row.getString(0))
    //fhl.foreach(saveDataMongoDB)
    val rows: RDD[Row] = fhl.map(filename => netCDFConverter.extractData(NetcdfFile.openInMemory(new URI(filename))))

    val dataFrame = spark.sqlContext.createDataFrame(rows, netCDFConverter.getSchema)
    dataFrame.write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode("append")
      .save()

    //saveAll(start + count, count, global_list, rootFTP)

  }

  /** Download the new argo data from: ftp://ftp.ifremer.fr/ifremer/argo/
    *
    * 1. Retrieve index of new NetCDF files from: ftp://ftp.ifremer.fr/ifremer/argo/ar_index_this_week_prof.txt
    * (contains only filenames now)
    * 2. Foreach new file run [[saveDataMongoDB]]
    *
    */
  def saveAllFromThisWeekList(): Unit = {
    val buoy_list = new ThisWeekList(sc, spark.sqlContext)
    val rootFTP = buoy_list.getRootFTP
    val weeklist = buoy_list.toRDD.map(row => rootFTP + "/" + row.getString(0)).collect().toList

    //weeklist.foreach(println)
    //weeklist.foreach(saveDataMongoDB)
    //    saveDataMongoDB(weeklist.head)
  }

  def analyzeVariables(netcdfFile: NetcdfFile, varnames: String*): Seq[(String, String)] = {
    if (varnames.isEmpty) netcdfFile.getVariables.asScala.map(v => (v.getNameAndDimensions, v.getDataType.toString))
    else varnames.map(name => {
      val v = netcdfFile.findVariable(name)
      (v.getNameAndDimensions, v.getDataType.toString)
    })
  }

  /** Store argo-data in mongodb of one specific NetCDF file.
    *
    * @param filename NetCDF file path.
    */
  def saveDataMongoDB(filename: String): Unit = {
    val netCDFObject = NetcdfFile.openInMemory(new URI(filename))

    // uncomment the following line to see a list of available variables and their type
    //println(analyzeVariables(netCDFObject).mkString("\n"))

    //println(netCDFObject.findDimension("N_PROF"))

    val data = sc.parallelize(Seq(netCDFConverter.extractData(netCDFObject)))
    val schema = netCDFConverter.getSchema
    val dataFrame = spark.sqlContext.createDataFrame(data, schema)
    dataFrame.write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode("append")
      .save()
  }

}
