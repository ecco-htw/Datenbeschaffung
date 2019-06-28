package main

import org.apache.log4j.{Level, Logger}
import akka.actor.{Actor, ActorSystem, Props}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import preprocessing.{GlobalList, ThisWeekList}
import netcdfhandling.BuoyData
import observer.FtpObserver
import org.apache.spark.rdd.RDD

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
  val hadoopDB = "ecco.buoy"
  val hadoopPort = sys.env.getOrElse("HTW_MONGO_PORT", "27020")
  val hadoopHost = sys.env.getOrElse("HTW_MONGO_HOST", "hadoop05.f4.htw-berlin.de")

  // Basic Spark configuration. Use 'buoy' as mongodb collection.
  val conf = new SparkConf()
    .setMaster("local[8]")
    .setAppName("HTW-Argo")
    //.set("spark.executor.memory", "471m")
    .set("spark.ui.port", "4050")
    .set("spark.mongodb.output.uri", s"mongodb://$hadoopUser:$hadoopPassword@$hadoopHost:$hadoopPort/$hadoopDB.buoyTestAll3")
    .set("spark.mongodb.input.uri", s"mongodb://$hadoopUser:$hadoopPassword@$hadoopHost:$hadoopPort/$hadoopDB.buoy?readPreference=primaryPreferred")
  val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName("Spark SQL for Argo Data")
    .config(conf)
    .getOrCreate()

  def main(args: Array[String]) {

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
    saveAll(0, buoyList, buoyList.rootFTP)
  }

  def saveAll(count: Int, global_list: GlobalList, rootFTP: String): Unit = {
    // if (count < global_list.lines) {

    println(count)
    //val rdd = global_list.toRDD((count, count + 500))
    val rdd = global_list.getSubRDD((count, count + 15))
    val fhl = rdd.map(row => rootFTP + "/" + row.getString(0)).collect().toList
    fhl.foreach(saveDataMongoDB)
    //saveAll(count + 500, global_list, rootFTP)

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

  /** Store argo-data in mongodb of one specific NetCDF file.
    *
    * @param filename NetCDF file path.
    */
  def saveDataMongoDB(filename: String): Unit = {
    val bd = new BuoyData(filename)
    val bdDF = bd.getDF(sc, spark.sqlContext)
    val extractCurrentParams = bdDF
      .select("parameter")
      .first
      .get(0)
      .asInstanceOf[Seq[Seq[String]]]
      .flatten
    val selectColumns = extractCurrentParams ++ Seq("floatSerialNo", "longitude", "latitude", "platformNumber", "projectName", "juld",
      "platformType", "configMissionNumber", "cycleNumber", "dateUpdate")
    bdDF.select(selectColumns.head, selectColumns.tail: _*).write.
      format("com.mongodb.spark.sql.DefaultSource").mode("append").
      save()
  }

}
