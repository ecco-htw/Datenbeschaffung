package main

import java.net.URI

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import preprocessing.{GlobalList, GlobalUpdater, IndexFile, ThisWeekList}
import netcdfhandling.buoyNetCDFConverter
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
  Logger.getLogger("org").setLevel(Level.WARN)

  val globalUpdater: GlobalUpdater = new GlobalUpdater(buoyNetCDFConverter)

  def main(args: Array[String]) {
    val start_time = System.currentTimeMillis()
    /** TIMER START */
    /*
    val index=new IndexFile()
    val rdd=index.data
    rdd.collect.foreach(x=>println(x.date.hour))
     */

    globalUpdater.update()

    /** TIMER END */
    val end_time = System.currentTimeMillis()
    println("time: " + (end_time-start_time))
  }

  def saveAll(start: Int, count: Int, global_list: GlobalList, rootFTP: String): Unit = {
    // if (count < global_list.lines) {

    println(start)
    //val rdd = global_list.toRDD((start, start + count))
    val rdd = EccoSpark.sparkContext.parallelize(global_list.getSubRDD((start, start + count)).collect())
    //val fhl = rdd.map(row => rootFTP + "/" + row.getString(0)).collect().toList
    //val fhl = rdd.map(row => rootFTP + "/" + row.getString(0))
    //fhl.foreach(saveDataMongoDB)


    /*
    val rows: RDD[Row] = rdd.map(row => buoyNetCDFConverter.extractData(NetcdfFile.openInMemory(new URI(rootFTP + "/" + row._1))))
    rows.collect()

    val dataFrame = spark.sqlContext.createDataFrame(rows, netCDFConverter.getSchema)
    dataFrame.write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode("append")
      .save()

     */

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
    /*
    val buoy_list = new ThisWeekList(sc, spark.sqlContext)
    val rootFTP = buoy_list.getRootFTP
    val weeklist = buoy_list.toRDD.map(row => rootFTP + "/" + row.getString(0)).collect().toList
     */

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

    /*
    val data = sc.parallelize(Seq(buoyNetCDFConverter.extractData(netCDFObject)))
    val schema = buoyNetCDFConverter.getSchema
    val dataFrame = spark.sqlContext.createDataFrame(data, schema)
    dataFrame.write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode("append")
      .save()
     */
  }

}
