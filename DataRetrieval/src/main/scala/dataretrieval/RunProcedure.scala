package dataretrieval

import java.net.URI

import akka.actor.ActorSystem
import dataretrieval.netcdfhandling.buoyNetCDFConverter
import dataretrieval.observer.FtpObserver
import dataretrieval.preprocessing.{GlobalList, GlobalUpdater, WeeklyUpdater}
import org.apache.log4j.{Level, Logger}
import ucar.nc2.NetcdfFile

/** Main object to run argo-data retrieval.
  *
  * @author Raimi Solorzano Niederhausen - s0557978@htw-berlin.de
  */
object RunProcedure {

  // Omit INFO log in console
  Logger.getLogger("org").setLevel(Level.WARN)
  val weeklyUpdater: WeeklyUpdater = new WeeklyUpdater(buoyNetCDFConverter)
  val globalUpdater: GlobalUpdater = new GlobalUpdater(buoyNetCDFConverter)

  def main(args: Array[String]): Unit = {

    doGlobalUpdate()

    val actorSystem = ActorSystem("eccoActorSystem")
    val ftpObserver = actorSystem.actorOf(FtpObserver.props(doWeeklyUpdate), "ftpObserver")
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

  /*
  def analyzeVariables(netcdfFile: NetcdfFile, varnames: String*): Seq[(String, String)] = {

    if (varnames.isEmpty) netcdfFile.getVariables.asScala.map(v => (v.getNameAndDimensions, v.getDataType.toString))
    else varnames.map(name => {
      val v = netcdfFile.findVariable(name)
      (v.getNameAndDimensions, v.getDataType.toString)
    })
  }

   */

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

  def doGlobalUpdate(): Unit = {
    globalUpdater.update()
  }

  def doWeeklyUpdate(): Unit = {
    weeklyUpdater.update()
  }
}
