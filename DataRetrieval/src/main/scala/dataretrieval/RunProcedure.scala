package dataretrieval

import java.net.URI

import scala.collection.JavaConverters._
import akka.actor.ActorSystem
import dataretrieval.netcdfhandling.buoyNetCDFConverter
import dataretrieval.observer.FtpObserver
import dataretrieval.preprocessing.{GlobalUpdater, WeeklyUpdater}
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

    // uncomment the following to get an overview of the fields inside the given netcdfFile
    // val netCDFObject = NetcdfFile.openInMemory(new URI("ftp://ftp.ifremer.fr/ifremer/argo/dac/aoml/1901509/profiles/R1901509_293.nc"))
    // println(analyzeVariables(netCDFObject).mkString("\n"))

    doGlobalUpdate()

    val actorSystem = ActorSystem("eccoActorSystem")
    val ftpObserver = actorSystem.actorOf(FtpObserver.props(doWeeklyUpdate), "ftpObserver")
  }

  /**
   * debug helper: lists all fields of netcdfFile and their dataypes
   *
   * @param netcdfFile NetcdfFile
   * @param fieldNames if provided, shows only these fields
   * @return
   */
  def analyzeVariables(netcdfFile: NetcdfFile, fieldNames: String*): Seq[(String, String)] = {

    if (fieldNames.isEmpty) netcdfFile.getVariables.asScala.map(v => (v.getNameAndDimensions, v.getDataType.toString))
    else fieldNames.map(name => {
      val v = netcdfFile.findVariable(name)
      (v.getNameAndDimensions, v.getDataType.toString)
    })
  }

  def doGlobalUpdate(): Unit = {
    globalUpdater.update()
  }

  def doWeeklyUpdate(): Unit = {
    weeklyUpdater.update()
  }
}
