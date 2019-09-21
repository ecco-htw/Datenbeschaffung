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

    //doGlobalUpdate()

    val actorSystem = ActorSystem("eccoActorSystem")
    val ftpObserver = actorSystem.actorOf(FtpObserver.props(doWeeklyUpdate), "ftpObserver")
  }

  def doGlobalUpdate(): Unit = {
    globalUpdater.update()
  }

  def doWeeklyUpdate(): Unit = {
    weeklyUpdater.update()
  }

}
