package preprocessing

import main.EccoSpark
import netcdfhandling.NetCDFConverter
import preprocessing.IndexFile.Date
import ucar.nc2.NetcdfFile

class GlobalUpdater(private val netCDFConverter: NetCDFConverter) {

  private val indexFile = new IndexFile(path = "ftp.ifremer.fr/ifremer/argo/ar_index_global_prof.txt")

  // TODO: maybe find better name
  private def retrieveCurrentProgress(): Date = Date("20190615090951") // DUMMY // should retrieve saved progress date

  // TODO: maybe find better name
  private def saveCurrentProgress(progress: Date): Unit = {} // DUMMY // should save new progress date

  def update(): Unit = {
    val minBucketSize = 1000
    val fullRdd = indexFile.data.sortBy(_.date.date).zipWithIndex()

    def processBucket(progress: Date): Unit = {
      val remaining = fullRdd.filter { case (entry, index) => entry.date > progress }
      if (remaining.count() > 0) {
        val maxIndex = remaining.first()._2 + minBucketSize
        val maxDate = remaining.filter { case (entry, index) => index == maxIndex }.first()._1.date
        val bucketRdd = remaining.filter { case (entry, index) => entry.date <= maxDate }

        //process and save bucketRDD
        val rows = bucketRdd.map {
          case (entry, index) => netCDFConverter.extractData(NetcdfFile.openInMemory(indexFile.rootFTP + "/" + entry.path))
        }
        EccoSpark.saveEccoData(rows, netCDFConverter.getSchema)

        saveCurrentProgress(maxDate)
        processBucket(maxDate)
      }
    }

    processBucket(retrieveCurrentProgress())
  }

}
