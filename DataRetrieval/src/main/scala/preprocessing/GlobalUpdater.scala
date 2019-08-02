package preprocessing

import main.EccoSpark
import netcdfhandling.NetCDFConverter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import preprocessing.IndexFile.Date
import ucar.nc2.NetcdfFile
import java.net.URI

class GlobalUpdater(private val netCDFConverter: NetCDFConverter) extends Serializable {

  //private val indexFile = new IndexFile(path = "ftp.ifremer.fr/ifremer/argo/ar_index_global_prof.txt")
  private val indexFile = new IndexFile(path = "/home/manuel/Downloads/ar_index_global_prof.txt")

  // TODO: maybe find better name
  private def retrieveCurrentProgress(): Date = EccoSpark.loadLastUpdateDate() // DUMMY // should retrieve saved progress date

  // TODO: maybe find better name
  private def saveCurrentProgress(progress: Date): Unit = EccoSpark.saveDate(progress) // DUMMY // should save new progress date

  def update(): Unit = {
    //EccoSpark.saveDate(Date("24210729090951"))
    //println(EccoSpark.loadLastUpdateDate())
    println("updating")
    val minBucketSize = 1000
    val fullRdd = indexFile.data.sortBy(_.date.date).zipWithIndex()

    def processBucket(progress: Date): Unit = {
      val remaining = fullRdd.filter { case (entry, index) => entry.date.date > progress.date }
      if (remaining.count() > 0) {
        val maxIndex = remaining.first()._2 + minBucketSize
        val maxDate = remaining.filter { case (entry, index) => index <= maxIndex }.sortBy(_._2, ascending = false).first()._1.date
        val bucket = remaining.flatMap {
          case (entry, index) if entry.date.date <= maxDate.date => Some(entry)
          case _ => None
        }.collect()
        val bucketRdd = EccoSpark.sparkContext.parallelize(bucket)

        //process and save bucketRDD
        val rows: RDD[Row] = bucketRdd.map {
          entry => netCDFConverter.extractData(NetcdfFile.openInMemory(new URI(indexFile.rootFTP + "/" + entry.path)))
        }
        EccoSpark.saveEccoData(rows, netCDFConverter.getSchema)

        saveCurrentProgress(maxDate)
        processBucket(maxDate)
      }
    }

    processBucket(retrieveCurrentProgress())
  }

}
