package preprocessing

import db.MongoDBManager
import netcdfhandling.NetCDFConverter
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import preprocessing.IndexFile.Date
import ucar.nc2.NetcdfFile

class GlobalUpdater(val netCDFConverter: NetCDFConverter, val mongoDBManager: MongoDBManager, val sqlContext: SQLContext,
                    val sparkContext: SparkContext) {
  val indexFile = new IndexFile(sparkContext, sqlContext, path = "ftp.ifremer.fr/ifremer/argo/ar_index_global_prof.txt")

  // TODO: maybe find better name
  def retrieveCurrentProgress(): Date = ??? // should retrieve saved progress date

  // TODO: maybe find better name
  def saveCurrentProgress(progress: Date): Unit = ??? // should save new progress date

  def update(): Unit = {
    val minBucketSize = 1000
    val fullRdd = indexFile.data.sortBy(_.date.date).zipWithIndex()

    def processBucket(progress: Date): Unit = {
      val remaining = fullRdd.filter { case (entry, index) => entry.date > progress }
      if (remaining.count() > 0) {
        val maxDate = remaining.filter { case (entry, index) => index == remaining.first()._2 + minBucketSize }.first()._1.date
        val bucketRdd = remaining.filter { case (entry, index) => entry.date <= maxDate }

        //process and save bucketRDD
        val rows = bucketRdd.map {
          case (entry, index) => netCDFConverter.extractData(NetcdfFile.openInMemory(indexFile.rootFTP + "/" + entry.path))
        }
        mongoDBManager.saveRDD(rows, netCDFConverter.getSchema)

        saveCurrentProgress(maxDate)
        processBucket(maxDate)
      }
    }

    processBucket(retrieveCurrentProgress())
  }

}
