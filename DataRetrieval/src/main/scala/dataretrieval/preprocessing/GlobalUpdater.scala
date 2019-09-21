package dataretrieval.preprocessing

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import ucar.nc2.NetcdfFile
import java.net.URI

import dataretrieval.EccoSpark
import dataretrieval.netcdfhandling.NetCDFConverter
import dataretrieval.preprocessing.IndexFile.Date
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class GlobalUpdater(private val netCDFConverter: NetCDFConverter) extends Serializable {

  private val indexFile = new IndexFile(path = "ftp.ifremer.fr/ifremer/argo/ar_index_global_prof.txt")
  //private val indexFile = new IndexFile(path = "/home/manuel/Downloads/ar_index_global_prof.txt")

  // TODO: maybe find better name
  private def retrieveCurrentProgress(): Date = EccoSpark.loadLastUpdateDate() // should retrieve saved progress date

  // TODO: maybe find better name
  private def saveCurrentProgress(progress: Date): Unit = EccoSpark.saveDate(progress) // should save new progress date

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
          entry => Row.fromSeq(netCDFConverter.extractData(entry))
        }
        val schema = StructType(netCDFConverter.getSchema)
        EccoSpark.saveEccoData(rows, schema)

        saveCurrentProgress(maxDate)
        processBucket(maxDate)
      }
    }

    processBucket(retrieveCurrentProgress())
  }

}
