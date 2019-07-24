package preprocessing

import org.apache.spark.rdd.RDD
import preprocessing.IndexFile.{Date, IndexFileEntry}

class GlobalUpdater {
  val indexFile = new IndexFile("ftp.ifremer.fr/ifremer/argo/ar_index_global_prof.txt")

  // TODO: maybe find better name
  def retrieveCurrentProgress(): Date = ???

  def update(): Unit = {
  }
}
