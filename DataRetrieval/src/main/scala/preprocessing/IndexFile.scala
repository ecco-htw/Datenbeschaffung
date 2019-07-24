package preprocessing

import org.apache.spark.rdd.RDD
import preprocessing.IndexFile.IndexFileEntry

class IndexFile(val filepath: String) {
  val data: RDD[IndexFileEntry] = ???
}

object IndexFile {
  case class Date(date: String) {
    def >(other: Date): Boolean = ???
    def <(other: Date): Boolean = ???
    def >=(other: Date): Boolean = ???
    def <=(other: Date): Boolean = ???
    def ==(other: Date): Boolean = ???
  }
  case class IndexFileEntry(path: String, date: Date)
}
