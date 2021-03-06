package dataretrieval.preprocessing

import dataretrieval.EccoSpark
import dataretrieval.preprocessing.IndexFile.{Date, IndexFileEntry}
import org.apache.spark.SparkFiles
import org.apache.spark.rdd.RDD

class IndexFile(path: String,
                username: String = "anonymous",
                password: String = "empty") extends Serializable {

  /** LOCAL FILE **/
  //private[this] def fullpath: String = s"tmp/ar_index_this_week_prof.txt"

  //val fullRDD: RDD[String] = EccoSpark.sparkContext.textFile(fullpath, 30)
  /** **************/

  /** REMOTE FILE **/
  private[this] def fullpath: String = s"ftp://$username:$password@$path"
  EccoSpark.sparkContext.addFile(fullpath)
  val fileName: String = SparkFiles.get(fullpath.split("/").last)
  val fullRDD: RDD[String] = EccoSpark.sparkContext.textFile(fileName, 30)
  /** ***************/

  val headerLineCount: Int =
    fullRDD
      .filter { str ⇒ str.startsWith("#") }.count().toInt + 1
  val rootFTP: String = fullRDD.take(headerLineCount).filter(line => line.contains("# FTP root number 1")).head.split(": ")(1).trim
  val data: RDD[IndexFileEntry] = fullRDD.zipWithIndex.filter(_._2 > headerLineCount)
    .map(lineAndIndex => {
      val args = lineAndIndex._1.split(",")
      IndexFileEntry(rootFTP + "/" + args.head, Date(args.last))
    })
}

object IndexFile {

  case class Date(str: String) {

    val year: Int = str.substring(0, 4).toInt
    val month: Int = str.substring(4, 6).toInt
    val day: Int = str.substring(6, 8).toInt
    val hour: Int = str.substring(8, 10).toInt
    val minute: Int = str.substring(10, 12).toInt
    val second: Int = str.substring(12, 14).toInt

    val units: List[Int] = List(year, month, day, hour, minute, second)

    def >(other: Date): Boolean = str > other.str

    def <(other: Date): Boolean = str < other.str

    def >=(other: Date): Boolean = str >= other.str

    def <=(other: Date): Boolean = str <= other.str

    def ==(other: Date): Boolean = str == other.str
  }

  case class IndexFileEntry(path: String, date: Date)

}
