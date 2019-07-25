package preprocessing

import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import preprocessing.IndexFile.IndexFileEntry

class IndexFile(sc: SparkContext,
                sqlContext: SQLContext,
                path: String = "ftp.ifremer.fr/ifremer/argo/ar_index_this_week_prof.txt",
                //path: String = "ftp.ifremer.fr/ifremer/argo/ar_index_global_prof.txt",
                username: String = "anonymous",
                password: String = "empty") extends Serializable {
  private[this] def fullpath: String = s"ftp://$username:$password@$path"

  sc.addFile(fullpath)
  val fileName: String = SparkFiles.get(fullpath.split("/").last)
  val fullRDD: RDD[String] = sc.textFile(fileName, 30)
  val nTopRows: Int =
    fullRDD
      .filter { str ⇒ str.startsWith("#") }.count().toInt + 1
  val rootFTP: String = fullRDD.take(nTopRows).filter(line => line.contains("# FTP root number 1")).head.split(": ")(1).trim
  val data: RDD[IndexFileEntry] = fullRDD.zipWithIndex.filter(_._2 > nTopRows).map(lineAndIndex => IndexFileEntry(lineAndIndex._1))
}

object IndexFile {

  case class Date(date: String) {

    val year: Int = date.substring(0, 4).toInt
    val month: Int = date.substring(4, 6).toInt
    val day: Int = date.substring(6, 8).toInt
    val hour: Int = date.substring(8, 10).toInt
    val minute: Int = date.substring(10, 12).toInt
    val second: Int = date.substring(12, 14).toInt

    val units: List[Int] = List(year, month, day, hour, minute,second)


    def >(other: Date): Boolean = {

      def compare(ownUnits: List[Int], otherUnits: List[Int]): Boolean = {
        if (ownUnits.head > otherUnits.head) true
        else if (ownUnits.tail.isEmpty) false
        else compare(ownUnits.tail, otherUnits.tail)
      }

      compare(units, other.units)
    }

    def <(other: Date): Boolean = {
      def compare(ownUnits: List[Int], otherUnits: List[Int]): Boolean = {
        if (ownUnits.head < otherUnits.head) true
        else if (ownUnits.tail.isEmpty) false
        else compare(ownUnits.tail, otherUnits.tail)
      }

      compare(units, other.units)
    }

    def >=(other: Date): Boolean = {
      def compare(ownUnits: List[Int], otherUnits: List[Int]): Boolean = {
        if (ownUnits.head > otherUnits.head) true
        else if (ownUnits.head < otherUnits.head) false
        else if (ownUnits.tail.isEmpty) true
        else compare(ownUnits.tail, otherUnits.tail)
      }

      compare(units, other.units)
    }

    def <=(other: Date): Boolean = {
      def compare(ownUnits: List[Int], otherUnits: List[Int]): Boolean = {
        if (ownUnits.head < otherUnits.head) true
        else if (ownUnits.head > otherUnits.head) false
        else if (ownUnits.tail.isEmpty) true
        else compare(ownUnits.tail, otherUnits.tail)
      }

      compare(units, other.units)
    }

    def ==(other: Date): Boolean = {
      def compare(ownUnits: List[Int], otherUnits: List[Int]): Boolean = {
        if (ownUnits.head != otherUnits.head) false
        else if (ownUnits.tail.isEmpty) true
        else compare(ownUnits.tail, otherUnits.tail)
      }

      compare(units, other.units)
    }

  }


  case class IndexFileEntry(line: String) {
    val asArray: Array[String] = line.split(",")

    val date: Date = Date(asArray.last)
    val path: String = asArray.head
  }

}
