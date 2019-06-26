package preprocessing

import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

case class GlobalList(sc: SparkContext,
                      sqlContext: SQLContext,
                      path: String = "ftp.ifremer.fr/ifremer/argo/ar_index_this_week_prof.txt",
                      //path: String = "ftp.ifremer.fr/ifremer/argo/ar_index_global_prof.txt",
                      username: String = "anonymous",
                      password: String = "empty") extends Serializable {
  private[this] def fullpath: String = s"ftp://$username:$password@$path"

  sc.addFile(fullpath)
  val fileName: String = SparkFiles.get(fullpath.split("/").last)
  val fullRDD: RDD[String] = sc.textFile(fileName, 4)
  val nTopRows: Int =
    fullRDD
      .zipWithIndex()
      .filter { case (str, index) ⇒ !str.startsWith("#") }
      .first()._2.toInt + 1 // x rows starting with #, and one header row
  val rootFTP: String =
    fullRDD
      .take(nTopRows)
      .view
      .filter(str ⇒ str.startsWith("#") && str.contains("FTP root number 1"))
      .head.split(": ")(1).trim
  val contentRDD: RDD[Array[String]] =
    fullRDD
      .zipWithIndex()
      .filter(_._2 > nTopRows)
      .keys
      .map(str => str.split(","))
      .sortBy(_.last)
  val nLines: Long = contentRDD.count()

  def getSubRDD(range: (Long, Long)): RDD[Row] = {
    val lowerBound = range._1
    val upperBound = if (range._2 <= nLines) range._2 else nLines
    contentRDD
      .zipWithIndex()
      .filter { case (arr, index) ⇒ index >= lowerBound && index < upperBound }
      .map { case (arr, index) ⇒ Row.fromSeq(arr) }
    // TODO ? parallelize?
  }

  //  def toRDD(range: (Long, Long)): RDD[Row] = {
  //
  //    val row_arr = fetch_txt_range((range._1, get_limited_upper(range._2)))
  //
  //    val arr = row_arr.map(row => row.split(","))
  //    sc.parallelize(arr, 1000).map(row => Row.fromSeq(row))
  //
  //  }

  //  private[this] def drop_schema(range: (Long, Long), arr: Array[String]): Array[String] = {
  //    if (range._1 != 0)
  //      arr
  //    else {
  //      val schema = arr.zipWithIndex.view.filter(row => row._1.charAt(0) != '#').head
  //      println(s"schema=  $schema")
  //      arr.drop(schema._2 + 1)
  //    }
  //
  //  }

  //  val lines: Long = textRDD.count()

  //  val rootFTP: String = fetch_txt_range((0, 10)).
  //    view.filter(row => row.charAt(0) == '#' && row.contains("FTP root number 1")).head.split(": ")(1).trim

  //  private[this] def fetch_txt_range(line_range: (Long, Long)): Array[Array[String]] = {
  //
  //    val x = textRDD.zipWithIndex().filter(line_with_index => line_with_index._2 >= line_range._1
  //      && line_with_index._2 < line_range._2).map(_._1).collect()
  //    x
  //  }

}
