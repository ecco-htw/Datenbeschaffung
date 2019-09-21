package dataretrieval.preprocessing

import java.net.URI

import dataretrieval.EccoSpark
import dataretrieval.netcdfhandling.NetCDFConverter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import ucar.nc2.NetcdfFile

class WeeklyUpdater(private val netCDFConverter: NetCDFConverter) extends Serializable {
  private val indexFile = new IndexFile(path = "ftp.ifremer.fr/ifremer/argo/ar_index_this_week_prof.txt")

  def update(): Unit = {
    val rows: RDD[Row] = indexFile.data.map {
      entry => Row.fromSeq(netCDFConverter.extractData(NetcdfFile.openInMemory(new URI(indexFile.rootFTP + "/" + entry.path))) :+ entry.date.date)
    }
    val schema = StructType(netCDFConverter.getSchema.toSeq :+ StructField("updateDate2", StringType))
    EccoSpark.saveEccoData(rows, schema)
  }


}
