package dataretrieval.preprocessing


import dataretrieval.EccoSpark
import dataretrieval.netcdfhandling.NetCDFConverter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

class WeeklyUpdater(private val netCDFConverter: NetCDFConverter) extends Serializable {
  private val indexFile = new IndexFile(path = "ftp.ifremer.fr/ifremer/argo/ar_index_this_week_prof.txt")

  def update(): Unit = {
    val rows: RDD[Row] = indexFile.data.flatMap {
      entry => netCDFConverter.extractData(entry).map {
        list => Row.fromSeq(list)
      }
    }
    val schema = StructType(netCDFConverter.getSchema)
    EccoSpark.saveEccoData(rows, schema)
  }


}
