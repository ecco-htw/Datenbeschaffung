package dataretrieval

import org.apache.spark.sql.types._
import dataretrieval.netcdfhandling.NetCDFConverter.{extractFirstProfile, extractVariable}
import dataretrieval.preprocessing.IndexFile.IndexFileEntry

package object netcdfhandling {
  val buoyNetCDFConverter = NetCDFConverter(
    (extractFirstProfile[Double]("JULD"), StructField("juld", DoubleType)),
    (extractFirstProfile[Int]("CYCLE_NUMBER"), StructField("cycleNumber", IntegerType)),
    (extractFirstProfile[Array[Char]]("FLOAT_SERIAL_NO", _.mkString.trim), StructField("floatSerialNo", StringType)),
    (extractFirstProfile[Array[Float]]("PRES", _.map(_.toDouble)), StructField("PRES", ArrayType(DoubleType))),
    (extractFirstProfile[Array[Float]]("TEMP", _.map(_.toDouble)), StructField("TEMP", ArrayType(DoubleType))),
    (extractFirstProfile[Array[Float]]("PSAL", _.map(_.toDouble)), StructField("PSAL", ArrayType(DoubleType))),
    (extractFirstProfile[Double]("LONGITUDE"), StructField("longitude", DoubleType)),
    (extractFirstProfile[Double]("LATITUDE"), StructField("latitude", DoubleType)),
    (extractVariable[Array[Char]]("DATE_UPDATE", _.mkString.trim), StructField("dateUpdate", StringType)),
    ((indexFileEntry: IndexFileEntry) => indexFileEntry.date.str, StructField("dateUpdate2", StringType))
  )
}
