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
    (extractFirstProfile[Int]("CONFIG_MISSION_NUMBER"), StructField("configMissionNumber", IntegerType)),
    (extractFirstProfile[Array[Char]]("PLATFORM_TYPE", _.mkString.trim), StructField("platformType", StringType)),
    (extractFirstProfile[Array[Char]]("PROJECT_NAME", _.mkString.trim), StructField("projectName", StringType)),
    (extractFirstProfile[Array[Char]]("PLATFORM_NUMBER", _.mkString.trim), StructField("platformNumber", StringType)),
    ((indexFileEntry: IndexFileEntry) => Some(indexFileEntry.date.str), StructField("dateUpdate", StringType))
  )
}
