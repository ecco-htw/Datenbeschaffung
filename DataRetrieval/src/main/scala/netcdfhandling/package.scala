import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField}
import netcdfhandling.NetCDFConverter.{extractFirstProfile, extractVariable}

package object netcdfhandling {
  val buoyNetCDFConverter = NetCDFConverter(
    (extractFirstProfile[Double] ("JULD"), StructField ("juld", DoubleType) ),
    (extractFirstProfile[Int] ("CYCLE_NUMBER"), StructField ("cycleNumber", IntegerType) ),
    (extractFirstProfile[Array[Char]] ("FLOAT_SERIAL_NO", _.mkString.trim), StructField ("floatSerialNo", StringType) ),
    (extractFirstProfile[Array[Float]] ("PRES", _.map (_.toDouble) ), StructField ("PRES", ArrayType (DoubleType) ) ),
    (extractFirstProfile[Array[Float]] ("TEMP", _.map (_.toDouble) ), StructField ("TEMP", ArrayType (DoubleType) ) ),
    (extractFirstProfile[Array[Float]] ("PSAL", _.map (_.toDouble) ), StructField ("PSAL", ArrayType (DoubleType) ) ),
    (extractFirstProfile[Double] ("LONGITUDE"), StructField ("longitude", DoubleType) ),
    (extractFirstProfile[Double] ("LATITUDE"), StructField ("latitude", DoubleType) )
  )
}
