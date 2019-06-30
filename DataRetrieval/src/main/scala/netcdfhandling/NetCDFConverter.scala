package netcdfhandling


import netcdfhandling.NetCDFConverter.ConversionInfo
import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import ucar.nc2._


object NetCDFConverter {
  def convertTo[T](ndJavaArray: AnyRef): List[T] = ndJavaArray.asInstanceOf[Array[T]].toList

  implicit class ConversionInfo(info: (NetcdfFile => List[_], StructField)) {
  }

  /**
    *
    * @param conversionInfos Tuple3(name: String, convFn: ndJavaArray => List[_], fieldSchema: StructField)
    *                        name = the name of the variable inside the NetCDF file
    *                        convFn = function that converts a ndJavaArray to a scala List
    *                        fieldSchema = StructField defining the name and type of the field
    */
  def apply(conversionInfos: (String, AnyRef => List[_], StructField)*): NetCDFConverter = {
    val info = conversionInfos.unzip(tuple => ((tuple._1, tuple._2), tuple._3))
    new NetCDFConverter(info._1, info._2)
  }
}

class NetCDFConverter(conversionInfo: Seq[(String, AnyRef => List[_])], schemaInfo: Seq[StructField]) {
  def getSchema: StructType = StructType(schemaInfo)
  def convertData(netcdfFile: NetcdfFile): List[Any] = {
    def extractVariable[T](name: String, convFn: AnyRef => List[T]) = {
      val ndJavaArray = netcdfFile.findVariable(name).read().copyToNDJavaArray()
      convFn[T](ndJavaArray)
    }

    val a = conversionInfo.map(info => extractVariable(info._1, info._2))

  }

}

class test() {
  import netcdfhandling.NetCDFConverter.convertTo
  def d() = NetCDFConverter(
    ("JULD", convertTo[Float], StructField("juld", FloatType))
  )
}
