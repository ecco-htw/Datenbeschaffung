package netcdfhandling


import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{FloatType, StructField, StructType}
import ucar.nc2._


object NetCDFConverter {
  /**
    *
    * @param conversionInfos Seq of Tuple2(convFn: ndJavaArray => Seq[_], fieldSchema: StructField)
    *                        convFn = function that extracts a specific Column of a NetCdf File and converts it to a scala List
    *                        fieldSchema = StructField defining the name and type of the field
    */
  def apply(conversionInfos: (NetcdfFile => Seq[_], StructField)*): NetCDFConverter = {
    val info = conversionInfos.unzip
    new NetCDFConverter(info._1, info._2)
  }

  def defaultfn[T]: AnyRef => Seq[T] = (any: AnyRef) => any.asInstanceOf[Array[T]].toSeq

  def extractVariable[T](name: String, convFn: AnyRef => Seq[T] = defaultfn[T])(netcdfFile: NetcdfFile): Seq[T] = {
    val ndJavaArray = netcdfFile.findVariable(name).read().copyToNDJavaArray()
    convFn(ndJavaArray)
  }
}

class NetCDFConverter(conversionFuncs: Seq[NetcdfFile => Seq[_]], schemaInfo: Seq[StructField]) {

  def getSchema: StructType = StructType(schemaInfo)

  def getData(netcdfFile: NetcdfFile): Seq[Row] = {
    // if you get an IllegalArgumentException on the following line it's probably because not
    // all conversion functions you provided created a List with the same size
    conversionFuncs.map(fn => fn(netcdfFile)).transpose.map(row => Row.fromSeq(row))
  }
}
