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
  def apply(conversionInfos: (NetcdfFile => Any, StructField)*): NetCDFConverter = {
    val (conversionFuncs, schemaInfo) = conversionInfos.unzip
    new NetCDFConverter(conversionFuncs, schemaInfo)
  }

  def defaultfn[T]: AnyRef => Seq[T] = (any: AnyRef) => any.asInstanceOf[Array[T]].toSeq

  def firstProfile[T <: Seq[Any]](seq: T): Any = seq.head

  def extractFirstProfile[T](name: String, convFn: T => Any = (a: T) => identity(a))(netcdfFile: NetcdfFile): Any = {
    val ndJavaArray = netcdfFile.findVariable(name).read().copyToNDJavaArray().asInstanceOf[Array[T]].head
    convFn(ndJavaArray)
  }

  def extractFirstProfile2[T](name: String, convFn: T => Any = (a: T) => identity(a))(netcdfFile: NetcdfFile): Any = {
    val v = netcdfFile.findVariable(name)
    val ndJavaArray = v.read().copyToNDJavaArray().asInstanceOf[Array[T]].head
    val r = convFn(ndJavaArray)
    println(s"ndims($r): " + v.getDimensions.size())
    r
  }

  def extractVariable[T](name: String, convFn: T => Any = (a: T) => identity(a))(netcdfFile: NetcdfFile): Any = {
    val ndJavaArray = netcdfFile.findVariable(name).read().copyToNDJavaArray().asInstanceOf[T]
    convFn(ndJavaArray)
  }
}

class NetCDFConverter(conversionFuncs: Seq[NetcdfFile => Any], schemaInfo: Seq[StructField]) {

  def getSchema: StructType = StructType(schemaInfo)

  def extractData(netcdfFile: NetcdfFile): Row = {
    // if you get an IllegalArgumentException on the following line it's probably because not
    // all conversion functions you provided created a List with the same size
    Row.fromSeq(conversionFuncs.map(fn => fn(netcdfFile)))
  }
}
