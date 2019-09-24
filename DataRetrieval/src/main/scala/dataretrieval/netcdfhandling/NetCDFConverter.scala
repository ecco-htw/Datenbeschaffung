package dataretrieval.netcdfhandling

import java.net.URI

import dataretrieval.preprocessing.IndexFile
import dataretrieval.preprocessing.IndexFile.IndexFileEntry
import org.apache.spark.sql.types.{StructField, StructType}
import ucar.nc2._


object NetCDFConverter {
  /**
    *
    * @param conversionInfos Seq of Tuple2(convFn: ndJavaArray => Seq[_], fieldSchema: StructField)
    *                        convFn = function that extracts a specific Column of a NetCdf File and converts it to a scala List
    *                        fieldSchema = StructField defining the name and type of the field
    */
  def apply(conversionInfos: (IndexFileEntry => Option[Any], StructField)*): NetCDFConverter = {
    val (conversionFuncs, schemaInfo) = conversionInfos.unzip
    new NetCDFConverter(conversionFuncs, schemaInfo)
  }

  private def getVariable(netcdfFilePath: String, variableName: String): Object = {
    val netcdfFile = NetcdfFile.openInMemory(new URI(netcdfFilePath))
    netcdfFile.findVariable(variableName).read().copyToNDJavaArray()
  }

  def extractFirstProfile[T](name: String, convFn: T => Any = (a: T) => identity(a))(indexFileEntry: IndexFileEntry): Option[Any] = {
    val variable = getVariable(indexFileEntry.path, name)
    if (variable == null) None
    else Some(convFn(variable.asInstanceOf[Array[T]].head))
  }

  def extractVariable[T](name: String, convFn: T => Any = (a: T) => identity(a))(indexFileEntry: IndexFileEntry): Option[Any] = {
    val variable = getVariable(indexFileEntry.path, name)
    if (variable == null) None
    else Some(convFn(variable.asInstanceOf[T]))
  }
}

class NetCDFConverter(conversionFuncs: Seq[IndexFileEntry => Option[Any]], schemaInfo: Seq[StructField]) extends Serializable {

  def getSchema: StructType = StructType(schemaInfo)

  def extractData(indexFileEntry: IndexFileEntry): Seq[Any] = {
    // if you get an IllegalArgumentException on the following line it's probably because not
    // all conversion functions you provided created a List with the same size
    conversionFuncs.flatMap(fn => fn(indexFileEntry))
  }
}
