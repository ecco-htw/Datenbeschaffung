package dataretrieval.netcdfhandling

import java.io.IOException
import java.net.URI

import dataretrieval.preprocessing.IndexFile.IndexFileEntry
import org.apache.log4j.Logger
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

  private def getVariable(netcdfFilePath: String, variableName: String): Option[Object] = {
    try {
      val netcdfFile = NetcdfFile.openInMemory(new URI(netcdfFilePath))
      val netcdfVar: Variable = netcdfFile.findVariable(variableName)
      if (netcdfVar == null) {
        Logger.getLogger("org").warn(s"The variable $variableName does not exist in NetCDF file $netcdfFilePath. This file will be skipped.")
        None
      }
      else Some(netcdfVar.read().copyToNDJavaArray())
    } catch {
      case e: IOException => None
    }
  }

  def extractFirstProfile[T](name: String, convFn: T => Any = (a: T) => identity(a))(indexFileEntry: IndexFileEntry): Option[Any] = {
    extractVariable[Array[T]](name, v => convFn(v.head))(indexFileEntry)
  }

  def extractVariable[T](name: String, convFn: T => Any = (a: T) => identity(a))(indexFileEntry: IndexFileEntry): Option[Any] = {
    getVariable(indexFileEntry.path, name).map {
      v => convFn(v.asInstanceOf[T])
    }
  }
}

class NetCDFConverter(conversionFuncs: Seq[IndexFileEntry => Option[Any]], schemaInfo: Seq[StructField]) extends Serializable {

  def getSchema: StructType = StructType(schemaInfo)

  def extractData(indexFileEntry: IndexFileEntry): Option[Seq[Any]] = {
    // if you get an IllegalArgumentException on the following line it's probably because not
    // all conversion functions you provided created a List with the same size
    val list = conversionFuncs.map(fn => fn(indexFileEntry))
    if (list.contains(None)) None
    else Some(list.flatten)
  }
}
