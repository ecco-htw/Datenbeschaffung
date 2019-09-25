package dataretrieval.netcdfhandling

import java.io.IOException
import java.net.URI

import dataretrieval.preprocessing.IndexFile.IndexFileEntry
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StructField, StructType}
import ucar.nc2._

object NetCDFConverter {

  /** Create a NetCDFConverter used to extract variables of a netcdf file and convert them to scala types.
    *
    * @param conversionInfos structure:
    *                        Seq( <br/>
    *                           (conversionFunction, structField), // variable 1 <br/>
    *                           (conversionFunction, structField), // variable 2 <br/>
    *                           (conversionFunction, structField), // variable 3 <br/>
    *                           ... <br/>
    *                        ) <br/>
    *                        <br/>
    *                        conversionFunction - a function that takes an IndexFileEntry and returns a variable extracted
    *                                             from it. Use extractVariable or extractFirstProfile for conveience <br/>
    *                        <br/>
    *                        structField - a StructField containing schema information about the variable for database storage <br>
    *                        <br/>
    *                        each entry in the sequence is for a separate variable <br/>
    */
  def createNetCDFConverter(conversionInfos: (IndexFileEntry => Option[Any], StructField)*): NetCDFConverter = {
    val (conversionFuncs, schemaInfo) = conversionInfos.unzip
    new NetCDFConverter(conversionFuncs, schemaInfo)
  }

  /**
    * Extract and return the value of a variable stored inside a netcdf file
    * @param name name of the variable
    * @param convFn function to convert the variable before returning
    * @param indexFileEntry the index file entry for the netcdf file
    * @tparam T type of the variable inside the netcdf file
    * @return the value of the variable if it exists None otherwise
    */
  def extractVariable[T](name: String, convFn: T => Any = (a: T) => identity(a))(indexFileEntry: IndexFileEntry): Option[Any] = {
    val netcdfFilePath = indexFileEntry.path
    try {
      val netcdfFile = NetcdfFile.openInMemory(new URI(indexFileEntry.path))
      val netcdfVar: Variable = netcdfFile.findVariable(name)
      if (netcdfVar == null) {
        Logger.getLogger("org").warn(s"The variable $name does not exist in NetCDF file $netcdfFilePath. This file will be skipped.")
        None
      }
      else Some(convFn(netcdfVar.read().copyToNDJavaArray()).asInstanceOf[T])
    } catch {
      case e: IOException => None
    }
  }

  /**
    * Convenient wrapper function for extractVariable that assumes an array of type T values inside the netcdf file and
    * extracts the first entry of the array. This is useful for Variables that contain multiple profiles while only the
    * first one is of interest. For more information see the documentation for extractVariable.
    * @param name name of the variable
    * @param convFn function to convert the variable before returning
    * @param indexFileEntry the index file entry for the netcdf file
    * @tparam T type of the variable inside the netcdf file
    * @return the value of the variable if it exists None otherwise
    */
  def extractFirstProfile[T](name: String, convFn: T => Any = (a: T) => identity(a))(indexFileEntry: IndexFileEntry): Option[Any] = {
    extractVariable[Array[T]](name, v => convFn(v.head))(indexFileEntry)
  }

}

/**
  * A class used to extract variables of a netcdf file and convert them to scala types. Use the NetCDFConverter.apply
  * function for better convenience
  * @param conversionFuncs a list of conversion functions for extracting variables from a netcdf file. Each list entry is
  *                        for a separate variable
  * @param schemaInfo a list of StructFields that describe the schema of the variables. Each entry is for a separate variable
  */
class NetCDFConverter(conversionFuncs: Seq[IndexFileEntry => Option[Any]], schemaInfo: Seq[StructField]) extends Serializable {

  def getSchema: StructType = StructType(schemaInfo)

  /**
    * Extract all variables specified in this NetCDFConverter object and return them in a sequence
    * @param indexFileEntry the index file entry for the netcdf file
    * @return if all variables were found inside the netcdf file returns them otherwise returns None
    */
  def extractData(indexFileEntry: IndexFileEntry): Option[Seq[Any]] = {
    // if you get an IllegalArgumentException on the following line it's probably because not
    // all conversion functions you provided created a List with the same size
    val list = conversionFuncs.map(fn => fn(indexFileEntry))
    if (list.contains(None)) None
    else Some(list.flatten)
  }
}
