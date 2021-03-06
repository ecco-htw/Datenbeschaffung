package dataretrieval.eccoutil

/** Customizable exception
  *
  * @param message
  * @param cause
  */
final class ArgoFloatException( private val message: String = "Error trying to create an ArgoFloatObject from raw data",
                                private val cause: Throwable = None.orNull) extends Exception(message, cause)
