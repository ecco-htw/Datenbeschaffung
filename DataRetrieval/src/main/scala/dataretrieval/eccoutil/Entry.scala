package dataretrieval.eccoutil

/*
Represents an entry of the "IndexFile"-class
 */
case class Entry(line: String) {
  val path: String = line

}
