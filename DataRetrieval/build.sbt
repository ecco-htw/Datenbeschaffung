name := "HTW-ECCO"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/edu.ucar/netcdf
libraryDependencies ++= Seq(
	"org.apache.spark" % "spark-core_2.11" % "2.0.0",
	"org.apache.spark" % "spark-sql_2.11" % "2.0.0",
	"edu.ucar" % "netcdf" % "4.2.20",
	"org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.3.1",
	"com.typesafe.akka" % "akka-actor_2.11" % "2.5.19",
	"junit" % "junit" % "4.11",
	"org.scalactic" %% "scalactic" % "3.0.5"
	//"org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

enablePlugins(JavaAppPackaging)