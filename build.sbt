name := "spark-playground"
version := "0.1.0"
scalaVersion := "2.13.12"

val sparkVersion = "3.5.0"
val scalatestVersion = "3.2.17"
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.scalactic" %% "scalactic" % scalatestVersion,
    "org.scalatest" %% "scalatest" % scalatestVersion % "test",
    "org.postgresql" % "postgresql" % "42.7.3"
)

javaOptions ++= Seq(
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)