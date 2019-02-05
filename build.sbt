name := "spark-dense-rank"

lazy val commonSettings = Seq(
  version := "0.1",
  organization := "com.example",
  scalaVersion := "2.12.8",
  test in assembly := {}
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"



lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("com.example.Main"),
    assemblyJarName in assembly := s"$name-$version.jar"
  )