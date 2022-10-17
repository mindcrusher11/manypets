ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.8"

lazy val root = (project in file("."))
  .settings(
    name := "manypets",
    idePackagePrefix := Some("org.manypets.cam")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Versions.spark,
  "org.apache.spark" %% "spark-sql" % Versions.spark,
  "org.apache.spark" %% "spark-mllib" % Versions.spark,
  "org.apache.spark" %% "spark-streaming" % Versions.spark,
  /*"org.apache.spark" %% "spark-hive" % Versions.spark,*/
  /*"com.databricks" %% "spark-csv" % Versions.dataBricksCsv,*/
  "com.typesafe" % "config" % Versions.config
 /* "org.scalatest" %% "scalatest" % Versions.scalaTest % Test,
  "org.scalacheck" %% "scalacheck" % Versions.scalaCheck % Test,
  "com.holdenkarau" %% "spark-testing-base" % Versions.sparkTestingBase % Test,
  "org.apache.logging.log4j" % "log4j-api" % Versions.log4japi,
  "org.apache.logging.log4j" % "log4j-core" % Versions.log4jcore,
  "org.apache.logging.log4j" %% "log4j-api-scala" % Versions.log4japiscala*/
)
