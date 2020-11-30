name := "flink-experiment"

version := "0.1.0"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % "1.11.1",
  "org.apache.flink" %% "flink-test-utils" % "1.11.1"
)
