
name := "MarketDataTool"

version := "0.1"

// scalaVersion := "2.12.12"
scalaVersion := "2.12.16"


lazy val sparkVersion = "3.1.2"
lazy val kafkaVersion = "2.8.0"
lazy val avroVersion = "1.8.2"
lazy val avroSerializerVersion = "4.0.0"
lazy val testcontainersScalaVersion = "0.38.8"

Compile / mainClass  := Some("ArbitrageProcessing")
//assembly / mainClass := Some("ArbitrageProcessing")
assembly / assemblyJarName := "arb2.jar"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.scalaj" %% "scalaj-http" % "2.4.2",
 // "org.apache.parquet" %% "parquet-scala" % "1.12.2",
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core"   % "2.12.0",
  // Use the "provided" scope instead when the "compile-internal" scope is not supported
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "2.12.0" % "provided",
  "com.typesafe"      % "config"         % "1.4.0",
  "org.apache.hadoop" % "hadoop-client" % "3.2.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "ch.qos.logback" % "logback-classic" % "1.2.10",


  "org.apache.hadoop" % "hadoop-client" % "3.2.1",
  "org.postgresql"   % "postgresql" % "42.2.18",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  "org.apache.spark" %% "spark-core"  % sparkVersion,
  "org.apache.spark" %% "spark-sql"   % sparkVersion,
 // "org.apache.spark" %% "spark-catalyst" % sparkVersion,

  "org.apache.avro" % "avro" % avroVersion,
  "io.confluent" % "kafka-avro-serializer" % avroSerializerVersion
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

