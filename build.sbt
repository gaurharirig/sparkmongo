

name := "SparkKafkaMongo"

version := "1.0"

scalaVersion := "2.10.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka_2.10
libraryDependencies ++= Seq(
    "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2",
    "org.apache.spark" % "spark-core_2.10" % "1.6.2",
    "org.apache.spark" % "spark-streaming_2.10" % "1.6.2",
    "com.google.code.gson" % "gson" % "1.7.1",
    "org.apache.httpcomponents" % "httpclient" % "4.5.2",
    "org.scalaj" % "scalaj-http_2.10" % "2.3.0",
    "com.typesafe" % "config" % "1.2.1",
    "com.novocode" % "junit-interface" % "0.8" % "test->default",
    "joda-time" % "joda-time" % "2.9.4",
    "org.mongodb" % "casbah-core_2.10" % "3.0.0",
    "com.novus" % "salat-core_2.10" % "1.9.9"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

    