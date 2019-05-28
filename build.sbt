name := "meetup_api_trending_topics"

version := "0.1"

scalaVersion := "2.11.8"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies ++= {
  val sparkVer = "2.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer,
    "org.apache.spark" %% "spark-streaming" % sparkVer,
    "com.ning" % "async-http-client" % "1.9.10",
    "com.typesafe.play" %% "play-json" % "2.7.1",
    "com.typesafe" % "config" % "1.3.2",
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",
    "com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.10.0" % "test"
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}