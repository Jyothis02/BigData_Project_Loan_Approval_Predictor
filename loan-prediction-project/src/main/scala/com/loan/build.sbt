name := "LoanPredictionPipeline"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.1.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-mllib" % "3.3.2",
  "org.apache.spark" %% "spark-streaming" % "3.3.2",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.3.2",
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.1"
)
javaOptions ++= Seq(
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
)
fork := true
