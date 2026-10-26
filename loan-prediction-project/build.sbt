name := "LoanPredictionPipeline"
version := "1.0"
scalaVersion := "2.12.15"

fork := true

javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"           % "3.4.1",
  "org.apache.spark"  %% "spark-sql"            % "3.4.1",
  "org.apache.spark"  %% "spark-mllib"          % "3.4.1",
  "org.apache.spark"  %% "spark-streaming"      % "3.4.1",
  "org.apache.spark"  %% "spark-sql-kafka-0-10" % "3.4.1",
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.2.1",
  "org.mongodb"       %  "mongodb-driver-sync"   % "4.9.0"
)
