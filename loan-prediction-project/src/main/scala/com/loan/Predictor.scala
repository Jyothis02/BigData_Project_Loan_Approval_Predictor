package com.loan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.mongodb.client.MongoClients
import org.bson.Document

object Predictor {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("LoanPredictor")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = StructType(Array(
      StructField("application_id", LongType),
      StructField("gender", StringType),
      StructField("married", StringType),
      StructField("dependents", IntegerType),
      StructField("education", StringType),
      StructField("self_employed", StringType),
      StructField("applicant_income", DoubleType),
      StructField("coapplicant_income", DoubleType),
      StructField("loan_amount", DoubleType),
      StructField("loan_term", IntegerType),
      StructField("credit_history", DoubleType),
      StructField("property_area", StringType),
      StructField("loan_status", IntegerType),
      StructField("timestamp", DoubleType)
    ))

    println("✓ Spark started! Reading from Kafka...")

    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "loan_applications")
      .option("startingOffsets", "latest")
      .load()

    val parsedDF = kafkaDF
      .select(
        from_json(
          col("value").cast("string"),
          schema
        ).alias("data")
      )
      .select("data.*")

    val cleanedDF = parsedDF
      .na.fill(Map(
        "gender"         -> "Male",
        "married"        -> "Yes",
        "education"      -> "Graduate",
        "self_employed"  -> "No",
        "credit_history" -> 1.0
      ))
      .filter(col("applicant_income") > 0)
      .filter(col("loan_amount") > 0)

    val engineeredDF = cleanedDF
      .withColumn("total_income",
        col("applicant_income") + col("coapplicant_income"))
      .withColumn("loan_to_income_ratio",
        col("loan_amount") / col("total_income"))
      .withColumn("prediction",
        when(
          col("credit_history") === 1.0 &&
          col("total_income") > 4000,
          1
        ).otherwise(0))
      .withColumn("confidence",
        when(col("prediction") === 1, 0.87).otherwise(0.72))

    // Write to MongoDB using foreachBatch
    val query = engineeredDF
      .writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
        println(s"✓ Processing batch $batchId with ${batchDF.count()} records")

        // Write to MongoDB
        batchDF
          .write
          .format("mongodb")
          .mode("append")
          .option("connection.uri", "mongodb://localhost:27017")
          .option("database", "loan_db")
          .option("collection", "processed_applications")
          .save()

        // Also write predictions separately
        batchDF
          .select(
            col("application_id"),
            col("prediction"),
            col("confidence"),
            col("total_income"),
            col("loan_amount"),
            col("timestamp")
          )
          .write
          .format("mongodb")
          .mode("append")
          .option("connection.uri", "mongodb://localhost:27017")
          .option("database", "loan_db")
          .option("collection", "predictions")
          .save()

        println(s"✓ Batch $batchId written to MongoDB!")
      }
      .option("checkpointLocation", "/tmp/loan_checkpoint")
      .start()

    println("✓ Pipeline running! Writing to MongoDB...")
    query.awaitTermination()
  }
}
