package com.loan

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._

object ModelTrainer {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ModelTraining")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("Reading data from MongoDB...")

    val rawData = spark.read
      .format("mongodb")
      .option("connection.uri", "mongodb://localhost:27017")
      .option("database", "loan_db")
      .option("collection", "processed_applications")
      .load()

    println(s"Total records: ${rawData.count()}")

    // Use loan_status as the label (1=approved, 0=denied)
    val dataWithLabel = rawData
      .withColumn("label", col("loan_status").cast("double"))
      .na.fill(Map(
        "gender"             -> "Male",
        "married"            -> "Yes",
        "education"          -> "Graduate",
        "self_employed"      -> "No",
        "credit_history"     -> 1.0,
        "applicant_income"   -> 0.0,
        "coapplicant_income" -> 0.0,
        "loan_amount"        -> 0.0,
        "total_income"       -> 0.0,
        "loan_to_income_ratio" -> 0.0
      ))
      .filter(col("label").isNotNull)
      .filter(col("applicant_income") > 0)

    println(s"Records after cleaning: ${dataWithLabel.count()}")

    // Feature Engineering
    val genderIndexer    = new StringIndexer().setInputCol("gender").setOutputCol("gender_idx").setHandleInvalid("keep")
    val marriedIndexer   = new StringIndexer().setInputCol("married").setOutputCol("married_idx").setHandleInvalid("keep")
    val educationIndexer = new StringIndexer().setInputCol("education").setOutputCol("education_idx").setHandleInvalid("keep")
    val selfEmpIndexer   = new StringIndexer().setInputCol("self_employed").setOutputCol("self_employed_idx").setHandleInvalid("keep")
    val propertyIndexer  = new StringIndexer().setInputCol("property_area").setOutputCol("property_area_idx").setHandleInvalid("keep")

    val genderEncoder    = new OneHotEncoder().setInputCol("gender_idx").setOutputCol("gender_enc")
    val marriedEncoder   = new OneHotEncoder().setInputCol("married_idx").setOutputCol("married_enc")
    val educationEncoder = new OneHotEncoder().setInputCol("education_idx").setOutputCol("education_enc")
    val selfEmpEncoder   = new OneHotEncoder().setInputCol("self_employed_idx").setOutputCol("self_employed_enc")
    val propertyEncoder  = new OneHotEncoder().setInputCol("property_area_idx").setOutputCol("property_area_enc")

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "gender_enc", "married_enc", "education_enc",
        "self_employed_enc", "property_area_enc",
        "applicant_income", "coapplicant_income",
        "loan_amount", "credit_history",
        "total_income", "loan_to_income_ratio"
      ))
      .setOutputCol("features")
      .setHandleInvalid("keep")

    val Array(trainSet, testSet) = dataWithLabel.randomSplit(Array(0.8, 0.2), seed = 42)
    println(s"Train: ${trainSet.count()} | Test: ${testSet.count()}")

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    // Common stages for all pipelines
    val commonStages = Array(
      genderIndexer, marriedIndexer, educationIndexer, selfEmpIndexer, propertyIndexer,
      genderEncoder, marriedEncoder, educationEncoder, selfEmpEncoder, propertyEncoder,
      assembler
    )

    // Model 1: Logistic Regression
    println("\nTraining Logistic Regression...")
    val lrClassifier = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction_lr")
    val lrModel = new Pipeline().setStages(commonStages :+ lrClassifier).fit(trainSet)
    val lrAUC = evaluator.evaluate(lrModel.transform(testSet))

    // Model 2: Decision Tree
    println("Training Decision Tree...")
    val dtClassifier = new DecisionTreeClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction_dt")
    val dtModel = new Pipeline().setStages(commonStages :+ dtClassifier).fit(trainSet)
    val dtAUC = evaluator.evaluate(dtModel.transform(testSet))

    // Model 3: Random Forest
    println("Training Random Forest...")
    val rfClassifier = new RandomForestClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setNumTrees(20)
      .setPredictionCol("prediction_rf")
    val rfModel = new Pipeline().setStages(commonStages :+ rfClassifier).fit(trainSet)
    val rfAUC = evaluator.evaluate(rfModel.transform(testSet))

    // Model 4: Naive Bayes
    println("Training Naive Bayes...")
    val nbClassifier = new NaiveBayes()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction_nb")
    val nbModel = new Pipeline().setStages(commonStages :+ nbClassifier).fit(trainSet)
    val nbAUC = evaluator.evaluate(nbModel.transform(testSet))

    // Results
    println("\n==========================================")
    println("          MODEL COMPARISON RESULTS")
    println("==========================================")
    println(f"  Logistic Regression : $lrAUC%.4f")
    println(f"  Decision Tree        : $dtAUC%.4f")
    println(f"  Random Forest        : $rfAUC%.4f")
    println(f"  Naive Bayes          : $nbAUC%.4f")
    println("==========================================")

    val bestAUC  = Seq(lrAUC, dtAUC, rfAUC, nbAUC).max
    val (bestName, bestModel) = bestAUC match {
      case x if x == lrAUC => ("Logistic Regression", lrModel)
      case x if x == dtAUC => ("Decision Tree", dtModel)
      case x if x == rfAUC => ("Random Forest", rfModel)
      case _               => ("Naive Bayes", nbModel)
    }

    println(s"\n✓ Best model: $bestName (AUC: $bestAUC)")
    bestModel.write.overwrite().save("/tmp/best_loan_model")
    println("✓ Model saved to /tmp/best_loan_model\n")

    // Save metrics to MongoDB for Grafana
    import spark.implicits._
    val resultsDF = Seq(
      ("Logistic Regression", lrAUC),
      ("Decision Tree", dtAUC),
      ("Random Forest", rfAUC),
      ("Naive Bayes", nbAUC)
    ).toDF("model_name", "auc_score")

    resultsDF.write
      .format("mongodb")
      .option("connection.uri", "mongodb://localhost:27017")
      .option("database", "loan_db")
      .option("collection", "model_metrics")
      .mode("overwrite")
      .save()

    println("✓ Model metrics successfully saved to MongoDB collection: model_metrics\n")
  }
}
