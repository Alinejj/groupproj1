"""
Trains a quick placeholder classifier on a sample of AG News so the
live classification query can be built and tested before the team's
final model (Part 2) is ready. Same pipeline stages as the final model.
"""
import argparse

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StopWordsRemover
from pyspark.sql import SparkSession


def parse_args():
    p = argparse.ArgumentParser(description="Train placeholder AG News model")
    p.add_argument("--train-path", required=True, help="AG News training Parquet file")
    p.add_argument("--model-path", required=True, help="Where to save the PipelineModel")
    p.add_argument("--sample-fraction", type=float, default=0.1)
    p.add_argument("--master", default="local[2]")
    return p.parse_args()


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("PlaceholderModel")
        .master(args.master)
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.train_path).select("text", "label")
    df = df.sample(fraction=args.sample_fraction, seed=42)
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    print(f"Training on {train.count()} articles, testing on {test.count()}")

    pipeline = Pipeline(stages=[
        RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W+", toLowercase=True),
        StopWordsRemover(inputCol="words", outputCol="filtered"),
        HashingTF(inputCol="filtered", outputCol="tf", numFeatures=1 << 16),
        IDF(inputCol="tf", outputCol="features", minDocFreq=2),
        LogisticRegression(featuresCol="features", labelCol="label", maxIter=20),
    ])

    model = pipeline.fit(train)
    accuracy = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    ).evaluate(model.transform(test))
    print(f"Placeholder model accuracy: {accuracy:.3f}")

    model.write().overwrite().save(args.model_path)
    print(f"Saved model to {args.model_path}")
    spark.stop()


if __name__ == "__main__":
    main()