"""
Streaming job for Group Project 1.

Part 1: reads messages from Kafka (topic1), extracts named entities with
spaCy, keeps a running count per entity, and writes the counts to Kafka
(topic2) at every trigger.

Part 2 (2.3c): if --model-path is given, also applies the saved MLlib
PipelineModel to each message and prints running counts per predicted class.
"""
import argparse

import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

# spaCy entity types worth counting (skips dates, numbers, money, etc.)
ENTITY_LABELS = {"PERSON", "ORG", "GPE", "LOC", "NORP", "PRODUCT", "EVENT"}

# AG News label ids -> names
CLASS_NAMES = {0: "World", 1: "Sports", 2: "Business", 3: "Sci/Tech"}

MESSAGE_SCHEMA = StructType([
    StructField("text", StringType()),
    StructField("timestamp", StringType()),
])

_nlp = None


def get_nlp():
    """load spaCy once per worker process, not once per batch."""
    global _nlp
    if _nlp is None:
        import spacy
        _nlp = spacy.load("en_core_web_sm", disable=["parser", "lemmatizer"])
    return _nlp


@pandas_udf(ArrayType(StringType()))
def extract_entities(texts: pd.Series) -> pd.Series:
    nlp = get_nlp()
    results = []
    for doc in nlp.pipe(texts.fillna("").tolist(), batch_size=64):
        ents = [
            ent.text.strip()
            for ent in doc.ents
            if ent.label_ in ENTITY_LABELS and len(ent.text.strip()) > 1
        ]
        results.append(ents)
    return pd.Series(results)


def parse_args():
    p = argparse.ArgumentParser(description="Named entity counts and live classification from Kafka")
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--input-topic", default="topic1")
    p.add_argument("--output-topic", default="topic2")
    p.add_argument("--checkpoint-dir", required=True,
                   help="Folder where Spark saves streaming progress")
    p.add_argument("--model-path", default=None,
                   help="Saved PipelineModel; enables live classification counts")
    p.add_argument("--trigger", default="30 seconds",
                   help='How often to emit counts, e.g. "10 seconds"')
    p.add_argument("--starting-offsets", default="earliest",
                   choices=["earliest", "latest"])
    p.add_argument("--master", default="local[2]")
    p.add_argument("--show-console", action="store_true",
                   help="Also print the top 10 entities to the console")
    return p.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("EntityCountStream")
        .master(args.master)
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    #read raw messages from topic1
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    #parse the JSON and keep non-empty text
    messages = (
        raw.select(F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m"))
        .select("m.text")
        .where(F.col("text").isNotNull() & (F.length(F.trim("text")) > 0))
    )

    #one row per entity mention, then a running count per entity
    entities = messages.select(F.explode(extract_entities(F.col("text"))).alias("entity"))
    counts = entities.groupBy("entity").count()

    #send updated counts to topic2 as JSON
    kafka_out = counts.select(
        F.col("entity").alias("key"),
        F.to_json(F.struct(
            F.col("entity"),
            F.col("count"),
            F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss").alias("updated_at"),
        )).alias("value"),
    )

    (
        kafka_out.writeStream.format("kafka")
        .queryName("entity_counts_to_kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("topic", args.output_topic)
        .option("checkpointLocation", f"{args.checkpoint_dir}/entity_counts")
        .outputMode("update")
        .trigger(processingTime=args.trigger)
        .start()
    )

    #live top-10 entities in the terminal
    if args.show_console:
        (
            counts.orderBy(F.desc("count"))
            .writeStream.format("console")
            .queryName("entity_counts_console")
            .outputMode("complete")
            .option("numRows", 10)
            .option("truncate", False)
            .option("checkpointLocation", f"{args.checkpoint_dir}/console")
            .trigger(processingTime=args.trigger)
            .start()
        )

    #part 2 (2.3c): classify each message and keep running counts per class
    if args.model_path:
        model = PipelineModel.load(args.model_path)
        class_map = F.create_map(*[F.lit(x) for k, v in CLASS_NAMES.items() for x in (k, v)])

        class_counts = (
            model.transform(messages)
            .select(class_map[F.col("prediction").cast("int")].alias("predicted_class"))
            .groupBy("predicted_class")
            .count()
        )

        (
            class_counts.orderBy(F.desc("count"))
            .writeStream.format("console")
            .queryName("class_counts_console")
            .outputMode("complete")
            .option("truncate", False)
            .option("checkpointLocation", f"{args.checkpoint_dir}/class_counts")
            .trigger(processingTime=args.trigger)
            .start()
        )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()