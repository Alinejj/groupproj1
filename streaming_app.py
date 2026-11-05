"""
Part 1 streaming job: reads messages from Kafka (topic1), extracts named
entities with spaCy, keeps a running count per entity, and writes the
counts to Kafka (topic2) at every trigger.
"""
import argparse

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

# spaCy entity types worth counting (skips dates, numbers, money, etc.)
ENTITY_LABELS = {"PERSON", "ORG", "GPE", "LOC", "NORP", "PRODUCT", "EVENT"}

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
    p = argparse.ArgumentParser(description="Named entity counts from Kafka")
    p.add_argument("--bootstrap-servers", default="localhost:9092")
    p.add_argument("--input-topic", default="topic1")
    p.add_argument("--output-topic", default="topic2")
    p.add_argument("--checkpoint-dir", required=True,
                   help="Folder where Spark saves streaming progress")
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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    #read raw messages from topic1
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", args.starting_offsets)
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

    #live top-10 in the terminal 
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

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()