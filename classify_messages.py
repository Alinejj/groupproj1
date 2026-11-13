import argparse
import os

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Spark connector needed to read from Kafka
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

# AG News label ids and the class names they stand for
CLASS_NAMES = {0: "World", 1: "Sports", 2: "Business", 3: "Sci/Tech"}

# fields we read from each topic1 message (extra fields such as id and source are ignored)
MESSAGE_SCHEMA = StructType([
    StructField("text", StringType()),
    StructField("timestamp", StringType())
])


def parse_args():
    # create the command-line argument parser
    parser = argparse.ArgumentParser(
        description="Batch-classify the streamed messages"
    )

    # specify the saved PipelineModel from train_model.py
    parser.add_argument(
        "--model-path",
        required=True
    )

    # specify the folder for all output files
    parser.add_argument(
        "--output-dir",
        required=True
    )

    # specify the Kafka broker address
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092"
    )

    # specify the Kafka topic with the streamed messages
    parser.add_argument(
        "--input-topic",
        default="topic1"
    )

    # optionally read the producer's saved JSONL file instead of Kafka
    parser.add_argument(
        "--input-jsonl",
        default=None
    )

    # specify how many random messages go in the hand-labeling sheet
    parser.add_argument(
        "--sample-size",
        type=int,
        default=50
    )

    # specify how many group members split the hand labeling
    parser.add_argument(
        "--labelers",
        type=int,
        default=4
    )

    # specify how many example messages to show per class
    parser.add_argument(
        "--examples-per-class",
        type=int,
        default=5
    )

    # specify the random seed so the sample and examples can be reproduced
    parser.add_argument(
        "--seed",
        type=int,
        default=42
    )

    # replace an existing hand-labeling sheet (this erases any labels in it)
    parser.add_argument(
        "--overwrite-sample",
        action="store_true"
    )

    # specify the Spark master
    parser.add_argument(
        "--master",
        default="local[2]"
    )

    return parser.parse_args()


def read_messages(spark, args):
    # read the producer's JSONL file if one was given
    if args.input_jsonl:
        return (
            spark.read.schema(MESSAGE_SCHEMA).json(args.input_jsonl)
            .withColumn("msg_id", F.monotonically_increasing_id().cast("string"))
            .select("msg_id", "text", "timestamp")
        )

    # otherwise read every message in the topic with a batch Kafka read
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # use partition-offset as a unique message id and parse the JSON value
    return raw.select(
        F.concat_ws("-", F.col("partition"), F.col("offset")).alias("msg_id"),
        F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m")
    ).select("msg_id", "m.text", "m.timestamp")


def shorten(text, n=220):
    # collapse whitespace and cut long messages so tables stay readable
    text = " ".join(str(text).split())
    if len(text) <= n:
        return text
    return text[:n - 3] + "..."


def main():
    args = parse_args()

    # create the output directory if it does not exist
    os.makedirs(args.output_dir, exist_ok=True)

    # start a local Spark session, with the Kafka connector when reading from Kafka
    builder = (
        SparkSession.builder.appName("ClassifyMessages")
        .master(args.master)
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if not args.input_jsonl:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # load the streamed messages and normalize repeated spaces and newlines
    raw = read_messages(spark, args).withColumn(
        "text", F.trim(F.regexp_replace(F.col("text"), "\\s+", " "))
    ).cache()
    n_read = raw.count()

    # remove empty messages
    non_empty = raw.where(F.col("text").isNotNull() & (F.length("text") > 0))
    n_non_empty = non_empty.count()

    # remove duplicate messages
    messages = non_empty.dropDuplicates(["text"]).cache()
    n_unique = messages.count()

    print(f"\nStreamed messages read:      {n_read}")
    print(f"Empty messages removed:      {n_read - n_non_empty}")
    print(f"Duplicate messages removed:  {n_non_empty - n_unique}")
    print(f"Messages remaining:          {n_unique}")

    # stop if there is nothing to classify, and warn if there are too few messages
    if n_unique == 0:
        raise SystemExit("No messages found. Is topic1 empty?")
    if n_unique < 1000:
        print(f"WARNING: only {n_unique} messages; the assignment asks for at least 1,000.")

    # load the saved model and apply it to every message
    model = PipelineModel.load(args.model_path)
    class_map = F.create_map(*[F.lit(x) for k, v in CLASS_NAMES.items() for x in (k, v)])
    probs = vector_to_array("probability")

    # keep the predicted class, its probability and the probability of every class
    scored = model.transform(messages).select(
        "msg_id",
        "timestamp",
        "text",
        class_map[F.col("prediction").cast("int")].alias("predicted_class"),
        F.round(F.array_max(probs), 4).alias("probability"),
        *[F.round(probs[i], 4).alias(f"prob_{name.replace('/', '')}")
          for i, name in CLASS_NAMES.items()]
    )

    # the results are small enough to handle in pandas from here on
    pdf = scored.toPandas()
    spark.stop()

    # sort by time and save the predictions for every message
    pdf = pdf.sort_values(["timestamp", "msg_id"], kind="stable", na_position="last")
    pdf = pdf.reset_index(drop=True)
    pdf.to_csv(os.path.join(args.output_dir, "predictions.csv"), index=False)

    # count the messages in each class, including classes with zero messages
    counts = (
        pdf.predicted_class.value_counts()
        .reindex(list(CLASS_NAMES.values()), fill_value=0)
        .rename_axis("predicted_class")
        .reset_index(name="count")
    )
    counts["percent"] = (100 * counts["count"] / counts["count"].sum()).round(1)
    counts.to_csv(os.path.join(args.output_dir, "class_counts.csv"), index=False)

    print("\nMessages per predicted class")
    print(counts.to_string(index=False))

    # pick random example messages for each class and show their probability
    example_lines = []
    for name in CLASS_NAMES.values():
        group = pdf[pdf.predicted_class == name]
        example_lines += [f"## {name} ({len(group)} messages)", ""]

        if group.empty:
            example_lines += ["_No messages were predicted as this class._", ""]
            continue

        examples = group.sample(n=min(args.examples_per_class, len(group)), random_state=args.seed)
        example_lines += ["| Probability | Message |", "|---:|---|"]
        for _, row in examples.sort_values("probability", ascending=False).iterrows():
            example_lines.append(f"| {row.probability:.3f} | {shorten(row.text).replace('|', '/')} |")
        example_lines.append("")

    # save the examples for the report
    with open(os.path.join(args.output_dir, "examples.md"), "w", encoding="utf-8") as file:
        file.write("# Example messages per predicted class\n\n" + "\n".join(example_lines))
    print("\n" + "\n".join(example_lines))

    # pick random messages for hand labeling, without the model's predictions so they do not bias anyone
    sheet_path = os.path.join(args.output_dir, "hand_labeling_sheet.csv")
    if os.path.exists(sheet_path) and not args.overwrite_sample:
        # never silently overwrite a sheet the group may already be labeling
        print(f"Kept existing {sheet_path} (use --overwrite-sample to regenerate)")
    else:
        sample = pdf.sample(n=min(args.sample_size, len(pdf)), random_state=args.seed)
        sheet = sample[["msg_id", "text"]].reset_index(drop=True)

        # number the rows and split them evenly between the labelers
        sheet.insert(0, "row", range(1, len(sheet) + 1))
        sheet.insert(1, "labeler", [f"Person {i * args.labelers // len(sheet) + 1}"
                                    for i in range(len(sheet))])
        sheet["hand_label"] = ""
        sheet["notes"] = ""

        # utf-8-sig so Excel shows special characters correctly
        sheet.to_csv(sheet_path, index=False, encoding="utf-8-sig")
        print(f"Wrote {len(sheet)} random messages to {sheet_path}; fill in hand_label")

    # find the time range of the collected messages
    stamps = pdf.timestamp.dropna().astype(str)
    if len(stamps):
        first, last = stamps.min(), stamps.max()
    else:
        first, last = "unknown", "unknown"

    # describe where the messages came from
    if args.input_jsonl:
        source = f"file `{args.input_jsonl}`"
    else:
        source = f"Kafka topic `{args.input_topic}`"

    # write all the numbers for the report into one markdown file
    lines = [
        "# Part 2 results (auto-generated by classify_messages.py)",
        "",
        "## Streamed data (2.1)",
        "",
        f"- Source: {source}",
        f"- Messages read: {n_read}",
        f"- Empty removed: {n_read - n_non_empty}",
        f"- Duplicates removed: {n_non_empty - n_unique}",
        f"- Messages remaining: {n_unique}",
        f"- Time range: {first} to {last}",
        "",
        "## Predicted classes (2.3a)",
        "",
        "| Class | Messages | Percent | Mean probability |",
        "|---|---:|---:|---:|"
    ]

    # add one table row per class with its average probability
    mean_prob = pdf.groupby("predicted_class").probability.mean()
    for _, row in counts.iterrows():
        if row.predicted_class in mean_prob:
            avg = f"{mean_prob[row.predicted_class]:.3f}"
        else:
            avg = "-"
        lines.append(f"| {row.predicted_class} | {row['count']} | {row.percent}% | {avg} |")

    # note how many predictions the model was unsure about
    low = int((pdf.probability < 0.5).sum())
    lines += [
        "",
        f"Mean probability of the predicted class: {pdf.probability.mean():.3f}. "
        f"{low} messages ({100 * low / len(pdf):.1f}%) had a top probability below 0.5.",
        "",
        "## Examples (five per class)",
        ""
    ] + example_lines

    with open(os.path.join(args.output_dir, "part2_results.md"), "w", encoding="utf-8") as file:
        file.write("\n".join(lines) + "\n")
    print(f"All outputs written to {args.output_dir}")


if __name__ == "__main__":
    main()
