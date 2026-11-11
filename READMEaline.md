# CS4371 Group Project 1: Spark Streaming with Kafka and Text Classification

## Requirements

- Linux or WSL (Ubuntu 22.04 tested)
- Docker
- Java 17
- Python 3.10+

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark==3.5.3 spacy pandas pyarrow
python -m spacy download en_core_web_sm
```

## Start Kafka

```bash
docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic2
```

Use `docker compose stop` and `docker compose start` to pause and resume Kafka. Do not use `docker compose down`, and do not edit `docker-compose.yml` once real data is being collected: recreating the container deletes all messages in the topics.

## Message formats

`topic1` (input text): `{"text": "...", "timestamp": "..."}`

`topic2` (entity counts): `{"entity": "...", "count": N, "updated_at": "..."}`. Each message is the latest running total for that entity.


## Producer (`producer.py`)

Continuously fetches new stories and comments from the Hacker News API, removes HTML tags and extra whitespace, and publishes the cleaned text as JSON messages to Kafka (`topic1`). Successfully published messages are also saved locally in JSONL format for batch classification in Part 2.

The producer tracks the last processed Hacker News item ID in a state file, allowing collection to resume after restarting the application. No API key is required.

### Setup

Activate the Python virtual environment and install the producer dependencies:

```bash
pip install requests beautifulsoup4 confluent-kafka
```

Make sure Kafka is running and `topic1` has been created using the instructions under **Start Kafka**.

### Run the producer

```bash
python producer.py
```

By default, the producer connects to `localhost:9092`, publishes to `topic1`, and polls the Hacker News API every 5 seconds.

To specify custom options:

```bash
python producer.py --bootstrap-servers localhost:9092 --topic topic1 --interval 5 --state-file producer_state.json --output data/messages.jsonl
```

| Argument | Required | Default | Description |
|---|---|---|---|
| `--bootstrap-servers` | No | `localhost:9092` | Kafka broker address |
| `--topic` | No | `topic1` | Destination Kafka topic |
| `--interval` | No | `5` | Seconds between API polling cycles |
| `--state-file` | No | `producer_state.json` | File storing the last processed Hacker News item ID |
| `--output` | No | `data/messages.jsonl` | File used to save collected messages |

Use `Ctrl+C` to stop the producer. On restart, it resumes from the last saved item ID.

### Message format

Each message published to `topic1` and saved to the JSONL dataset follows this format:

```json
{
  "id": 50448813,
  "text": "Claude Code is accidentally exposing secrets all over GitHub",
  "timestamp": "2026-11-11T01:40:18+00:00",
  "source": "hackernews"
}
```

- `id`: Unique Hacker News item ID used for tracking processed messages
- `text`: Cleaned story title, body, or comment text
- `timestamp`: Original Hacker News publication time in ISO 8601 format
- `source`: Identifies the data source as Hacker News

### Verify Kafka output

To view messages published to `topic1`:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --from-beginning
```

Remove `--from-beginning` to consume only newly arriving messages.

### Saved dataset

The producer appends each successfully published message to `data/messages.jsonl`, with one JSON object per line.

To count the collected messages:

```bash
wc -l data/messages.jsonl
```

To view the first five messages:

```bash
head -n 5 data/messages.jsonl
```

The dataset can be loaded directly into Spark:

```python
df = spark.read.json("data/messages.jsonl")
```

At least 1,000 streamed messages are required for Part 2. Empty and duplicate messages should be removed before classification.

### Troubleshooting

- **Kafka connection failure:** Make sure Docker is running, the Kafka container is started, and the configured broker address is correct
- **No new messages:** The producer only processes Hacker News item IDs newer than its saved position
- **Missing dataset:** The JSONL output file is created after the first successfully published message
- **Restarting the producer:** The state file allows collection to resume from the last processed item ID
- **API/network errors:** The current implementation may terminate if a request fails; automatic retry handling has not yet been implemented


## Streaming app (`streaming_app.py`)

Reads messages from `topic1`, extracts named entities with spaCy (people, organizations, places, nationalities, products and events), keeps a running count per entity, and writes updated counts to `topic2` at every trigger. When a saved PipelineModel is passed with `--model-path`, it also classifies each incoming message into World, Sports, Business or Sci/Tech and prints running counts per class to the console (Part 2, 2.3c).

```bash
python streaming_app.py --checkpoint-dir <CHECKPOINT_DIR> --model-path <MODEL_DIR>
```

| Argument | Required | Default | Description |
|---|---|---|---|
| `--checkpoint-dir` | Yes | | Folder where Spark stores streaming progress |
| `--model-path` | No | | Saved PipelineModel; enables live classification |
| `--bootstrap-servers` | No | `localhost:9092` | Kafka broker address |
| `--input-topic` | No | `topic1` | Topic with incoming messages |
| `--output-topic` | No | `topic2` | Topic for entity counts |
| `--trigger` | No | `30 seconds` | How often counts are emitted |
| `--starting-offsets` | No | `earliest` | `earliest` or `latest` |
| `--master` | No | `local[2]` | Spark master; use `local[1]` on machines with 8 GB RAM |
| `--show-console` | No | off | Also print the top 10 entities to the console |

To check the output, read `topic2` directly:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic2 --from-beginning
```

If the code or the Kafka topics change, delete the checkpoint folder before restarting the app.

## Placeholder model (`train_placeholder_model.py`)

Trains a quick model on a sample of AG News so the live classification query can be tested before the final Part 2 model is ready. It uses the same pipeline stages as the final model, so swapping models only means changing `--model-path`.

```bash
python train_placeholder_model.py --train-path <AG_NEWS_TRAIN_PARQUET> --model-path <MODEL_DIR>
```

The AG News training file is available at https://huggingface.co/datasets/fancyzhx/ag_news (`data/train-00000-of-00001.parquet`).

## Troubleshooting

- **Laptop freezes or WSL disconnects:** Spark, spaCy and Kafka together need several GB of memory. Close other Docker containers and heavy apps, and run with `--master "local[1]"`.
- **"Cannot fetch offset" error:** the Kafka topic was reset. Delete the checkpoint folder and restart the app.
- **"Current batch is falling behind" warning:** harmless. The trigger is shorter than the time a batch takes; use a longer `--trigger`.