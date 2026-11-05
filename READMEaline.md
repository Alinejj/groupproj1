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