# CS4371 Group Project 1: Spark Streaming with Kafka and Text Classification

Team [number]: [names]

This project collects live text from Hacker News, counts the named entities being mentioned in real time, visualizes the top 10 in Kibana, and classifies the same messages with a Spark MLlib model trained on AG News.

**Pipeline:** Hacker News API → `producer.py` → Kafka `topic1` → `streaming_app.py` (spaCy NER, running counts) → Kafka `topic2` → Logstash → Elasticsearch → Kibana

**Part 2:** `train_model.py` trains and saves the classifier on AG News. `classify_messages.py` classifies every message collected in `topic1`, and `streaming_app.py --model-path` classifies messages live as they arrive.

No paths are hard-coded: every input and output location is a command-line argument. No API keys are needed (the Hacker News API is free and keyless).

## Repository layout

| File | Purpose |
|---|---|
| `docker-compose.yml` | Kafka (KRaft mode, single broker) |
| `producer.py` | Hacker News producer: writes to `topic1` and saves messages to JSONL |
| `streaming_app.py` | Part 1 named entity counts to `topic2`; Part 2 (2.3c) live classification |
| `elastic/logstash.conf` | Logstash pipeline: `topic2` → Elasticsearch |
| `elastic/export.ndjson` | Exported Kibana dashboard and top-10 visualization |
| `train_model.py` | Part 2 (2.1, 2.2): AG News class counts, train and save the model |
| `classify_messages.py` | Part 2 (2.1, 2.3a, 2.3b): clean and batch-classify streamed messages, draw the hand-label sample |
| `compare_hand_labels.py` | Part 2 (2.3b): compare hand labels with the model's predictions |
| `train_placeholder_model.py` | Optional quick model for testing the live query |
| `models/ag_news_model/` | The saved model used for all Part 2 results |
| `output/` | Part 2 results (see [Output files](#output-files)) |

## Requirements

- Linux or WSL (Ubuntu 22.04 tested). On Windows, run every command below in a WSL terminal.
- Docker (Docker Desktop on Windows, with WSL integration turned on for your Ubuntu distro)
- Java 17
- Python 3.10+
- About 8 GB of free RAM to run the whole pipeline at once (Spark, Kafka and the Elastic stack). Individual parts run with less.

## 1. Setup

```bash
sudo apt install -y openjdk-17-jdk-headless python3-venv python3-pip
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark==3.5.3 spacy pandas pyarrow requests beautifulsoup4 confluent-kafka
python -m spacy download en_core_web_sm
```

The Spark Kafka connector (`spark-sql-kafka-0-10_2.12:3.5.3`) is downloaded automatically the first time a Spark script runs.

## 2. Start Kafka

```bash
docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic topic2
```

Kafka listens on `localhost:9092` for the Python scripts and on `kafka:29092` for containers on the same Docker network (Logstash).

Use `docker compose stop` and `docker compose start` to pause and resume Kafka. Do not use `docker compose down`, and do not edit `docker-compose.yml` once real data is being collected: recreating the container deletes all messages in the topics.

## 3. Start Elasticsearch, Kibana and Logstash

These containers join the network that `docker compose up` created. Its name is `<folder name>_default`; with the repository cloned as `groupproj1` it is `groupproj1_default` (check with `docker network ls`).

```bash
docker run -d --name elasticsearch --network groupproj1_default -p 9200:9200 \
  -e "discovery.type=single-node" -e "xpack.security.enabled=false" -e "ES_JAVA_OPTS=-Xms512m -Xmx512m" \
  docker.elastic.co/elasticsearch/elasticsearch:8.15.0

docker run -d --name kibana --network groupproj1_default -p 5601:5601 \
  -e "ELASTICSEARCH_HOSTS=http://elasticsearch:9200" \
  docker.elastic.co/kibana/kibana:8.15.0

docker run -d --name logstash --network groupproj1_default \
  -v "$(pwd)/elastic/logstash.conf:/usr/share/logstash/pipeline/logstash.conf" \
  -e "LS_JAVA_OPTS=-Xms512m -Xmx512m" \
  docker.elastic.co/logstash/logstash:8.15.0
```

Logstash reads `topic2` from `kafka:29092` and writes to the Elasticsearch index `entity-counts`, using the entity name as the document ID. Each entity therefore has one document holding its latest running count.

To stop them later: `docker stop logstash kibana elasticsearch` (and `docker start ...` to resume).

## 4. Run the producer

```bash
python producer.py --bootstrap-servers localhost:9092 --topic topic1 --interval 5 --state-file <STATE_FILE> --output <MESSAGES_JSONL>
```

For example: `--state-file data/producer_state.json --output data/messages.jsonl`.

The producer polls the Hacker News API for new item IDs every `--interval` seconds and fetches each new story and comment. It skips deleted and dead items, strips HTML tags and extra whitespace, and publishes each message to `topic1`, waiting for Kafka to confirm delivery. Every published message is also appended to the JSONL file, and the last processed item ID is saved to the state file so collection resumes after a restart. Press `Ctrl+C` to stop.

| Argument | Default | Description |
|---|---|---|
| `--bootstrap-servers` | `localhost:9092` | Kafka broker address |
| `--topic` | `topic1` | Destination topic |
| `--interval` | `5` | Seconds between API polls |
| `--state-file` | `producer_state.json` | Last processed Hacker News item ID |
| `--output` | `data/messages.jsonl` | Saved copy of every published message |

Message format in `topic1` and the JSONL file:

```json
{"id": 50448813, "text": "Claude Code is accidentally exposing secrets all over GitHub", "timestamp": "2026-11-11T01:40:18+00:00", "source": "hackernews"}
```

To watch messages arrive:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1
```

## 5. Run the streaming application (Part 1, and Part 2 step 2.3c)

```bash
python streaming_app.py --checkpoint-dir <CHECKPOINT_DIR> --model-path <MODEL_DIR>
```

For example: `--checkpoint-dir ~/gp1_checkpoints --model-path models/ag_news_model`.

The app reads `topic1`, extracts named entities with spaCy's `en_core_web_sm` (people, organizations, countries and cities, locations, nationalities and groups, products and events), and keeps a running count per entity. At every trigger it writes the entities whose counts changed to `topic2` as:

```json
{"entity": "Nvidia", "count": 42, "updated_at": "2026-11-12T23:10:30"}
```

With `--model-path`, the same app also applies the saved Part 2 model to each incoming message and prints a running count per predicted class (World, Sports, Business, Sci/Tech) to the console.

| Argument | Required | Default | Description |
|---|---|---|---|
| `--checkpoint-dir` | Yes | | Folder where Spark stores streaming progress |
| `--model-path` | No | | Saved PipelineModel; enables live classification |
| `--bootstrap-servers` | No | `localhost:9092` | Kafka broker address |
| `--input-topic` | No | `topic1` | Topic with incoming messages |
| `--output-topic` | No | `topic2` | Topic for entity counts |
| `--trigger` | No | `30 seconds` | How often counts are emitted |
| `--starting-offsets` | No | `earliest` | `earliest` or `latest` |
| `--master` | No | `local[2]` | Spark master; use `local[1]` on 8 GB machines |
| `--show-console` | No | off | Also print the top 10 entities to the console |

To check the entity counts directly:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic2 --from-beginning
```

If the code or the Kafka topics change, delete the checkpoint folder before restarting the app.

## 6. View the top 10 entities in Kibana

Check that counts are reaching Elasticsearch:

```bash
curl -s "http://localhost:9200/entity-counts/_count"
```

Open `http://localhost:5601`. To load the saved dashboard, go to **Stack Management → Saved Objects → Import**, select `elastic/export.ndjson`, and open the dashboard **entity countd**. The **Top 10 Entities** bar chart uses the `entity-counts` index, with the top 10 values of `entity.keyword` on the x-axis and the maximum of `count` on the y-axis. Set the time range to cover the run and click **Refresh**.

## 7. Part 2: Text classification

### 7.1 Download AG News

```bash
wget -O <AG_NEWS_TRAIN_PARQUET> https://huggingface.co/datasets/fancyzhx/ag_news/resolve/main/data/train-00000-of-00001.parquet
```

### 7.2 Train and save the model (2.1, 2.2)

```bash
python train_model.py --train-path <AG_NEWS_TRAIN_PARQUET> --model-path <MODEL_DIR> --output-dir <OUTPUT_DIR>
```

This prints the number of articles per class, then fits the pipeline RegexTokenizer (lowercase, splits on non-word characters) → StopWordsRemover → HashingTF (2^18 features) → IDF (minDocFreq 2) → LogisticRegression (regParam 0.01, maxIter 50). It first holds out 10% of the data to measure accuracy, weighted F1, per-class precision and recall and a confusion matrix, then refits on all 120,000 articles and saves the model to `<MODEL_DIR>`. The metrics are written to `<OUTPUT_DIR>/model_metrics.json`.

| Optional argument | Default |
|---|---|
| `--num-features` | 262144 |
| `--min-doc-freq` | 2 |
| `--reg-param` | 0.01 |
| `--max-iter` | 50 |
| `--holdout` | 0.1 |
| `--master` | `local[2]` |

The same saved model is used for every step that follows, including the live query in step 5.

### 7.3 Classify the streamed messages (2.1, 2.3a)

```bash
python classify_messages.py --model-path <MODEL_DIR> --output-dir <OUTPUT_DIR>
```

This reads every message in `topic1` with a batch Kafka read, removes empty and duplicate messages, prints how many remain, and classifies them. To read the producer's saved file instead of Kafka, add `--input-jsonl <MESSAGES_JSONL>`.

If `hand_labeling_sheet.csv` already exists in `<OUTPUT_DIR>`, it is kept so hand labels aren't lost; `--overwrite-sample` replaces it. Do not re-run this step after hand labeling has started: the sheet refers to messages by their Kafka position, so a new run on different data would no longer match it.

### 7.4 Compare hand labels with the model (2.3b)

Open `<OUTPUT_DIR>/hand_labeling_sheet.csv`. The `labeler` column splits the 50 random messages between the team members, and the model's predictions are left out so they don't influence the labels. Fill in `hand_label` with `World`, `Sports`, `Business`, `Sci/Tech` or `None of these` (`notes` is optional), and save it as CSV with the same name. Then run:

```bash
python compare_hand_labels.py --input <OUTPUT_DIR>/hand_labeling_sheet.csv --predictions <OUTPUT_DIR>/predictions.csv --output-dir <OUTPUT_DIR>
```

This adds the model's prediction and probability next to each hand label, and reports how many messages fit none of the four classes and the accuracy on the messages that do. Unrecognized labels are listed by row, and blank rows are reported per labeler.

## Output files

| File | Contents |
|---|---|
| `output/model_metrics.json` | AG News class counts, settings, holdout accuracy, F1 and confusion matrix |
| `output/predictions.csv` | Every streamed message with its predicted class, its probability, and the probability of each class |
| `output/class_counts.csv` | Messages per predicted class |
| `output/examples.md` | Five example messages per class with probabilities |
| `output/part2_results.md` | Message counts, class table and examples for the report |
| `output/hand_labeling_sheet.csv` | The 50 random messages with the team's hand labels |
| `output/hand_labels_vs_predictions.csv` | The 50 hand labels next to the model's predictions |
| `output/hand_label_comparison.md` | Accuracy, "none of these" count, confusion matrix and the messages the model got wrong |

## Optional: placeholder model

`train_placeholder_model.py` trains a quick model on a 10% sample of AG News, with the same pipeline stages, so the live query in step 5 can be tested before the final model exists:

```bash
python train_placeholder_model.py --train-path <AG_NEWS_TRAIN_PARQUET> --model-path <PLACEHOLDER_MODEL_DIR>
```

## Shutting down

```bash
docker stop logstash kibana elasticsearch
docker compose stop
```

## Troubleshooting

- **Laptop freezes or WSL disconnects:** the full pipeline needs several GB of RAM. Stop other Docker containers (`docker ps`), close heavy apps, and run Spark with `--master "local[1]"`.
- **pip fails with "Network is unreachable" in WSL:** set `networkingMode=mirrored` under `[wsl2]` in `%USERPROFILE%\.wslconfig`, then run `wsl --shutdown` in PowerShell.
- **"Cannot fetch offset" error in the streaming app:** the Kafka topic was reset. Delete the checkpoint folder and restart the app.
- **"Current batch is falling behind" warning:** harmless; the trigger is shorter than a batch takes. Use a longer `--trigger`.
- **Logstash can't reach Kafka:** check that it is on the same Docker network as the `kafka` container (`docker network ls`), and that the config uses `kafka:29092`, not `localhost:9092`.
- **Producer stops on a network error:** the producer does not retry failed API requests. Restart it; it resumes from the saved item ID.
- **No new producer messages:** it only processes Hacker News items newer than the ID in its state file.