# Part 2: Text Classification of Streamed Text

Uses the same environment as the main README (WSL, Java 17, the `.venv` with `pyspark==3.5.3 pandas pyarrow`) and Kafka with the Part 1 messages in `topic1`. No paths are hard-coded; every input and output location is a command-line argument. Replace the `<...>` placeholders with your own paths.

| Script | Assignment step |
|---|---|
| `train_model.py` | 2.1 (AG News class counts), 2.2 (build, fit and save the Pipeline) |
| `classify_messages.py` | 2.1 (load streamed text, remove empty and duplicate messages), 2.3a (batch classification), 2.3b (random sample for hand labels) |
| `compare_hand_labels.py` | 2.3b (hand labels vs. predictions) |
| `streaming_app.py --model-path` | 2.3c (live class counts, see main README) |

## 1. Download AG News

Get the training file from https://huggingface.co/datasets/fancyzhx/ag_news:

```bash
wget -O <AG_NEWS_TRAIN_PARQUET> https://huggingface.co/datasets/fancyzhx/ag_news/resolve/main/data/train-00000-of-00001.parquet
```

## 2. Train and save the model (2.1, 2.2)

```bash
python train_model.py --train-path <AG_NEWS_TRAIN_PARQUET> --model-path <MODEL_DIR> --output-dir <OUTPUT_DIR>
```

Prints the number of articles per class, then fits this Pipeline:

RegexTokenizer (lowercase, splits on non-word characters) → StopWordsRemover → HashingTF (2^18 features) → IDF (minDocFreq 2) → LogisticRegression (regParam 0.01, maxIter 50)

To report accuracy, 10% of the training file is held out first and scored (accuracy, weighted F1, per-class precision/recall, confusion matrix). The final model is then fit on all 120,000 articles and saved to `<MODEL_DIR>`. The numbers are saved in `<OUTPUT_DIR>/model_metrics.json`.

| Optional argument | Default |
|---|---|
| `--num-features` | 262144 |
| `--min-doc-freq` | 2 |
| `--reg-param` | 0.01 |
| `--max-iter` | 50 |
| `--holdout` | 0.1 |
| `--master` | `local[2]` |

Everything that follows uses this same saved model, including the live query:

```bash
python streaming_app.py --checkpoint-dir <CHECKPOINT_DIR> --model-path <MODEL_DIR>
```

## 3. Classify the streamed messages (2.1, 2.3a)

```bash
python classify_messages.py --model-path <MODEL_DIR> --output-dir <OUTPUT_DIR>
```

Reads every message in `topic1` with a batch Kafka read (`startingOffsets` = earliest), so it can run while the producer keeps going. It removes empty and duplicate messages, prints how many remain, and classifies them. Use `--input-jsonl <FILE>` instead to read the producer's saved file.

Files written to `<OUTPUT_DIR>`:

| File | Contents |
|---|---|
| `predictions.csv` | **Required output file:** every message with its predicted class, the probability of that class, and the probability of each of the four classes |
| `class_counts.csv` | Messages per predicted class |
| `examples.md` | Five random example messages per class with their predicted probability |
| `hand_labeling_sheet.csv` | 50 random messages split between 4 labelers, with an empty `hand_label` column and no predictions shown |
| `part2_results.md` | Message counts, the class table and the examples, ready for the report |

If `hand_labeling_sheet.csv` already exists, it is kept so hand labels aren't lost on a re-run. `--overwrite-sample` replaces it.

## 4. Label 50 messages by hand (2.3b)

Open `<OUTPUT_DIR>/hand_labeling_sheet.csv` (Excel or Google Sheets). The `labeler` column splits the 50 rows between the group members (`--labelers` sets how many). The model's predictions are left out of this sheet so they don't influence anyone's labels. Fill in `hand_label` with `World`, `Sports`, `Business`, `Sci/Tech` or `None of these`; `notes` is optional. Save it as CSV with the same name.

```bash
python compare_hand_labels.py --input <OUTPUT_DIR>/hand_labeling_sheet.csv --predictions <OUTPUT_DIR>/predictions.csv --output-dir <OUTPUT_DIR>
```

This adds the model's prediction and probability next to each hand label, then reports how many messages fit none of the classes and the **accuracy on the messages that fit one of the four**. Unrecognized labels are listed by row number, and rows still blank are reported per labeler. It writes:

- `hand_labels_vs_predictions.csv`: **required file:** the 50 hand labels next to the model's predictions
- `hand_label_comparison.md`: the numbers, a confusion matrix, and the list of messages the model got wrong

## 5. Part 2 summary (for the report)

The assignment asks for a brief summary. Use the numbers from `model_metrics.json`, `part2_results.md` and `hand_label_comparison.md`, and cover:

1. **Do the predicted classes make sense for our data source?** Give the class breakdown, say whether it matches what the source actually talks about, and note how many of the 50 messages fit none of the four classes.
2. **What the model handles well:** for example, messages that read like news, with clear topic words (team names, company or market terms, countries and politicians, tech products).
3. **What it handles badly, and why:** AG News is formal news text, but streamed messages are often short, informal, or off-topic. A TF-IDF model has to put every message in one of four classes, so off-topic messages get forced into one of them (check which class absorbs them). Short messages have very few known words, so their probabilities tend to be low.
4. **A few examples it got wrong:** copy 3–4 rows from the "got wrong" table in `hand_label_comparison.md`, and say why each one was probably misclassified.
