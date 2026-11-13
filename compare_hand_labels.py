import argparse
import os
import sys

import pandas as pd

# the four AG News classes the model can predict
CLASSES = ["World", "Sports", "Business", "Sci/Tech"]

# hand label for messages that fit none of the four classes
NONE = "None of these"

# spellings we accept in the hand_label column (compared in lowercase)
ALIASES = {
    "world": "World",
    "sports": "Sports",
    "sport": "Sports",
    "business": "Business",
    "sci/tech": "Sci/Tech",
    "scitech": "Sci/Tech",
    "sci-tech": "Sci/Tech",
    "sci tech": "Sci/Tech",
    "tech": "Sci/Tech",
    "science": "Sci/Tech",
    "none of these": NONE,
    "none": NONE,
    "other": NONE,
    "n/a": NONE
}


def parse_args():
    # create the command-line argument parser
    parser = argparse.ArgumentParser(
        description="Compare hand labels with the model's predictions"
    )

    # specify the filled-in hand labeling sheet
    parser.add_argument(
        "--input",
        required=True
    )

    # specify predictions.csv from classify_messages.py
    parser.add_argument(
        "--predictions",
        required=True
    )

    # specify the folder for the comparison files
    parser.add_argument(
        "--output-dir",
        required=True
    )

    return parser.parse_args()


def read_csv(path):
    # Excel may save the sheet as UTF-8 (with or without a BOM) or as Windows-1252
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="cp1252")


def normalize(label):
    # return None for rows that have not been labeled yet
    label = str(label).strip()
    if not label:
        return None

    # map any accepted spelling to the standard class name, or "?" if it is unknown
    return ALIASES.get(label.lower(), "?")


def shorten(text, n=160):
    # collapse whitespace and cut long messages so tables stay readable
    text = " ".join(str(text).split())
    if len(text) <= n:
        return text
    return text[:n - 3] + "..."


def main():
    args = parse_args()

    # load the hand labels and the model's predictions
    sheet = read_csv(args.input)
    preds = read_csv(args.predictions)[["msg_id", "text", "predicted_class", "probability"]]

    # make sure the sheet has the columns we need
    for column in ("msg_id", "hand_label"):
        if column not in sheet.columns:
            sys.exit(f"{args.input} has no {column} column")

    # make sure every message in the sheet exists in predictions.csv
    missing = sorted(set(sheet.msg_id) - set(preds.msg_id))
    if missing:
        sys.exit(f"These msg_ids are not in {args.predictions}: {missing}")

    # add the model's prediction next to each hand label (text comes from predictions.csv)
    df = sheet.drop(columns=["text", "predicted_class", "probability"], errors="ignore")
    df = df.merge(preds, on="msg_id", how="left")

    # convert the hand labels to standard class names
    df["hand"] = df["hand_label"].map(normalize)

    # list every row with a label we do not recognize, so they can all be fixed at once
    unknown = df[df.hand == "?"]
    if not unknown.empty:
        print("These hand labels are not recognized. Use World, Sports, Business, Sci/Tech or None of these:")
        for i, row in unknown.iterrows():
            print(f"  row {row.get('row', i + 1)}: {row.hand_label!r}")
        sys.exit(1)

    # keep only the labeled rows and warn about any that are still blank
    labeled = df[df.hand.notna()].copy()
    if labeled.empty:
        sys.exit(f"No hand labels found in {args.input}. Fill in the hand_label column first.")

    blank = df[df.hand.isna()]
    if not blank.empty:
        if "labeler" in blank.columns:
            who = blank.labeler.value_counts().to_dict()
        else:
            who = {}
        print(f"WARNING: {len(blank)} rows have no hand label yet {who}")

    # mark whether the model agreed with the hand label
    labeled["correct"] = labeled.hand == labeled.predicted_class
    labeled["fits_a_class"] = labeled.hand != NONE

    # split into messages that fit one of the four classes and ones that fit none
    n = len(labeled)
    fits = labeled[labeled.fits_a_class]
    n_none = n - len(fits)

    # accuracy only counts the messages that fit one of the four classes
    if len(fits):
        accuracy = fits.correct.mean()
    else:
        accuracy = float("nan")

    # count hand label vs. prediction for the confusion matrix
    rows = CLASSES + [NONE]
    matrix = pd.crosstab(
        pd.Categorical(labeled.hand, categories=rows),
        pd.Categorical(labeled.predicted_class, categories=CLASSES),
        dropna=False
    )

    # summary numbers for the report
    lines = [
        "# Hand labels vs. model predictions (2.3b)",
        "",
        f"- Messages hand-labeled: {n}",
        f"- Fit none of the four classes (None of these): {n_none} ({100 * n_none / n:.1f}%)",
        f"- Fit one of the four classes: {len(fits)}",
        f"- **Accuracy on messages that fit one of the four classes: "
        f"{int(fits.correct.sum())}/{len(fits)} = {100 * accuracy:.1f}%**",
        "",
        "## Confusion matrix (rows = hand label, columns = model prediction)",
        "",
        "| Hand \\ Predicted | " + " | ".join(CLASSES) + " |",
        "|---|" + "---:|" * len(CLASSES)
    ]
    for r in rows:
        lines.append(f"| {r} | " + " | ".join(str(matrix.loc[r, c]) for c in CLASSES) + " |")

    # how often the model was right for each hand-labeled class
    lines += [
        "",
        "## Accuracy by hand label",
        "",
        "| Hand label | Messages | Model correct |",
        "|---|---:|---:|"
    ]
    for r in CLASSES:
        group = labeled[labeled.hand == r]
        if len(group):
            lines.append(f"| {r} | {len(group)} | {int(group.correct.sum())} "
                         f"({100 * group.correct.mean():.0f}%) |")

    # which classes the model forced the none-of-these messages into
    lines += ["", "## Where the None-of-these messages went", ""]
    if n_none:
        spread = labeled[~labeled.fits_a_class].predicted_class.value_counts()
        for name, count in spread.items():
            lines.append(f"- {name}: {count}")
    else:
        lines.append("_No messages were labeled None of these._")

    # list the messages the model got wrong, for the summary
    lines += [
        "",
        "## Messages the model got wrong (fit a class but predicted another)",
        "",
        "| Message | Predicted (probability) | Hand label | Notes |",
        "|---|---|---|---|"
    ]
    for _, row in fits[~fits.correct].iterrows():
        text = shorten(row.text).replace("|", "/")
        notes = str(row.get("notes", "")).replace("|", "/")
        lines.append(f"| {text} | {row.predicted_class} ({row.probability}) | {row.hand} | {notes} |")

    # create the output directory if it does not exist
    os.makedirs(args.output_dir, exist_ok=True)

    # save the comparison for the report
    out_md = os.path.join(args.output_dir, "hand_label_comparison.md")
    with open(out_md, "w", encoding="utf-8") as file:
        file.write("\n".join(lines) + "\n")

    # save the hand labels next to the model's predictions (required submission file)
    out_csv = os.path.join(args.output_dir, "hand_labels_vs_predictions.csv")
    labeled["hand_label"] = labeled.hand
    columns = ["msg_id", "text", "hand_label", "predicted_class", "probability",
               "fits_a_class", "correct"]
    for extra in ("labeler", "notes"):
        if extra in labeled.columns:
            columns.append(extra)
    labeled[columns].to_csv(out_csv, index=False, encoding="utf-8-sig")

    print("\n".join(lines[:7]))
    print(f"Wrote {out_md} and {out_csv}")


if __name__ == "__main__":
    main()
