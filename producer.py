import json
import requests
import time
from datetime import datetime, timezone
from pathlib import Path
from bs4 import BeautifulSoup
from confluent_kafka import Producer

# base URL for the Hacker News Firebase API
API_BASE = "https://hacker-news.firebaseio.com/v0"

# file used to save the last processed Hacker News item ID
STATE_FILE = Path("producer_state.json")

# Kafka broker address and destination topic
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "topic1"

def get_json(session, url):
    # send a GET request and return the JSON response
    response = session.get(url, timeout=15)

    # raise an exception if the request failed
    response.raise_for_status()

    # convert the JSON response into a Python object
    return response.json()


def clean_text(text):
    # return an empty string if there is no text
    if not text:
        return ""

    # remove HTML tags and strip extra whitespace
    text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)

    # normalize repeated spaces and newlines into single spaces
    return " ".join(text.split())


def extract_message(item):
    # skip missing, deleted, or dead items
    if not item or item.get("deleted") or item.get("dead"):
        return None

    # determine the Hacker News item type
    item_type = item.get("type")

    # for stories, combine the title and optional body text
    if item_type == "story":
        text = " ".join([
            item.get("title", ""),
            item.get("text", "")
        ])

    # for comments, use the comment text
    elif item_type == "comment":
        text = item.get("text", "")

    # ignore all other item types
    else:
        return None

    # clean the extracted text
    text = clean_text(text)

    # skip items that still have no usable text after cleaning
    if not text:
        return None

    # convert the Hacker News Unix timestamp into ISO format
    timestamp = datetime.fromtimestamp(
        item["time"], tz=timezone.utc
    ).isoformat()

    # return the cleaned message in a consistent format
    return {
        "id": item["id"],
        "text": text,
        "timestamp": timestamp,
        "source": "hackernews"
    }


def load_last_id():
    if not STATE_FILE.exists():
        return None

    with STATE_FILE.open("r", encoding="utf-8") as file:
        state = json.load(file)

    # return the saved last processed item ID
    return state.get("last_id")


def save_last_id(last_id):
    # create a temporary state file
    temp_file = STATE_FILE.with_suffix(".tmp")

    # write the latest processed ID to the temporary file
    with temp_file.open("w", encoding="utf-8") as file:
        json.dump({"last_id": last_id}, file)

    # replace the previous state file with the new one
    temp_file.replace(STATE_FILE)


def publish_message(producer, message):
    # track whether Kafka successfully received the message
    delivery = {"confirmed": False, "error": None}

    # callback executed when Kafka reports the delivery result
    def delivery_callback(error, record):
        delivery["error"] = error
        delivery["confirmed"] = error is None

    # convert the Python dictionary into JSON bytes
    message_json = json.dumps(message).encode("utf-8")

    # send the message to topic1 using its item ID as the key
    producer.produce(
        topic=KAFKA_TOPIC,
        key=str(message["id"]).encode("utf-8"),
        value=message_json,
        callback=delivery_callback
    )

    # wait for Kafka to finish delivering the message
    remaining = producer.flush(15)

    # raise an exception if delivery failed or timed out
    if remaining != 0 or not delivery["confirmed"]:
        raise RuntimeError(
            f"failed to publish item {message['id']}: {delivery['error']}"
        )



def main():
    # initialize the Kafka producer
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True
    })

    # create a reusable HTTP session
    with requests.Session() as session:

        # try to restore the previously saved item ID
        last_id = load_last_id()

        # if no saved state exists, start from the current latest item ID
        if last_id is None:
            last_id = get_json(session, f"{API_BASE}/maxitem.json")
            save_last_id(last_id)
            print(f"no saved state found so starting from Hacker News item ID: {last_id}")
        else:
            print(f"resuming from saved Hacker News item ID: {last_id}")

        try:
            # continuously poll for newly created items
            while True:
                # get the newest available item ID
                latest_id = get_json(session, f"{API_BASE}/maxitem.json")

                # process every new item ID since the previous poll
                for item_id in range(last_id + 1, latest_id + 1):
                    # fetch the item data for this ID
                    item = get_json(
                        session,
                        f"{API_BASE}/item/{item_id}.json"
                    )

                    # extract a cleaned message if the item is usable
                    message = extract_message(item)

                    if message:
                        # publish the message to Kafka topic1
                        publish_message(producer, message)

                        # print confirmation after successful delivery
                        print(f"published item {item_id} to {KAFKA_TOPIC}")

                    # update progress only after processing succeeds
                    last_id = item_id
                    save_last_id(last_id)

                # wait before polling again
                time.sleep(5)

        except KeyboardInterrupt:
            # gracefully exit when the user presses Ctrl+C
            print("\nproducer stopped by user")

        finally:
            # finish handling any pending Kafka deliveries
            producer.flush(15)



if __name__ == "__main__":
    main()