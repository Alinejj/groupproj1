import requests
import time
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# base URL for the Hacker News Firebase API
API_BASE = "https://hacker-news.firebaseio.com/v0"


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

    # return the cleaned message
    return {
        "id": item["id"],
        "text": text,
        "timestamp": timestamp,
        "source": "hackernews"
    }


def main():
    # create a reusable HTTP session
    with requests.Session() as session:

        # set the starting point to the current latest item ID
        last_id = get_json(session, f"{API_BASE}/maxitem.json")

        print(f"starting from Hacker News item ID: {last_id}")

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
                        print(message)

                    # update the last processed item ID
                    last_id = item_id

                # wait before polling again
                time.sleep(5)
        except KeyboardInterrupt:
            # gracefully exit when the user presses Ctrl+C
            print("\nproducer stopped by user")

if __name__ == "__main__":
    main()