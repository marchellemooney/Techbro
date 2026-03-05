"""
sync_to_notion.py
-----------------
Fetches the latest Marketing Insights brief from a Slack channel
(posted by Momentum) and creates a page in a Notion database.

Run manually or schedule weekly with cron:
    0 9 * * MON python sync_to_notion.py

Setup:
    pip install -r requirements.txt
    cp .env.example .env
    # Fill in your credentials in .env
"""

import os
import sys
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from notion_client import Client as NotionClient
from notion_client.errors import APIResponseError

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = os.environ["SLACK_CHANNEL_ID"]

NOTION_API_KEY = os.environ["NOTION_API_KEY"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]

# Notion property names — change these to match your database columns
PROP_TITLE = os.getenv("NOTION_PROP_TITLE", "Name")
PROP_DATE = os.getenv("NOTION_PROP_DATE", "Date")
PROP_CONTENT = os.getenv("NOTION_PROP_CONTENT", "Content")

# How far back to look for messages (default: 8 days to safely catch a weekly post)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "8"))

# Keyword used to identify Momentum briefs — adjust if needed
BRIEF_KEYWORD = os.getenv(
    "BRIEF_KEYWORD",
    "reasons our customers leave their current software",
)


# ---------------------------------------------------------------------------
# Slack helpers
# ---------------------------------------------------------------------------

def fetch_latest_brief(slack: WebClient) -> dict | None:
    """Return the most recent Slack message in the channel that looks like
    a Momentum marketing brief, posted within the lookback window."""
    oldest_ts = (
        datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    ).timestamp()

    try:
        response = slack.conversations_history(
            channel=SLACK_CHANNEL_ID,
            oldest=str(oldest_ts),
            limit=50,
        )
    except SlackApiError as e:
        print(f"[ERROR] Slack API error: {e.response['error']}")
        sys.exit(1)

    messages = response.get("messages", [])
    for msg in messages:
        text = msg.get("text", "")
        # Momentum briefs may also arrive as attachments or blocks
        if not text:
            for attachment in msg.get("attachments", []):
                text += attachment.get("text", "") + "\n"
            for block in msg.get("blocks", []):
                if block.get("type") == "section":
                    text += block.get("text", {}).get("text", "") + "\n"

        if BRIEF_KEYWORD.lower() in text.lower():
            return {"text": text.strip(), "ts": msg["ts"]}

    return None


# ---------------------------------------------------------------------------
# Notion helpers
# ---------------------------------------------------------------------------

def brief_already_synced(notion: NotionClient, slack_ts: str) -> bool:
    """Check whether a page with this Slack timestamp already exists."""
    try:
        results = notion.databases.query(
            database_id=NOTION_DATABASE_ID,
            filter={
                "property": PROP_TITLE,
                "title": {"contains": slack_ts},
            },
        )
        return len(results["results"]) > 0
    except APIResponseError:
        # If the property doesn't support this filter, skip dedup check
        return False


def build_notion_page(brief_text: str, slack_ts: str) -> dict:
    """Build the Notion page payload from the brief text."""
    posted_at = datetime.fromtimestamp(float(slack_ts), tz=timezone.utc)
    date_str = posted_at.strftime("%Y-%m-%d")
    title = f"Marketing Insights — {date_str}"

    # Split the brief into paragraph blocks (Notion has a 2000-char limit per block)
    paragraphs = [p.strip() for p in brief_text.split("\n") if p.strip()]
    children = []
    for para in paragraphs:
        # Chunk if a single paragraph exceeds Notion's limit
        for chunk in [para[i : i + 1999] for i in range(0, len(para), 1999)]:
            children.append(
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{"type": "text", "text": {"content": chunk}}]
                    },
                }
            )

    properties: dict = {
        PROP_TITLE: {
            "title": [{"type": "text", "text": {"content": title}}]
        },
    }

    # Add Date property only if it exists in the database
    properties[PROP_DATE] = {"date": {"start": date_str}}

    return {"properties": properties, "children": children}


def push_to_notion(notion: NotionClient, brief_text: str, slack_ts: str) -> str:
    """Create the Notion page and return its URL."""
    page_payload = build_notion_page(brief_text, slack_ts)

    try:
        page = notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            **page_payload,
        )
    except APIResponseError as e:
        # If the Date property doesn't exist, retry without it
        if "properties" in str(e) and PROP_DATE in str(e):
            print(f"[WARN] '{PROP_DATE}' property not found in Notion DB — skipping date.")
            page_payload["properties"].pop(PROP_DATE, None)
            page = notion.pages.create(
                parent={"database_id": NOTION_DATABASE_ID},
                **page_payload,
            )
        else:
            raise

    return page.get("url", "")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    slack = WebClient(token=SLACK_BOT_TOKEN)
    notion = NotionClient(auth=NOTION_API_KEY)

    print(f"[INFO] Looking for briefs in the last {LOOKBACK_DAYS} days...")
    brief = fetch_latest_brief(slack)

    if not brief:
        print("[INFO] No marketing brief found in the lookback window. Nothing to sync.")
        return

    print(f"[INFO] Found brief (Slack ts={brief['ts']}).")

    if brief_already_synced(notion, brief["ts"]):
        print("[INFO] This brief has already been synced to Notion. Skipping.")
        return

    url = push_to_notion(notion, brief["text"], brief["ts"])
    print(f"[OK]  Brief synced to Notion: {url}")


if __name__ == "__main__":
    main()
