"""
sync_to_notion.py
-----------------
Fetches the latest Marketing Insights brief posted by Momentum to Slack
and creates a row in the Notion database.

Run manually or schedule weekly with cron:
    0 9 * * MON python sync_to_notion.py
"""

import os
import sys
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from notion_client import Client as NotionClient

load_dotenv()

# ---------------------------------------------------------------------------
# Credentials (loaded from .env file)
# ---------------------------------------------------------------------------

SLACK_BOT_TOKEN   = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID  = os.environ["SLACK_CHANNEL_ID"]
NOTION_API_KEY    = os.environ["NOTION_API_KEY"]
NOTION_DATABASE_ID = "7c56d30fe9be4803bd00a91cd5c40ca0"

# How far back to look for Momentum briefs (8 days catches any weekly post)
LOOKBACK_DAYS = 8


# ---------------------------------------------------------------------------
# Step 1 — Pull the brief from Slack
# ---------------------------------------------------------------------------

def fetch_latest_brief(slack: WebClient) -> dict | None:
    """Return the most recent Momentum brief from the Slack channel."""
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
        print(f"[ERROR] Could not read Slack channel: {e.response['error']}")
        sys.exit(1)

    for msg in response.get("messages", []):
        # Collect all text from the message, including attachments and blocks
        text = msg.get("text", "")
        for attachment in msg.get("attachments", []):
            text += "\n" + attachment.get("text", "")
        for block in msg.get("blocks", []):
            if block.get("type") == "section":
                text += "\n" + block.get("text", {}).get("text", "")
        text = text.strip()

        # Identify Momentum briefs by their recurring title phrase
        if "leave their current software" in text.lower():
            return {"text": text, "ts": msg["ts"]}

    return None


# ---------------------------------------------------------------------------
# Step 2 — Check for duplicates
# ---------------------------------------------------------------------------

def already_synced(notion: NotionClient, slack_ts: str) -> bool:
    """Return True if this brief was already added to Notion."""
    results = notion.databases.query(
        database_id=NOTION_DATABASE_ID,
        filter={
            "property": "Insight Title",
            "title": {"contains": slack_ts},
        },
    )
    return len(results["results"]) > 0


# ---------------------------------------------------------------------------
# Step 3 — Push to Notion
# ---------------------------------------------------------------------------

def push_to_notion(notion: NotionClient, brief_text: str, slack_ts: str) -> str:
    """Create a new page in the Notion database and return its URL."""
    posted_at = datetime.fromtimestamp(float(slack_ts), tz=timezone.utc)
    date_str  = posted_at.strftime("%Y-%m-%d")

    # Use the Slack timestamp in the title so we can detect duplicates later
    title = f"Marketing Insights Brief — {date_str} [{slack_ts}]"

    # Break the brief text into paragraph blocks for the page body
    # (Notion has a 2,000 character limit per block)
    children = []
    for line in brief_text.split("\n"):
        line = line.strip()
        if not line:
            continue
        for chunk in [line[i:i+1999] for i in range(0, len(line), 1999)]:
            children.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": chunk}}]
                },
            })

    page = notion.pages.create(
        parent={"database_id": NOTION_DATABASE_ID},
        properties={
            # Required: the page title
            "Insight Title": {
                "title": [{"type": "text", "text": {"content": title}}]
            },
            # Date the brief was posted in Slack
            "Date Collected": {
                "date": {"start": date_str}
            },
            # Who/what sent the insight
            "Contributor": {
                "rich_text": [{"type": "text", "text": {"content": "Momentum"}}]
            },
            # The full brief text goes into Key Takeaways
            "Key Takeaways": {
                "rich_text": [{"type": "text", "text": {"content": brief_text[:1999]}}]
            },
            # Source is the Slack channel
            "Source Document": {
                "rich_text": [{"type": "text", "text": {"content": "Slack #marketing-insights"}}]
            },
        },
        # Full brief text also appears in the page body for easy reading
        children=children,
    )

    return page.get("url", "")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    slack  = WebClient(token=SLACK_BOT_TOKEN)
    notion = NotionClient(auth=NOTION_API_KEY)

    print(f"Checking Slack for briefs from the last {LOOKBACK_DAYS} days...")
    brief = fetch_latest_brief(slack)

    if not brief:
        print("No Momentum brief found in that window. Nothing to sync.")
        return

    print("Brief found. Checking if it's already in Notion...")
    if already_synced(notion, brief["ts"]):
        print("Already synced — skipping.")
        return

    print("Syncing to Notion...")
    url = push_to_notion(notion, brief["text"], brief["ts"])
    print(f"Done! New Notion page: {url}")


if __name__ == "__main__":
    main()
