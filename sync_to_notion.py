"""
sync_to_notion.py
-----------------
Fetches the latest Marketing Insights brief from Slack (posted by Momentum)
and creates one Notion database row per competitor section.
"""

import os
import re
import sys
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from notion_client import Client as NotionClient

load_dotenv()

SLACK_BOT_TOKEN    = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID   = os.environ["SLACK_CHANNEL_ID"]
NOTION_API_KEY     = os.environ["NOTION_API_KEY"]
NOTION_DATABASE_ID = "7c56d30fe9be4803bd00a91cd5c40ca0"

LOOKBACK_DAYS = 8


# ---------------------------------------------------------------------------
# Step 1 — Fetch the brief from Slack
# ---------------------------------------------------------------------------

def fetch_latest_brief(slack: WebClient) -> dict | None:
    oldest_ts = (datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)).timestamp()

    try:
        response = slack.conversations_history(
            channel=SLACK_CHANNEL_ID,
            oldest=str(oldest_ts),
            limit=50,
        )
    except SlackApiError as e:
        print(f"[ERROR] Slack: {e.response['error']}")
        sys.exit(1)

    for msg in response.get("messages", []):
        text = msg.get("text", "")
        for attachment in msg.get("attachments", []):
            text += "\n" + attachment.get("text", "")
        for block in msg.get("blocks", []):
            if block.get("type") == "section":
                text += "\n" + block.get("text", {}).get("text", "")
        text = text.strip()

        if "leave their current software" in text.lower():
            return {"text": text, "ts": msg["ts"]}

    return None


# ---------------------------------------------------------------------------
# Step 2 — Parse the brief into one section per competitor
# ---------------------------------------------------------------------------

def extract_date_range(text: str) -> str:
    """Pull the start date from 'Week of YYYY-MM-DD to ...'"""
    match = re.search(r"Week of (\d{4}-\d{2}-\d{2})", text, re.IGNORECASE)
    return match.group(1) if match else datetime.now().strftime("%Y-%m-%d")


def is_section_header(line: str) -> bool:
    """
    A competitor section header is a short line that ends with * (Slack bold marker)
    and isn't a sub-item like '# of prospects' or 'Primary reasons for switching'.
    """
    if not line.endswith("*"):
        return False
    if line.strip() in ("undefined*", "*"):
        return False
    skip_prefixes = (
        "#", "primary", "nuance", "critical", "new pattern",
        "this", "these", "the ", "a ", "an ",
        "booker", "square/", "fresha", "moxie", "boomerang", "helmbot",
    )
    if any(line.lower().startswith(p) for p in skip_prefixes):
        return False
    # Sub-items tend to be long sentences; headers are short names
    if len(line) > 160:
        return False
    return True


def parse_sections(brief_text: str) -> list[dict]:
    """
    Split the brief into one dict per competitor.
    Each dict has: name, content, date_str.
    """
    date_str = extract_date_range(brief_text)

    sections: list[dict] = []
    current: dict | None = None

    for line in brief_text.split("\n"):
        line = line.strip()
        if not line:
            continue

        # Skip title and intro summary lines
        if "leave their current software" in line.lower():
            continue
        if line.lower().startswith(("this compiled", "this consolidated")):
            continue

        if is_section_header(line):
            if current:
                sections.append(current)
            name = line.rstrip("*").strip()
            current = {"name": name, "content": "", "date_str": date_str}
        elif current:
            current["content"] += ("" if not current["content"] else "\n") + line

    if current:
        sections.append(current)

    return sections


# ---------------------------------------------------------------------------
# Step 3 — Push to Notion (one row per competitor)
# ---------------------------------------------------------------------------

def already_synced(notion: NotionClient, competitor: str, date_str: str) -> bool:
    results = notion.databases.query(
        database_id=NOTION_DATABASE_ID,
        filter={
            "property": "Insight Title",
            "title": {"contains": f"{competitor} \u2014 {date_str}"},
        },
    )
    return len(results["results"]) > 0


def rich_text(value: str) -> list:
    return [{"type": "text", "text": {"content": value[:1999]}}]


def build_properties(section: dict) -> dict:
    title = f"{section['name']} \u2014 {section['date_str']}"

    return {
        "Insight Title":  {"title": rich_text(title)},
        "Date Collected": {"date": {"start": section["date_str"]}},
        "Contributor":    {"rich_text": rich_text("Momentum")},
        "Key Takeaways":  {"rich_text": rich_text(section["content"])},
        "Insight Type":   {"select": {"name": "Competitive Intelligence"}},
        "Source Document":{"rich_text": rich_text("Slack #marketing-insights")},
    }


def sync_sections(notion: NotionClient, sections: list[dict]) -> tuple[int, int]:
    created = 0
    skipped = 0

    for section in sections:
        if already_synced(notion, section["name"], section["date_str"]):
            print(f"  Already exists — skipping: {section['name']}")
            skipped += 1
            continue

        props = build_properties(section)

        # Full content also goes in the page body for easy reading
        children = [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {"rich_text": rich_text(chunk)},
            }
            for chunk in [section["content"][i:i+1999]
                          for i in range(0, len(section["content"]), 1999)]
        ]

        page = notion.pages.create(
            parent={"database_id": NOTION_DATABASE_ID},
            properties=props,
            children=children,
        )
        print(f"  Created: {section['name']} → {page.get('url', '')}")
        created += 1

    return created, skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    slack  = WebClient(token=SLACK_BOT_TOKEN)
    notion = NotionClient(auth=NOTION_API_KEY)

    print(f"Checking Slack for briefs from the last {LOOKBACK_DAYS} days...")
    brief = fetch_latest_brief(slack)

    if not brief:
        print("No brief found. Nothing to sync.")
        return

    print("Parsing competitor sections...")
    sections = parse_sections(brief["text"])

    if not sections:
        print("No sections found — the brief format may have changed.")
        return

    print(f"Found {len(sections)} section(s): {[s['name'] for s in sections]}\n")
    print("Syncing to Notion...")
    created, skipped = sync_sections(notion, sections)
    print(f"\nDone! {created} row(s) created, {skipped} skipped.")


if __name__ == "__main__":
    main()
