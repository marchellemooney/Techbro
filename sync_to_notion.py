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


def extract_competitor_name(line: str) -> str:
    """Pull the competitor/section name from a brief line."""
    # Strip leading bullets and label prefixes like NEW:, CRITICAL:, * undefined
    clean = re.sub(r"^[\*\-\s]*(?:undefined\s+)?", "", line).strip()
    clean = re.sub(r"^(?:NEW|CRITICAL):\s*", "", clean, flags=re.IGNORECASE).strip()

    # Name is everything before the first significant separator keyword
    match = re.match(
        r"^(.*?)\s+-\s+(?:#\s*of\s+prospects|Source:|Primary\s+reasons|Data\s+&|Strategic\s+Implication)",
        clean,
        re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()

    # Fallback: text before the first ' - '
    parts = clean.split(" - ", 1)
    return parts[0].strip()[:120]


def parse_sections(brief_text: str) -> list[dict]:
    """
    Split the brief into one dict per competitor.
    Each dict has: name, content, date_str.
    """
    date_str = extract_date_range(brief_text)

    sections = []
    for line in brief_text.split("\n"):
        line = line.strip()
        if not line:
            continue

        # Skip the title line and the opening summary paragraph
        if "leave their current software" in line.lower():
            continue
        if line.lower().startswith("this consolidated"):
            continue

        name = extract_competitor_name(line)
        if name:
            sections.append({
                "name": name,
                "content": line,
                "date_str": date_str,
            })

    return sections


# ---------------------------------------------------------------------------
# Step 3 — Push to Notion (one row per competitor)
# ---------------------------------------------------------------------------

def get_property_types(notion: NotionClient) -> dict:
    """Return {property_name: type_string} for every column in the DB."""
    db = notion.databases.retrieve(database_id=NOTION_DATABASE_ID)
    return {name: prop["type"] for name, prop in db["properties"].items()}


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


def build_properties(section: dict, prop_types: dict) -> dict:
    title = f"{section['name']} \u2014 {section['date_str']}"

    props: dict = {
        "Insight Title": {"title": rich_text(title)},
    }

    def set_prop(name, value):
        """Set a property using the correct type for this database."""
        if name not in prop_types:
            return
        t = prop_types[name]
        if t == "rich_text":
            props[name] = {"rich_text": rich_text(value)}
        elif t == "select":
            props[name] = {"select": {"name": value}}
        elif t == "multi_select":
            props[name] = {"multi_select": [{"name": value}]}
        elif t == "url":
            props[name] = {"url": value}
        elif t == "email":
            pass  # skip
        elif t == "date":
            props[name] = {"date": {"start": value}}

    set_prop("Date Collected", section["date_str"])
    set_prop("Contributor", "Momentum")
    set_prop("Key Takeaways", section["content"])
    set_prop("Insight Type", "Competitive Intelligence")
    set_prop("Source Document", "Slack #marketing-insights")

    return props


def sync_sections(notion: NotionClient, sections: list[dict]) -> tuple[int, int]:
    prop_types = get_property_types(notion)

    created = 0
    skipped = 0

    for section in sections:
        if already_synced(notion, section["name"], section["date_str"]):
            print(f"  Already exists — skipping: {section['name']}")
            skipped += 1
            continue

        props = build_properties(section, prop_types)

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
