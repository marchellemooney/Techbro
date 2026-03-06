"""
sync_to_notion.py
-----------------
Fetches the latest Marketing Insights brief from Slack (posted by Momentum)
and upserts one Notion database row per competitor — updating it each week
rather than adding new rows.
"""

import json
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
PROMOTION_THRESHOLD = 3          # weeks a name must appear before getting its own row
MENTION_COUNTS_FILE = os.path.join(os.path.dirname(__file__), "mention_counts.json")


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
# Step 2b — Promote frequent "other platforms" competitors to their own row
# ---------------------------------------------------------------------------

def load_mention_counts() -> dict:
    if os.path.exists(MENTION_COUNTS_FILE):
        with open(MENTION_COUNTS_FILE) as f:
            return json.load(f)
    return {}


def save_mention_counts(counts: dict) -> None:
    with open(MENTION_COUNTS_FILE, "w") as f:
        json.dump(counts, f, indent=2)


def is_catchall_section(name: str) -> bool:
    keywords = ("other", "additional", "misc", "various", "alternative")
    return any(k in name.lower() for k in keywords)


def extract_named_competitors(content: str) -> list[str]:
    """Extract competitor names from lines like 'Booker: ...' or 'Square - ...'"""
    names = []
    for line in content.split("\n"):
        match = re.match(r"^\*?([A-Z][A-Za-z0-9/& ]{1,30}?)\*?\s*[:\-–]", line.strip())
        if match:
            names.append(match.group(1).strip())
    return names


def promote_competitors(sections: list[dict], counts: dict) -> list[dict]:
    """
    Scan catchall sections for named competitors. Increment their mention count.
    If a competitor hits PROMOTION_THRESHOLD and doesn't already have its own
    section, inject a dedicated section for them.
    """
    top_level_names = {s["name"].lower() for s in sections if not is_catchall_section(s["name"])}
    extra: list[dict] = []

    for section in sections:
        if not is_catchall_section(section["name"]):
            continue

        for name in extract_named_competitors(section["content"]):
            if name.lower() in top_level_names:
                continue  # already has its own section

            counts[name] = counts.get(name, 0) + 1

            if counts[name] >= PROMOTION_THRESHOLD:
                if not any(s["name"].lower() == name.lower() for s in extra):
                    relevant = [
                        l for l in section["content"].split("\n")
                        if name.lower() in l.lower()
                    ]
                    extra.append({
                        "name": name,
                        "content": "\n".join(relevant),
                        "date_str": section["date_str"],
                    })
                    print(f"  Promoting '{name}' to its own row "
                          f"(mentioned {counts[name]} weeks in other platforms)")

    return extra


# ---------------------------------------------------------------------------
# Step 3 — Upsert to Notion (one persistent row per competitor)
# ---------------------------------------------------------------------------

def rich_text(value: str) -> list:
    return [{"type": "text", "text": {"content": value[:1999]}}]


def find_competitor_page(notion: NotionClient, competitor_name: str) -> str | None:
    """Return the page_id of an existing row for this competitor, or None."""
    results = notion.databases.query(
        database_id=NOTION_DATABASE_ID,
        filter={
            "property": "Insight Title",
            "title": {"equals": competitor_name},
        },
    )
    pages = results.get("results", [])
    return pages[0]["id"] if pages else None


def build_properties(section: dict) -> dict:
    return {
        "Insight Title":  {"title": rich_text(section["name"])},
        "Date Collected": {"date": {"start": section["date_str"]}},
        "Key Takeaways":  {"rich_text": rich_text(section["content"])},
        "Insight Type":   {"select": {"name": "Competitive Intelligence"}},
    }


def content_blocks(content: str) -> list:
    return [
        {
            "object": "block",
            "type": "paragraph",
            "paragraph": {"rich_text": rich_text(chunk)},
        }
        for chunk in [content[i:i+1999] for i in range(0, len(content), 1999)]
    ]


def get_page_body_text(notion: NotionClient, page_id: str) -> str:
    """Return all plain text from existing page blocks."""
    existing = notion.blocks.children.list(block_id=page_id)
    lines = []
    for block in existing.get("results", []):
        if block.get("type") in ("paragraph", "heading_2"):
            for rt in block.get(block["type"], {}).get("rich_text", []):
                lines.append(rt.get("plain_text", ""))
    return "\n".join(lines)


def filter_new_lines(new_content: str, existing_text: str) -> str:
    """Return only lines from new_content that don't already exist in the page."""
    existing_lines = {l.strip().lower() for l in existing_text.split("\n") if l.strip()}
    new_lines = [
        line for line in new_content.split("\n")
        if line.strip() and line.strip().lower() not in existing_lines
    ]
    return "\n".join(new_lines)


def upsert_sections(notion: NotionClient, sections: list[dict]) -> tuple[int, int]:
    created = 0
    updated = 0

    for section in sections:
        page_id = find_competitor_page(notion, section["name"])

        if page_id:
            existing_text = get_page_body_text(notion, page_id)
            new_content = filter_new_lines(section["content"], existing_text)

            if new_content.strip():
                # Prepend a week heading so new additions are clearly dated
                week_heading = {
                    "object": "block",
                    "type": "heading_2",
                    "heading_2": {"rich_text": rich_text(f"Week of {section['date_str']}")},
                }
                notion.blocks.children.append(
                    block_id=page_id,
                    children=[week_heading] + content_blocks(new_content),
                )
                # Update Date Collected to reflect the latest sync
                notion.pages.update(
                    page_id=page_id,
                    properties={"Date Collected": {"date": {"start": section["date_str"]}}},
                )
                print(f"  Updated : {section['name']} — new insights added ({section['date_str']})")
                updated += 1
            else:
                print(f"  Skipped : {section['name']} — no new insights this week")

        else:
            props = build_properties(section)
            week_heading = {
                "object": "block",
                "type": "heading_2",
                "heading_2": {"rich_text": rich_text(f"Week of {section['date_str']}")},
            }
            page = notion.pages.create(
                parent={"database_id": NOTION_DATABASE_ID},
                properties=props,
                children=[week_heading] + content_blocks(section["content"]),
            )
            print(f"  Created : {section['name']} → {page.get('url', '')}")
            created += 1

    return created, updated


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

    counts = load_mention_counts()
    promoted = promote_competitors(sections, counts)
    save_mention_counts(counts)
    sections.extend(promoted)

    print("Upserting to Notion...")
    created, updated = upsert_sections(notion, sections)
    print(f"\nDone! {created} row(s) created, {updated} updated.")


if __name__ == "__main__":
    main()
