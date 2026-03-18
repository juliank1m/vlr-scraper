from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://www.vlr.gg"
RESULTS_URL = f"{BASE_URL}/matches/results"
HEADERS = {"User-Agent": "Mozilla/5.0"}
DEFAULT_MAX_PAGES = 628
MAX_ALLOWED_PAGES = 5000
OUTPUT_PATH = Path("data/raw/matches.csv")
FIELDNAMES = [
    "match_id",
    "match_url",
    "time",
    "date",
    "winner",
    "team1",
    "team2",
    "team1_score",
    "team2_score",
    "event",
    "stage",
    "status",
]


def parse_max_pages(value: str) -> int:
    page_count = int(value)
    if page_count < 1:
        raise argparse.ArgumentTypeError("--max-pages must be at least 1.")
    if page_count > MAX_ALLOWED_PAGES:
        raise argparse.ArgumentTypeError(
            f"--max-pages must be {MAX_ALLOWED_PAGES} or less."
        )
    return page_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update the VLR results CSV by scraping only new result pages."
    )
    parser.add_argument(
        "--max-pages",
        type=parse_max_pages,
        default=DEFAULT_MAX_PAGES,
        help=f"Maximum number of result pages to scan (default: {DEFAULT_MAX_PAGES}).",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore the existing CSV and rebuild it from scratch.",
    )
    return parser.parse_args()


def read_existing_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file, delimiter="|")
        return list(reader)


def write_rows(path: Path, rows: list[dict[str, str | None]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES, delimiter="|")
        writer.writeheader()
        writer.writerows(rows)


def deduplicate_rows(rows: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
    seen_match_ids: set[str] = set()
    deduplicated_rows: list[dict[str, str | None]] = []

    for row in rows:
        match_id = row.get("match_id")
        if not match_id or match_id in seen_match_ids:
            continue
        seen_match_ids.add(match_id)
        deduplicated_rows.append(row)

    return deduplicated_rows


def extract_match_id(href: str | None) -> str | None:
    if not href:
        return None

    match = re.search(r"/(\d+)/", href)
    return match.group(1) if match else None


def get_text(tag: Tag | None) -> str | None:
    if not tag:
        return None
    text = tag.get_text(" ", strip=True)
    return text or None


def get_direct_text(tag: Tag | None) -> str | None:
    if not tag:
        return None

    text = "".join(
        string.strip() for string in tag.find_all(string=True, recursive=False) if string.strip()
    )
    return text or None


def parse_match_item(match_item: Tag, current_date: str | None) -> dict[str, str | None] | None:
    href = match_item.get("href")
    match_id = extract_match_id(href)
    status = get_text(match_item.select_one(".ml-status"))

    if not href or not match_id or status != "Completed":
        return None

    teams = [get_text(tag) for tag in match_item.select(".match-item-vs-team-name .text-of")]
    scores = [get_text(tag) for tag in match_item.select(".match-item-vs-team-score")]

    if len(teams) < 2 or len(scores) < 2:
        return None

    return {
        "match_id": match_id,
        "match_url": f"{BASE_URL}{href}",
        "time": get_text(match_item.select_one(".match-item-time")),
        "date": current_date,
        "winner": get_text(match_item.select_one(".match-item-vs-team.mod-winner .text-of")),
        "team1": teams[0],
        "team2": teams[1],
        "team1_score": scores[0],
        "team2_score": scores[1],
        "event": get_direct_text(match_item.select_one(".match-item-event")),
        "stage": get_text(match_item.select_one(".match-item-event-series")),
        "status": status,
    }


def parse_results_page(html: str) -> list[dict[str, str | None]]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("div.col.mod-1")
    if not container:
        return []

    matches: list[dict[str, str | None]] = []
    current_date: str | None = None

    for element in container.children:
        if not isinstance(element, Tag):
            continue

        classes = set(element.get("class", []))
        if "wf-label" in classes:
            current_date = get_direct_text(element)
            continue

        if "wf-card" not in classes:
            continue

        for match_item in element.select("a.wf-module-item.match-item[href]"):
            match_row = parse_match_item(match_item, current_date)
            if match_row:
                matches.append(match_row)

    return matches


def fetch_results_page(session: requests.Session, page: int) -> str | None:
    response = session.get(f"{RESULTS_URL}/?page={page}", timeout=15)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.text


def scrape_new_rows(
    session: requests.Session,
    existing_match_ids: set[str],
    max_pages: int,
) -> list[dict[str, str | None]]:
    new_rows: list[dict[str, str | None]] = []

    for page in range(1, max_pages + 1):
        html = fetch_results_page(session, page)
        if html is None:
            print(f"Reached the end of results at page {page}.")
            break

        page_rows = parse_results_page(html)
        if not page_rows:
            print(f"Reached an empty results page at page {page}.")
            break

        page_new_rows: list[dict[str, str | None]] = []
        saw_existing_match = False

        for row in page_rows:
            match_id = row.get("match_id")
            if match_id in existing_match_ids:
                saw_existing_match = True
                continue
            page_new_rows.append(row)

        new_rows.extend(page_new_rows)
        print(
            f"Done page {page}: {len(page_new_rows)} new matches, "
            f"{len(page_rows) - len(page_new_rows)} existing matches"
        )

        # Results pages are newest-first, so once a known match appears we can stop scanning.
        if saw_existing_match:
            break

    return new_rows


def main() -> None:
    args = parse_args()
    output_path = OUTPUT_PATH
    existing_rows = [] if args.full_refresh else read_existing_rows(output_path)
    existing_match_ids = {row["match_id"] for row in existing_rows if row.get("match_id")}

    session = requests.Session()
    session.headers.update(HEADERS)

    new_rows = scrape_new_rows(
        session=session,
        existing_match_ids=existing_match_ids,
        max_pages=args.max_pages,
    )

    if args.full_refresh:
        combined_rows = deduplicate_rows(new_rows)
    else:
        combined_rows = deduplicate_rows(new_rows + existing_rows)

    if not new_rows and output_path.exists() and not args.full_refresh:
        print(f"No new matches found. {output_path} is already up to date.")
        return

    write_rows(output_path, combined_rows)
    print(f"Wrote {len(combined_rows)} matches to {output_path}")


if __name__ == "__main__":
    main()
