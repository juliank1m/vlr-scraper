from __future__ import annotations

import csv
import os
import re
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

MATCHES_PATH = "data/raw/matches.csv"
OUTPUT_PATH = "data/raw/games.csv"
HEADERS = {"User-Agent": "Mozilla/5.0"}
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_FETCH_ATTEMPTS = 6
RETRY_BASE_DELAY_SECONDS = 2
FIELDNAMES = [
    "match_id",
    "game_id",
    "map_number",
    "map_name",
    "team1_score",
    "team2_score",
    "winner",
]


def extract_first_number(value: str | None) -> int | None:
    if value is None:
        return None
    match = re.search(r"-?\d+", value)
    return int(match.group()) if match else None


def extract_map_name(header) -> str | None:
    map_container = header.find(class_="map")
    if not map_container:
        return None

    map_name = map_container.find(string=re.compile(r"\S"))
    return map_name.strip() if map_name else None


def extract_map_number(label: str) -> str | None:
    match = re.search(r"\b(\d+)\b", label)
    return match.group(1) if match else None


def build_map_number_lookup(soup: BeautifulSoup) -> dict[str, str]:
    lookup = {}
    for nav_item in soup.select(".vm-stats-gamesnav-item[data-game-id]"):
        game_id = nav_item.get("data-game-id")
        if not game_id or game_id == "all":
            continue

        label = " ".join(nav_item.stripped_strings)
        map_number = extract_map_number(label)
        if map_number:
            lookup[game_id] = map_number

    return lookup


def extract_game_rows(soup: BeautifulSoup, match_row: pd.Series) -> list[dict[str, str | None]]:
    map_number_lookup = build_map_number_lookup(soup)
    game_rows = []

    for fallback_index, game_panel in enumerate(soup.select(".vm-stats-game[data-game-id]"), start=1):
        game_id = game_panel.get("data-game-id")
        if not game_id or game_id == "all":
            continue

        header = game_panel.find(class_="vm-stats-game-header")
        if not header:
            continue

        scores = [score.get_text(strip=True) for score in header.select(".score")]
        if len(scores) < 2:
            continue

        team1_score = scores[0]
        team2_score = scores[-1]
        team1_rounds = extract_first_number(team1_score)
        team2_rounds = extract_first_number(team2_score)

        winner = None
        if team1_rounds is not None and team2_rounds is not None and team1_rounds != team2_rounds:
            winner = match_row["team1"] if team1_rounds > team2_rounds else match_row["team2"]

        game_rows.append(
            {
                "match_id": match_row["match_id"],
                "game_id": game_id,
                "map_number": map_number_lookup.get(game_id, str(fallback_index)),
                "map_name": extract_map_name(header),
                "team1_score": team1_score,
                "team2_score": team2_score,
                "winner": winner,
            }
        )

    return game_rows


def fetch_match_soup(session: requests.Session, url: str) -> BeautifulSoup:
    last_error: requests.RequestException | None = None

    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        try:
            response = session.get(url, timeout=15)
            if response.status_code in RETRYABLE_STATUS_CODES:
                response.raise_for_status()
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except requests.RequestException as exc:
            last_error = exc
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            is_retryable = status_code in RETRYABLE_STATUS_CODES or status_code is None

            if not is_retryable or attempt == MAX_FETCH_ATTEMPTS:
                raise

            delay_seconds = RETRY_BASE_DELAY_SECONDS * attempt
            print(
                f"Fetch failed for {url} "
                f"(attempt {attempt}/{MAX_FETCH_ATTEMPTS}, status={status_code or 'network'}). "
                f"Retrying in {delay_seconds}s."
            )
            time.sleep(delay_seconds)

    if last_error:
        raise last_error
    raise RuntimeError(f"Unable to fetch {url}")


def read_existing_rows(path: str) -> list[dict[str, str]]:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return []

    with open(path, "r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file, delimiter="|")
        return list(reader)


def write_rows(path: str, rows: list[dict[str, str | None]]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES, delimiter="|")
        writer.writeheader()
        writer.writerows(rows)


def append_rows(path: str, rows: list[dict[str, str | None]]) -> None:
    if not rows:
        return

    os.makedirs(os.path.dirname(path), exist_ok=True)
    file_exists = os.path.exists(path) and os.path.getsize(path) > 0

    with open(path, "a", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES, delimiter="|")
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def deduplicate_rows(rows: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
    seen_keys: set[tuple[str | None, str | None]] = set()
    deduplicated_rows: list[dict[str, str | None]] = []

    for row in rows:
        key = (row.get("match_id"), row.get("game_id"))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduplicated_rows.append(row)

    return deduplicated_rows


def scrape_match_games(session: requests.Session, match_row: pd.Series) -> list[dict[str, str | None]]:
    soup = fetch_match_soup(session, match_row["match_url"])
    games = extract_game_rows(soup, match_row)

    if not games:
        overview_soup = fetch_match_soup(session, f'{match_row["match_url"]}?tab=overview')
        games = extract_game_rows(overview_soup, match_row)

    return games


def collect_games_for_matches(session: requests.Session, match_rows: list[dict[str, str]], start_label: str) -> list[dict[str, str | None]]:
    collected_rows: list[dict[str, str | None]] = []

    for offset, match_row_dict in enumerate(match_rows):
        match_row = pd.Series(match_row_dict)
        games = scrape_match_games(session, match_row)
        collected_rows.extend(games)
        print(
            f"{start_label} {offset + 1}/{len(match_rows)} "
            f'({match_row["match_id"]}) -> {len(games)} games'
        )

    return collected_rows


def main() -> None:
    df = pd.read_csv(MATCHES_PATH, sep="|")
    df["match_id"] = df["match_id"].astype(str)
    match_rows = df.to_dict("records")
    match_index = {row["match_id"]: i for i, row in enumerate(match_rows)}
    existing_rows = read_existing_rows(OUTPUT_PATH)
    session = requests.Session()
    session.headers.update(HEADERS)

    prepend_matches: list[dict[str, str]] = []
    append_matches: list[dict[str, str]] = []
    kept_existing_rows = existing_rows
    resume_rewrite_needed = False

    if existing_rows:
        top_existing_match_id = existing_rows[0]["match_id"]
        top_existing_index = match_index.get(top_existing_match_id)
        if top_existing_index is None:
            prepend_matches = match_rows
            kept_existing_rows = []
            print("Top existing match_id was not found in matches.csv; rebuilding games.csv from current matches.")
        elif top_existing_index > 0:
            prepend_matches = match_rows[:top_existing_index]
            print(f"Found {len(prepend_matches)} new matches to prepend at the top.")
            resume_rewrite_needed = True

        last_existing_match_id = existing_rows[-1]["match_id"]
        last_existing_index = match_index.get(last_existing_match_id)
        if last_existing_index is not None and last_existing_index < len(match_rows) - 1:
            kept_existing_rows = [row for row in kept_existing_rows if row["match_id"] != last_existing_match_id]
            append_matches = match_rows[last_existing_index:]
            resume_rewrite_needed = True
            print(
                f"Resuming interrupted historical scrape from match_id {last_existing_match_id}; "
                "removed its existing rows before continuing."
            )
    else:
        append_matches = match_rows
        print("games.csv does not exist yet; scraping all matches.")

    prepended_rows = collect_games_for_matches(session, prepend_matches, "Prepend") if prepend_matches else []

    if not prepended_rows and not append_matches and not resume_rewrite_needed:
        print("games.csv is already up to date.")
        return

    base_rows = deduplicate_rows(prepended_rows + kept_existing_rows)
    write_rows(OUTPUT_PATH, base_rows)

    appended_row_count = 0
    if append_matches:
        for offset, match_row_dict in enumerate(append_matches):
            match_row = pd.Series(match_row_dict)
            games = scrape_match_games(session, match_row)
            append_rows(OUTPUT_PATH, games)
            appended_row_count += len(games)
            print(
                f"Append {offset + 1}/{len(append_matches)} "
                f'({match_row["match_id"]}) -> {len(games)} games'
            )

    print(
        f"Wrote {len(base_rows) + appended_row_count} total game rows "
        f"({len(prepended_rows)} prepended, {appended_row_count} appended)."
    )


if __name__ == "__main__":
    main()
