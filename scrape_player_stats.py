from __future__ import annotations

import csv
import re
import time
from collections import defaultdict
from pathlib import Path

import requests
from bs4 import BeautifulSoup, Tag

BASE_URL = "https://www.vlr.gg"
MATCHES_PATH = Path("data/raw/matches.csv")
GAMES_PATH = Path("data/raw/games.csv")
OUTPUT_PATH = Path("data/raw/player_stats.csv")
HEADERS = {"User-Agent": "Mozilla/5.0"}
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
MAX_FETCH_ATTEMPTS = 6
RETRY_BASE_DELAY_SECONDS = 2
STAT_FIELDS = [
    "rating",
    "acs",
    "kills",
    "deaths",
    "assists",
    "kill_death_diff",
    "kast",
    "adr",
    "hs_percent",
    "first_kills",
    "first_deaths",
    "first_kill_death_diff",
]
FIELDNAMES = [
    "match_id",
    "game_id",
    "map_number",
    "map_name",
    "team_name",
    "player_id",
    "player_url",
    "player_name",
    "agent",
    *STAT_FIELDS,
]


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    collapsed = " ".join(value.split())
    return collapsed or None


def read_existing_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
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


def append_rows(path: Path, rows: list[dict[str, str | None]]) -> None:
    if not rows:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists() and path.stat().st_size > 0

    with path.open("a", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES, delimiter="|")
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def deduplicate_rows(rows: list[dict[str, str | None]]) -> list[dict[str, str | None]]:
    seen_keys: set[tuple[str | None, str | None, str | None]] = set()
    deduplicated_rows: list[dict[str, str | None]] = []

    for row in rows:
        player_key = row.get("player_id") or row.get("player_name")
        key = (row.get("game_id"), row.get("team_name"), player_key)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduplicated_rows.append(row)

    return deduplicated_rows


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


def build_match_index(match_rows: list[dict[str, str]]) -> dict[str, int]:
    return {row["match_id"]: index for index, row in enumerate(match_rows)}


def group_games_by_match(game_rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    seen_game_keys: set[tuple[str, str]] = set()

    for row in game_rows:
        match_id = row.get("match_id")
        game_id = row.get("game_id")
        if not match_id or not game_id:
            continue

        key = (match_id, game_id)
        if key in seen_game_keys:
            continue

        seen_game_keys.add(key)
        grouped_rows[match_id].append(row)

    return dict(grouped_rows)


def get_scrapable_match_rows(
    match_rows: list[dict[str, str]],
    games_by_match: dict[str, list[dict[str, str]]],
) -> list[dict[str, str]]:
    scrapable_match_rows = [row for row in match_rows if row.get("match_id") in games_by_match]
    skipped_match_count = len(match_rows) - len(scrapable_match_rows)

    if skipped_match_count:
        print(
            f"Skipping {skipped_match_count} matches that do not have game rows in {GAMES_PATH} yet."
        )

    return scrapable_match_rows


def extract_player_id(href: str | None) -> str | None:
    if not href:
        return None

    match = re.search(r"/player/(\d+)/", href)
    return match.group(1) if match else None


def to_absolute_url(href: str | None) -> str | None:
    if not href:
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return f"{BASE_URL}{href}"


def extract_stat_value(stat_cell: Tag) -> str | None:
    for selector in (".side.mod-side.mod-both", ".side.mod-both", ".mod-both"):
        side_value = stat_cell.select_one(selector)
        if side_value:
            value = normalize_text(side_value.get_text(" ", strip=True))
            if value:
                return value

    return normalize_text(stat_cell.get_text(" ", strip=True))


def extract_agent_name(row: Tag) -> str | None:
    agent_names: list[str] = []

    for agent_image in row.select("td.mod-agents img"):
        agent_name = normalize_text(agent_image.get("title") or agent_image.get("alt"))
        if agent_name and agent_name not in agent_names:
            agent_names.append(agent_name)

    if not agent_names:
        return None
    if len(agent_names) == 1:
        return agent_names[0]
    return ", ".join(agent_names)


def extract_team_names(game_panel: Tag, match_row: dict[str, str]) -> list[str | None]:
    header = game_panel.find(class_="vm-stats-game-header")
    if not header:
        return [match_row.get("team1"), match_row.get("team2")]

    team_names = [
        normalize_text(team_name.get_text(" ", strip=True))
        for team_name in header.select(".team-name")
    ]
    if len(team_names) >= 2:
        return team_names[:2]

    fallback_team_names = [match_row.get("team1"), match_row.get("team2")]
    return team_names + fallback_team_names[len(team_names) :]


def build_player_row(
    player_row: Tag,
    game_row: dict[str, str],
    team_name: str | None,
) -> dict[str, str | None] | None:
    player_cell = player_row.select_one("td.mod-player")
    if not player_cell:
        return None

    player_link = player_cell.select_one("a[href*='/player/']")
    player_name_tag = player_cell.select_one(".text-of")
    player_name = normalize_text(
        player_name_tag.get_text(" ", strip=True) if player_name_tag else None
    )
    if not player_name and player_link:
        player_name = normalize_text(player_link.get_text(" ", strip=True))
    if not player_name:
        return None

    stat_cells = player_row.select("td.mod-stat")
    stat_values = {field: None for field in STAT_FIELDS}
    for field, stat_cell in zip(STAT_FIELDS, stat_cells):
        stat_values[field] = extract_stat_value(stat_cell)

    player_href = player_link.get("href") if player_link else None
    return {
        "match_id": game_row.get("match_id"),
        "game_id": game_row.get("game_id"),
        "map_number": game_row.get("map_number"),
        "map_name": game_row.get("map_name"),
        "team_name": team_name,
        "player_id": extract_player_id(player_href),
        "player_url": to_absolute_url(player_href),
        "player_name": player_name,
        "agent": extract_agent_name(player_row),
        **stat_values,
    }


def extract_game_player_rows(
    game_panel: Tag,
    match_row: dict[str, str],
    game_row: dict[str, str],
) -> list[dict[str, str | None]]:
    team_names = extract_team_names(game_panel, match_row)
    tables = game_panel.select("table.wf-table-inset.mod-overview")
    player_rows: list[dict[str, str | None]] = []

    for table_index, table in enumerate(tables[:2]):
        team_name = team_names[table_index] if table_index < len(team_names) else None
        for stats_row in table.select("tbody tr"):
            player_stats = build_player_row(stats_row, game_row, team_name)
            if player_stats:
                player_rows.append(player_stats)

    return player_rows


def extract_player_rows(
    soup: BeautifulSoup,
    match_row: dict[str, str],
    game_rows: list[dict[str, str]],
) -> list[dict[str, str | None]]:
    game_panel_lookup = {}
    for game_panel in soup.select(".vm-stats-game[data-game-id]"):
        game_id = game_panel.get("data-game-id")
        if not game_id or game_id == "all":
            continue
        game_panel_lookup[game_id] = game_panel

    player_rows: list[dict[str, str | None]] = []
    for game_row in game_rows:
        game_id = game_row.get("game_id")
        if not game_id:
            continue
        game_panel = game_panel_lookup.get(game_id)
        if not game_panel:
            continue
        player_rows.extend(extract_game_player_rows(game_panel, match_row, game_row))

    return player_rows


def scrape_match_player_stats(
    session: requests.Session,
    match_row: dict[str, str],
    game_rows: list[dict[str, str]],
) -> list[dict[str, str | None]]:
    if not game_rows:
        return []

    soup = fetch_match_soup(session, match_row["match_url"])
    player_rows = extract_player_rows(soup, match_row, game_rows)
    requested_game_ids = {row["game_id"] for row in game_rows if row.get("game_id")}
    scraped_game_ids = {row["game_id"] for row in player_rows if row.get("game_id")}

    if requested_game_ids - scraped_game_ids:
        overview_soup = fetch_match_soup(session, f'{match_row["match_url"]}?tab=overview')
        player_rows = extract_player_rows(overview_soup, match_row, game_rows)
        scraped_game_ids = {row["game_id"] for row in player_rows if row.get("game_id")}

    missing_game_ids = sorted(requested_game_ids - scraped_game_ids)
    if missing_game_ids:
        print(
            f"Missing player stats for {len(missing_game_ids)} games in match_id "
            f'{match_row["match_id"]}: {", ".join(missing_game_ids)}'
        )

    return deduplicate_rows(player_rows)


def collect_player_stats_for_matches(
    session: requests.Session,
    match_rows: list[dict[str, str]],
    games_by_match: dict[str, list[dict[str, str]]],
    start_label: str,
) -> list[dict[str, str | None]]:
    collected_rows: list[dict[str, str | None]] = []

    for offset, match_row in enumerate(match_rows):
        game_rows = games_by_match.get(match_row["match_id"], [])
        player_rows = scrape_match_player_stats(session, match_row, game_rows)
        collected_rows.extend(player_rows)
        print(
            f"{start_label} {offset + 1}/{len(match_rows)} "
            f'({match_row["match_id"]}) -> {len(player_rows)} player rows'
        )

    return collected_rows


def main() -> None:
    match_rows = read_existing_rows(MATCHES_PATH)
    game_rows = read_existing_rows(GAMES_PATH)
    games_by_match = group_games_by_match(game_rows)
    scrapable_match_rows = get_scrapable_match_rows(match_rows, games_by_match)
    match_index = build_match_index(scrapable_match_rows)
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
            prepend_matches = scrapable_match_rows
            kept_existing_rows = []
            print(
                "Top existing match_id was not found in the current match/game inputs; "
                "rebuilding player_stats.csv from available rows."
            )
        elif top_existing_index > 0:
            prepend_matches = scrapable_match_rows[:top_existing_index]
            print(f"Found {len(prepend_matches)} new matches to prepend at the top.")
            resume_rewrite_needed = True

        last_existing_match_id = existing_rows[-1]["match_id"]
        last_existing_index = match_index.get(last_existing_match_id)
        if last_existing_index is not None and last_existing_index < len(scrapable_match_rows) - 1:
            kept_existing_rows = [
                row for row in kept_existing_rows if row["match_id"] != last_existing_match_id
            ]
            append_matches = scrapable_match_rows[last_existing_index:]
            resume_rewrite_needed = True
            print(
                f"Resuming interrupted historical scrape from match_id {last_existing_match_id}; "
                "removed its existing rows before continuing."
            )
    else:
        append_matches = scrapable_match_rows
        print("player_stats.csv does not exist yet; scraping all matches with known game ids.")

    prepended_rows = (
        collect_player_stats_for_matches(session, prepend_matches, games_by_match, "Prepend")
        if prepend_matches
        else []
    )

    if not prepended_rows and not append_matches and not resume_rewrite_needed:
        print("player_stats.csv is already up to date.")
        return

    base_rows = deduplicate_rows(prepended_rows + kept_existing_rows)
    write_rows(OUTPUT_PATH, base_rows)

    appended_row_count = 0
    if append_matches:
        for offset, match_row in enumerate(append_matches):
            player_rows = scrape_match_player_stats(
                session,
                match_row,
                games_by_match.get(match_row["match_id"], []),
            )
            append_rows(OUTPUT_PATH, player_rows)
            appended_row_count += len(player_rows)
            print(
                f"Append {offset + 1}/{len(append_matches)} "
                f'({match_row["match_id"]}) -> {len(player_rows)} player rows'
            )

    print(
        f"Wrote {len(base_rows) + appended_row_count} total player rows "
        f"({len(prepended_rows)} prepended, {appended_row_count} appended)."
    )


if __name__ == "__main__":
    main()
