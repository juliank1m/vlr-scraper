"""Scrape team logos from VLR match pages → CSV output.

Usage:
    python scrape_logos.py
"""

import csv
from collections import defaultdict
from pathlib import Path

import requests
from bs4 import BeautifulSoup

MATCHES_CSV = Path("data/raw/matches.csv")
OUTPUT_CSV = Path("data/raw/team_logos.csv")
HEADERS = {"User-Agent": "Mozilla/5.0"}


def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    with MATCHES_CSV.open("r", encoding="utf-8") as f:
        matches = list(csv.DictReader(f, delimiter="|"))

    print(f"Loaded {len(matches)} matches")

    # One URL per team name
    team_url: dict[str, tuple[str, str]] = {}
    for row in matches:
        t1, t2, url = row.get("team1"), row.get("team2"), row.get("match_url")
        if not url:
            continue
        if t1 and t1 not in team_url:
            team_url[t1] = (url, "mod-1")
        if t2 and t2 not in team_url:
            team_url[t2] = (url, "mod-2")

    # Group by URL so each page is fetched once
    url_teams: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for name, (url, mod) in team_url.items():
        url_teams[url].append((name, mod))

    total = len(url_teams)
    print(f"Need to fetch {total} unique match pages for {len(team_url)} teams")

    # Load existing progress
    done: dict[str, str] = {}
    if OUTPUT_CSV.exists():
        with OUTPUT_CSV.open("r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                done[row["team_name"]] = row["logo_url"]
        print(f"Resuming: {len(done)} teams already done")

    out = OUTPUT_CSV.open("a" if done else "w", encoding="utf-8", newline="")
    writer = csv.DictWriter(out, fieldnames=["team_name", "logo_url"])
    if not done:
        writer.writeheader()

    updated = 0
    try:
        for i, (url, teams) in enumerate(url_teams.items(), 1):
            remaining = [(n, m) for n, m in teams if n not in done]
            if not remaining:
                continue

            print(f"[{i}/{total}] {url}...", end=" ", flush=True)
            try:
                resp = session.get(url, timeout=15)
                resp.raise_for_status()
            except Exception as e:
                print(f"✗ {e}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for name, mod in remaining:
                link = soup.select_one(f"a.match-header-link.{mod}")
                if not link:
                    continue
                img = link.select_one("img[src]")
                if not img:
                    continue
                src = img.get("src", "")
                if src.startswith("//"):
                    src = f"https:{src}"
                if not src:
                    continue

                writer.writerow({"team_name": name, "logo_url": src})
                out.flush()
                done[name] = src
                updated += 1

            print(f"✓ ({updated} logos found)")
    except KeyboardInterrupt:
        print(f"\nInterrupted. {updated} logos saved.")
    finally:
        out.close()

    print(f"Done: {updated} new logos written to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
