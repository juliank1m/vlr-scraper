# scrape_vlr

![Python](https://img.shields.io/badge/python-3.x-blue)

Simple VLR.gg scrapers for:

> match results

> map/game results

> player stats by map

## Files

> `scrape_results.py` -> scrapes completed matches into `data/raw/matches.csv`

> `scrape_games.py` -> scrapes map-level results into `data/raw/games.csv`

> `scrape_player_stats.py` -> scrapes player stats into `data/raw/player_stats.csv`

## Run Order

```bash
python3 scrape_results.py
python3 scrape_games.py
python3 scrape_player_stats.py
```

## Output

> All generated data is stored in `data/raw/`.

> Small example files in `data/raw/local/`.

## Notes

> The scrapers are incremental, so rerunning them updates existing datasets instead of rebuilding everything every time.

> `scrape_player_stats.py` depends on both `matches.csv` and `games.csv`.

> Main dependencies are `requests`, `beautifulsoup4`, and `pandas`.
