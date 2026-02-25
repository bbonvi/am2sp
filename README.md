# am2sp

CLI to transfer Music.app library + playlists to Spotify.

Priorities:
- Preserve library order by Music.app `dateAdded`.
- Sync playlists with track order.
- Support dry-run, retries/backoff, logs, and JSON reports.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set `.env`:

```bash
SPOTIFY_CLIENT_ID=...
SPOTIFY_SECRET=...
SPOTIFY_REDIRECT_URI=http://127.0.0.1:8888/callback
```

Important: `SPOTIFY_REDIRECT_URI` must exactly match one Redirect URI in your Spotify app settings.

## Usage

Extract source data only:

```bash
python3 am2sp.py extract --output .dev/exports/music-export.json
```

Dry-run full sync:

```bash
python3 am2sp.py sync --dry-run
```

Real sync:

```bash
python3 am2sp.py sync
```

Useful flags:
- `--library-only`
- `--playlists-only`
- `--playlist-strategy create-missing|create|append|replace`
- `--limit-tracks N`, `--limit-playlists N`
- `--no-browser`

Outputs go to `.dev/logs/` and `.dev/reports/` by default.
