# am2sp

CLI to transfer Music.app library + playlists to Spotify.

Priorities:
- Preserve library order by Music.app `dateAdded`.
- Sync playlists with track order.
- Support dry-run, retries/backoff, logs, and JSON reports.

## Setup

```bash
uv sync
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
uv run am2sp.py extract --output data/exports/music-export.json
```

Dry-run full sync:

```bash
uv run am2sp.py sync --dry-run
```

Real sync:

```bash
uv run am2sp.py sync
```

Useful flags:
- `--library-only`
- `--playlists-only`
- `--playlist-strategy create-missing|create|append|replace`
- `--limit-tracks N`, `--limit-playlists N`
- `--no-browser`

Outputs go to `data/` by default (`data/logs`, `data/reports`, `data/cache`).

Rich progress UI is enabled by default. Disable with `--no-rich-progress`.
