#!/usr/bin/env python3
"""Transfer Apple Music library/playlists from Music.app to Spotify.

Design priorities:
- Preserve main library order by Music.app `dateAdded` using Spotify `timestamped_ids`.
- Keep extraction read-only by querying Music.app via JXA (no DB writes/touches).
- Be resumable and fast with local mapping cache + concurrent search workers.
- Be safe by default with dry-run support and explicit logging/reporting.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import hashlib
import http.server
import json
import logging
import os
import random
import re
import secrets
import threading
import time
import urllib.parse
import webbrowser
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import subprocess


SPOTIFY_API_BASE = "https://api.spotify.com/v1"
SPOTIFY_ACCOUNTS_BASE = "https://accounts.spotify.com"
REQUEST_TIMEOUT = 30
HEARTBEAT_SECONDS = 10
EXTRACTION_HEARTBEAT_SECONDS = 3
DEFAULT_SPOTIFY_REDIRECT_URI = "http://127.0.0.1:8888/callback"


@dataclasses.dataclass(frozen=True)
class SourceTrack:
    """Canonical source track representation from Music.app."""

    persistent_id: str
    database_id: Optional[int]
    name: str
    artist: str
    album: str
    duration_seconds: Optional[float]
    date_added_iso: Optional[str]


@dataclasses.dataclass
class MappingResult:
    """Resolved Spotify mapping for a source track."""

    spotify_id: Optional[str]
    spotify_uri: Optional[str]
    confidence: float
    reason: str
    query: Optional[str]


@dataclasses.dataclass
class AuthCodeResult:
    """OAuth callback output."""

    code: Optional[str] = None
    state: Optional[str] = None
    error: Optional[str] = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_env_file(path: Path) -> Dict[str, str]:
    """Minimal `.env` parser to avoid extra runtime deps."""
    values: Dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"").strip("'")
        values[key] = value
    return values


def configure_logging(log_file: Path, verbose: bool) -> logging.Logger:
    ensure_dir(log_file.parent)
    logger = logging.getLogger("am2sp")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def detect_osascript_prefix() -> List[str]:
    """Return command prefix for osascript in this environment.

    - On macOS hosts: `osascript`
    - In OrbStack Linux guests: `mac osascript`
    """
    for cmd in (["osascript"], ["mac", "osascript"]):
        try:
            subprocess.run(
                cmd + ["-e", "return \"ok\""],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            return cmd
        except Exception:
            continue
    raise RuntimeError(
        "Could not find a working osascript runtime. "
        "Tried `osascript` and `mac osascript`."
    )


def run_music_jxa(config: Dict[str, Any], logger: logging.Logger) -> Dict[str, Any]:
    """Extract library + playlists from Music.app via JXA and return parsed JSON."""
    cmd_prefix = detect_osascript_prefix()
    logger.debug("Using JXA command prefix: %s", " ".join(cmd_prefix))

    # Count first so long extraction logs can report meaningful targets.
    counts_script = """
const Music = Application('Music');
function safeCall(fn, fallback) {
  try {
    const v = fn();
    return v === undefined ? fallback : v;
  } catch (e) {
    return fallback;
  }
}
const lib = safeCall(() => Music.libraryPlaylists[0], null);
const libraryTrackCount = lib ? safeCall(() => lib.tracks().length, 0) : 0;
const userPlaylistCount = safeCall(() => Music.userPlaylists().length, 0);
JSON.stringify({ libraryTrackCount, userPlaylistCount });
"""
    counts_raw = subprocess.run(
        cmd_prefix + ["-l", "JavaScript"],
        input=counts_script,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    expected_tracks: Optional[int] = None
    expected_playlists: Optional[int] = None
    if counts_raw.returncode == 0 and counts_raw.stdout.strip():
        try:
            counts_payload = json.loads(counts_raw.stdout.strip())
            expected_tracks = int(counts_payload.get("libraryTrackCount") or 0)
            expected_playlists = int(counts_payload.get("userPlaylistCount") or 0)
        except Exception:
            expected_tracks = None
            expected_playlists = None

    if expected_tracks is not None and config.get("limit_tracks", 0):
        expected_tracks = min(expected_tracks, int(config["limit_tracks"]))
    if expected_playlists is not None and config.get("limit_playlists", 0):
        expected_playlists = min(expected_playlists, int(config["limit_playlists"]))

    if expected_tracks is not None and expected_playlists is not None:
        logger.info(
            "Music.app preflight: expecting up to %d tracks and %d playlists",
            expected_tracks,
            expected_playlists,
        )
    else:
        logger.info("Music.app preflight: counting skipped; starting extraction")

    jxa_config = json.dumps(config)
    script = f"""
ObjC.import('Foundation');

function safeCall(fn, fallback) {{
  try {{
    const v = fn();
    return v === undefined ? fallback : v;
  }} catch (e) {{
    return fallback;
  }}
}}

function clean(v) {{
  if (v === null || v === undefined) return '';
  return String(v).replace(/\\s+/g, ' ').trim();
}}

function isoDate(v) {{
  try {{
    if (!v) return null;
    const d = new Date(v);
    if (isNaN(d.getTime())) return null;
    return d.toISOString();
  }} catch (e) {{
    return null;
  }}
}}

function arr(v) {{
  if (v === null || v === undefined) return [];
  return Array.isArray(v) ? v : [v];
}}

function tracksToObjs(trackSpec, limit) {{
  const names = arr(safeCall(() => trackSpec.name(), []));
  const artists = arr(safeCall(() => trackSpec.artist(), []));
  const albums = arr(safeCall(() => trackSpec.album(), []));
  const pids = arr(safeCall(() => trackSpec.persistentID(), []));
  const dbids = arr(safeCall(() => trackSpec.databaseID(), []));
  const durs = arr(safeCall(() => trackSpec.duration(), []));
  const dates = arr(safeCall(() => trackSpec.dateAdded(), []));

  const total = Math.max(
    names.length,
    artists.length,
    albums.length,
    pids.length,
    dbids.length,
    durs.length,
    dates.length
  );

  const cap = (limit !== null && limit >= 0) ? Math.min(total, limit) : total;
  const out = [];
  for (let i = 0; i < cap; i++) {{
    const obj = {{
      persistentId: clean(pids[i] ?? ''),
      databaseId: (dbids[i] === null || dbids[i] === undefined || dbids[i] === '') ? null : Number(dbids[i]),
      name: clean(names[i] ?? ''),
      artist: clean(artists[i] ?? ''),
      album: clean(albums[i] ?? ''),
      duration: (durs[i] === null || durs[i] === undefined || durs[i] === '') ? null : Number(durs[i]),
      dateAdded: isoDate(dates[i] ?? null),
    }};
    if (!obj.persistentId || !obj.name) continue;
    out.push(obj);
  }}
  return out;
}}

const cfg = {jxa_config};
const Music = Application('Music');
Music.includeStandardAdditions = true;

const libPlaylist = safeCall(() => Music.libraryPlaylists[0], null);
if (!libPlaylist) {{
  throw new Error('Could not access Music library playlist');
}}

const libTracks = safeCall(() => libPlaylist.tracks, null);
const libraryLimit = cfg.limit_tracks && cfg.limit_tracks > 0 ? cfg.limit_tracks : null;
const library = libTracks ? tracksToObjs(libTracks, libraryLimit) : [];

const allUserPlaylists = safeCall(() => Music.userPlaylists(), []);
const playlists = [];
const playlistLimit = cfg.limit_playlists && cfg.limit_playlists > 0 ? cfg.limit_playlists : null;

for (let i = 0; i < allUserPlaylists.length; i++) {{
  if (playlistLimit !== null && playlists.length >= playlistLimit) break;

  const p = allUserPlaylists[i];
  const smart = Boolean(safeCall(() => p.smart(), false));
  if (!cfg.include_smart_playlists && smart) continue;

  const pname = clean(safeCall(() => p.name(), ''));
  const pid = clean(safeCall(() => p.persistentID(), ''));
  if (!pname || !pid) continue;

  const ptracks = tracksToObjs(safeCall(() => p.tracks, null), null);

  if (cfg.skip_empty_playlists && ptracks.length === 0) continue;

  playlists.push({{
    persistentId: pid,
    name: pname,
    smart: smart,
    tracks: ptracks,
    trackCount: ptracks.length,
  }});
}}

const payload = {{
  generatedAt: (new Date()).toISOString(),
  libraryTrackCount: library.length,
  playlistCount: playlists.length,
  library: library,
  playlists: playlists,
}};

JSON.stringify(payload);
"""

    started = time.perf_counter()
    stop_event = threading.Event()

    def heartbeat() -> None:
        while not stop_event.wait(EXTRACTION_HEARTBEAT_SECONDS):
            elapsed = time.perf_counter() - started
            if expected_tracks is not None and expected_playlists is not None:
                logger.info(
                    "Music.app extraction in progress (%.0fs elapsed, target <=%d tracks/%d playlists)...",
                    elapsed,
                    expected_tracks,
                    expected_playlists,
                )
            else:
                logger.info("Music.app extraction in progress (%.0fs elapsed)...", elapsed)

    hb_thread = threading.Thread(target=heartbeat, daemon=True)
    hb_thread.start()
    try:
        result = subprocess.run(
            cmd_prefix + ["-l", "JavaScript"],
            input=script,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    finally:
        stop_event.set()
        hb_thread.join(timeout=0.2)

    logger.info("Music.app extraction command finished in %.1fs", time.perf_counter() - started)
    if result.returncode != 0:
        raise RuntimeError(
            "Music.app extraction failed: "
            f"exit={result.returncode} stderr={result.stderr.strip()}"
        )

    output = result.stdout.strip()
    if not output:
        raise RuntimeError("Music.app extraction returned empty payload")
    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Could not parse Music extraction JSON: {exc}; sample={output[:500]}"
        ) from exc


class OAuthCallbackHandler(http.server.BaseHTTPRequestHandler):
    """Capture OAuth callback query params into shared state."""

    result: AuthCodeResult = AuthCodeResult()
    event = threading.Event()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        OAuthCallbackHandler.result = AuthCodeResult(
            code=(query.get("code") or [None])[0],
            state=(query.get("state") or [None])[0],
            error=(query.get("error") or [None])[0],
        )
        OAuthCallbackHandler.event.set()

        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(
            b"<html><body><h3>Authorization captured.</h3>"
            b"You can close this tab and return to the terminal.</body></html>"
        )

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


class SpotifyAuth:
    """Spotify OAuth token manager with refresh + PKCE authorization flow."""

    def __init__(
        self,
        client_id: str,
        client_secret: Optional[str],
        redirect_uri: str,
        token_cache_path: Path,
        no_browser: bool,
        callback_wait_seconds: int,
        logger: logging.Logger,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = redirect_uri
        self.token_cache_path = token_cache_path
        self.no_browser = no_browser
        self.callback_wait_seconds = max(5, callback_wait_seconds)
        self.logger = logger
        self._lock = threading.Lock()
        self._token_cache = self._load_cache()

    def _load_cache(self) -> Dict[str, Any]:
        if self.token_cache_path.exists():
            try:
                return json.loads(self.token_cache_path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def _save_cache(self) -> None:
        ensure_dir(self.token_cache_path.parent)
        self.token_cache_path.write_text(
            json.dumps(self._token_cache, indent=2), encoding="utf-8"
        )

    def _token_valid(self) -> bool:
        expires_at = self._token_cache.get("expires_at")
        access_token = self._token_cache.get("access_token")
        if not access_token or not expires_at:
            return False
        return utc_now() < datetime.fromisoformat(expires_at) - timedelta(seconds=60)

    def access_token(self, scopes: Iterable[str]) -> str:
        with self._lock:
            if self._token_valid():
                return self._token_cache["access_token"]

            if self._token_cache.get("refresh_token"):
                if self._refresh_token(scopes=scopes):
                    return self._token_cache["access_token"]

            self._authorize_interactively(scopes=scopes)
            return self._token_cache["access_token"]

    def _refresh_token(self, scopes: Iterable[str]) -> bool:
        self.logger.info("Refreshing Spotify access token")
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self._token_cache["refresh_token"],
            "client_id": self.client_id,
        }
        if self.client_secret:
            data["client_secret"] = self.client_secret
        if scopes:
            data["scope"] = " ".join(sorted(set(scopes)))

        resp = requests.post(
            f"{SPOTIFY_ACCOUNTS_BASE}/api/token",
            data=data,
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            self.logger.warning(
                "Token refresh failed (%s): %s", resp.status_code, resp.text[:300]
            )
            return False

        payload = resp.json()
        self._update_token_cache(payload)
        return True

    def _update_token_cache(self, payload: Dict[str, Any]) -> None:
        self._token_cache["access_token"] = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self._token_cache["expires_at"] = (
            utc_now() + timedelta(seconds=expires_in)
        ).isoformat()
        if payload.get("refresh_token"):
            self._token_cache["refresh_token"] = payload["refresh_token"]
        self._save_cache()

    def _authorize_interactively(self, scopes: Iterable[str]) -> None:
        self.logger.info("Starting Spotify OAuth authorization flow")
        parsed = urllib.parse.urlparse(self.redirect_uri)
        if parsed.scheme != "http" or not parsed.hostname or not parsed.port:
            raise RuntimeError(
                "redirect URI must be an http://host:port/callback URI for local auth"
            )

        code_verifier = base64.urlsafe_b64encode(os.urandom(64)).decode("utf-8").rstrip("=")
        code_challenge = base64.urlsafe_b64encode(
            hashlib.sha256(code_verifier.encode("utf-8")).digest()
        ).decode("utf-8").rstrip("=")
        state = secrets.token_urlsafe(24)

        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "scope": " ".join(sorted(set(scopes))),
            "redirect_uri": self.redirect_uri,
            "state": state,
            "code_challenge_method": "S256",
            "code_challenge": code_challenge,
        }
        auth_url = f"{SPOTIFY_ACCOUNTS_BASE}/authorize?{urllib.parse.urlencode(params)}"
        self.logger.info("Using Spotify redirect URI: %s", self.redirect_uri)

        OAuthCallbackHandler.result = AuthCodeResult()
        OAuthCallbackHandler.event.clear()

        server = http.server.ThreadingHTTPServer(
            (parsed.hostname, parsed.port), OAuthCallbackHandler
        )
        server.timeout = 1

        def serve() -> None:
            while not OAuthCallbackHandler.event.is_set():
                server.handle_request()

        thread = threading.Thread(target=serve, daemon=True)
        thread.start()

        self.logger.info("Open this URL to authorize:\n%s", auth_url)
        self.logger.info(
            "If browser lands on an error page (for example HTTP 502), copy the full callback URL and paste it in the terminal when prompted."
        )
        if not self.no_browser:
            try:
                webbrowser.open(auth_url, new=2)
            except Exception as exc:
                self.logger.warning("Failed to open browser automatically: %s", exc)

        OAuthCallbackHandler.event.wait(timeout=self.callback_wait_seconds)
        server.server_close()

        result = OAuthCallbackHandler.result
        if result.error:
            raise RuntimeError(f"Spotify auth error: {result.error}")

        if not result.code:
            self.logger.info("No callback received within %ss.", self.callback_wait_seconds)
            self.logger.info("Paste the full redirected URL or raw code value now.")
            pasted = input("Authorization response: ").strip()
            if pasted.startswith("http"):
                q = urllib.parse.parse_qs(urllib.parse.urlparse(pasted).query)
                result.code = (q.get("code") or [None])[0]
                result.state = (q.get("state") or [None])[0]
            else:
                result.code = pasted

        if not result.code:
            raise RuntimeError("Authorization code was not provided")
        if result.state and result.state != state:
            raise RuntimeError("Authorization state mismatch")

        data = {
            "grant_type": "authorization_code",
            "code": result.code,
            "redirect_uri": self.redirect_uri,
            "client_id": self.client_id,
            "code_verifier": code_verifier,
        }
        if self.client_secret:
            data["client_secret"] = self.client_secret

        token_resp = requests.post(
            f"{SPOTIFY_ACCOUNTS_BASE}/api/token",
            data=data,
            timeout=REQUEST_TIMEOUT,
        )
        if token_resp.status_code != 200:
            guidance = ""
            if "invalid redirect uri" in token_resp.text.lower():
                guidance = (
                    " The redirect URI must exactly match one configured in the Spotify app settings."
                )
            raise RuntimeError(
                f"Token exchange failed ({token_resp.status_code}): {token_resp.text[:500]}{guidance}"
            )
        self._update_token_cache(token_resp.json())


class SpotifyClient:
    """Resilient Spotify API wrapper with centralized retry/backoff behavior."""

    def __init__(
        self,
        auth: SpotifyAuth,
        scopes: Iterable[str],
        logger: logging.Logger,
        max_retries: int = 6,
    ):
        self.auth = auth
        self.scopes = list(scopes)
        self.logger = logger
        self.max_retries = max_retries
        self.stats: Dict[str, int] = {
            "http_calls": 0,
            "http_retries": 0,
            "http_429": 0,
            "http_5xx": 0,
            "http_failures": 0,
        }
        self._stats_lock = threading.Lock()

    def _inc(self, key: str, n: int = 1) -> None:
        with self._stats_lock:
            self.stats[key] = self.stats.get(key, 0) + n

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        expected: Tuple[int, ...] = (200,),
    ) -> requests.Response:
        url = f"{SPOTIFY_API_BASE}{path}"
        attempt = 0
        while True:
            attempt += 1
            self._inc("http_calls")
            token = self.auth.access_token(self.scopes)
            headers = {"Authorization": f"Bearer {token}"}
            try:
                resp = requests.request(
                    method,
                    url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    timeout=REQUEST_TIMEOUT,
                )
            except requests.RequestException as exc:
                if attempt <= self.max_retries:
                    self._inc("http_retries")
                    delay = min(20.0, (2 ** (attempt - 1)) + random.random())
                    self.logger.warning(
                        "Request error (%s %s): %s; retrying in %.1fs",
                        method,
                        path,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                    continue
                self._inc("http_failures")
                raise

            if resp.status_code in expected:
                return resp

            if resp.status_code == 401 and attempt <= self.max_retries:
                self._inc("http_retries")
                self.logger.warning("401 from Spotify; retrying with refreshed token")
                # Force refresh by clearing expiry.
                self.auth._token_cache["expires_at"] = "1970-01-01T00:00:00+00:00"
                continue

            if resp.status_code == 429 and attempt <= self.max_retries:
                self._inc("http_retries")
                self._inc("http_429")
                retry_after = resp.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else min(30.0, 2 ** attempt)
                delay += random.uniform(0, 0.5)
                self.logger.warning(
                    "Rate-limited (%s %s). Waiting %.2fs before retry", method, path, delay
                )
                time.sleep(delay)
                continue

            if 500 <= resp.status_code < 600 and attempt <= self.max_retries:
                self._inc("http_retries")
                self._inc("http_5xx")
                delay = min(20.0, (2 ** (attempt - 1)) + random.random())
                self.logger.warning(
                    "Spotify %s on %s %s; retrying in %.1fs",
                    resp.status_code,
                    method,
                    path,
                    delay,
                )
                time.sleep(delay)
                continue

            self._inc("http_failures")
            raise RuntimeError(
                f"Spotify request failed: {method} {path} -> {resp.status_code} {resp.text[:500]}"
            )

    def get_me(self) -> Dict[str, Any]:
        return self.request("GET", "/me", expected=(200,)).json()

    def iter_saved_tracks(self) -> Iterable[Dict[str, Any]]:
        offset = 0
        limit = 50
        while True:
            resp = self.request(
                "GET",
                "/me/tracks",
                params={"limit": limit, "offset": offset},
                expected=(200,),
            )
            payload = resp.json()
            items = payload.get("items", [])
            for item in items:
                yield item
            if not items or payload.get("next") is None:
                break
            offset += limit

    def save_tracks_with_timestamps(self, timestamped_ids: List[Dict[str, str]]) -> None:
        self.request(
            "PUT",
            "/me/tracks",
            json_body={"timestamped_ids": timestamped_ids},
            expected=(200, 201, 204),
        )

    def search_tracks(self, query: str, limit: int, market: Optional[str]) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "q": query,
            "type": "track",
            "limit": limit,
        }
        if market:
            params["market"] = market
        resp = self.request("GET", "/search", params=params, expected=(200,))
        return resp.json().get("tracks", {}).get("items", [])

    def iter_user_playlists(self) -> Iterable[Dict[str, Any]]:
        offset = 0
        limit = 50
        while True:
            resp = self.request(
                "GET",
                "/me/playlists",
                params={"limit": limit, "offset": offset},
                expected=(200,),
            )
            payload = resp.json()
            items = payload.get("items", [])
            for item in items:
                yield item
            if not items or payload.get("next") is None:
                break
            offset += limit

    def create_playlist(
        self, user_id: str, name: str, description: str, public: bool = False
    ) -> Dict[str, Any]:
        return self.request(
            "POST",
            f"/users/{user_id}/playlists",
            json_body={"name": name, "description": description, "public": public},
            expected=(200, 201),
        ).json()

    def replace_playlist_items(self, playlist_id: str, uris: List[str]) -> None:
        first = uris[:100]
        self.request(
            "PUT",
            f"/playlists/{playlist_id}/tracks",
            json_body={"uris": first},
            expected=(200, 201),
        )
        for batch in chunked(uris[100:], 100):
            self.add_playlist_items(playlist_id, batch)

    def add_playlist_items(self, playlist_id: str, uris: List[str]) -> None:
        self.request(
            "POST",
            f"/playlists/{playlist_id}/tracks",
            json_body={"uris": uris},
            expected=(200, 201),
        )


def chunked(values: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(values), n):
        yield values[i : i + n]


def normalize_text(value: str) -> str:
    value = (value or "").lower()
    value = re.sub(r"\(.*?\)", " ", value)
    value = re.sub(r"\[.*?\]", " ", value)
    # Keep unicode letters/digits for non-Latin titles/artists.
    value = re.sub(r"[\W_]+", " ", value, flags=re.UNICODE)
    return re.sub(r"\s+", " ", value).strip()


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def score_candidate(source: SourceTrack, candidate: Dict[str, Any]) -> float:
    src_title = normalize_text(source.name)
    src_artist = normalize_text(source.artist)
    src_album = normalize_text(source.album)

    cand_title = normalize_text(candidate.get("name", ""))
    artists = candidate.get("artists") or []
    cand_artist = normalize_text(" ".join(a.get("name", "") for a in artists))
    cand_album = normalize_text((candidate.get("album") or {}).get("name", ""))

    title_score = similarity(src_title, cand_title)
    artist_score = similarity(src_artist, cand_artist)
    album_score = similarity(src_album, cand_album)

    duration_bonus = 0.0
    src_dur_ms = int((source.duration_seconds or 0) * 1000)
    cand_dur_ms = int(candidate.get("duration_ms") or 0)
    if src_dur_ms and cand_dur_ms:
        diff = abs(src_dur_ms - cand_dur_ms)
        if diff <= 2000:
            duration_bonus = 8.0
        elif diff <= 5000:
            duration_bonus = 5.0
        elif diff <= 10000:
            duration_bonus = 2.0
        elif diff >= 25000:
            duration_bonus = -12.0

    return (title_score * 60.0) + (artist_score * 30.0) + (album_score * 10.0) + duration_bonus


def track_lookup_key(track: SourceTrack) -> str:
    return "|".join(
        [
            normalize_text(track.name),
            normalize_text(track.artist),
            normalize_text(track.album),
            str(int(round(track.duration_seconds or 0))),
        ]
    )


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def unique_preserve_order(values: Iterable[str]) -> List[str]:
    """Return unique non-empty values preserving original order."""
    seen: set[str] = set()
    out: List[str] = []
    for value in values:
        v = value.strip()
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def query_value(raw: str) -> str:
    """Sanitize a token for Spotify search query usage."""
    return re.sub(r"\s+", " ", (raw or "").replace('"', " ")).strip()


def strip_bracketed(text: str) -> str:
    """Remove (...) and [...] fragments, then normalize whitespace."""
    text = re.sub(r"\(.*?\)", " ", text or "")
    text = re.sub(r"\[.*?\]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def build_title_variants(title: str) -> List[str]:
    """Generate practical search title variants for cross-platform mismatches."""
    base = query_value(title)
    no_brackets = query_value(strip_bracketed(base))
    no_feat = query_value(
        re.sub(r"\s+(feat\.?|ft\.?|featuring)\s+.*$", "", no_brackets, flags=re.IGNORECASE)
    )

    stem_dash = query_value(re.split(r"\s[-–:]\s", no_feat, maxsplit=1)[0]) if no_feat else ""
    stem_comma = query_value(no_feat.split(",", 1)[0]) if no_feat else ""

    return unique_preserve_order([base, no_brackets, no_feat, stem_dash, stem_comma])


def build_artist_variants(artist: str) -> List[str]:
    """Generate artist variants when source has collaborations/credits."""
    base = query_value(artist)
    first_credit = query_value(
        re.split(r"\s*(?:,|&| x | and | feat\.?|ft\.?|featuring)\s*", base, maxsplit=1, flags=re.IGNORECASE)[
            0
        ]
    )
    return unique_preserve_order([base, first_credit])


def build_track_search_queries(track: SourceTrack) -> List[str]:
    """Build staged query variants from strict to relaxed."""
    titles = build_title_variants(track.name)[:5]
    artists = build_artist_variants(track.artist)[:2]
    queries: List[str] = []

    # Stage 1: strict fielded searches.
    for title in titles[:3]:
        for artist in artists:
            if title and artist:
                queries.append(f'track:"{title}" artist:"{artist}"')

    # Stage 2: fielded title-only.
    for title in titles:
        if title:
            queries.append(f'track:"{title}"')

    # Stage 3: relaxed full-text fallback.
    for title in titles[:3]:
        for artist in artists:
            if title and artist:
                queries.append(f'"{title}" "{artist}"')
    for title in titles:
        if title:
            queries.append(f'"{title}"')

    return unique_preserve_order(queries)[:14]


def resolve_track_mapping(
    track: SourceTrack,
    client: SpotifyClient,
    market: Optional[str],
) -> MappingResult:
    queries = build_track_search_queries(track)

    best: Optional[Tuple[float, Dict[str, Any], str]] = None
    for query_index, query in enumerate(queries):
        # Pull a wider candidate window in later relaxed stages.
        limit = 8 if query_index < 6 else 15
        items = client.search_tracks(query=query, limit=limit, market=market)
        for item in items:
            score = score_candidate(track, item)
            if best is None or score > best[0]:
                best = (score, item, query)

        # Early stop once we have a high-confidence candidate.
        if best and best[0] >= 62:
            break

    if not best:
        return MappingResult(None, None, 0.0, "no-results", queries[0] if queries else None)

    score, item, query = best
    if score < 45:
        return MappingResult(None, None, score, "below-threshold", query)

    reason = "matched" if query == queries[0] else "matched-variant"
    return MappingResult(
        spotify_id=item.get("id"),
        spotify_uri=item.get("uri"),
        confidence=score,
        reason=reason,
        query=query,
    )


def as_source_track(raw: Dict[str, Any]) -> SourceTrack:
    return SourceTrack(
        persistent_id=str(raw.get("persistentId") or "").strip(),
        database_id=(int(raw["databaseId"]) if raw.get("databaseId") not in (None, "") else None),
        name=str(raw.get("name") or "").strip(),
        artist=str(raw.get("artist") or "").strip(),
        album=str(raw.get("album") or "").strip(),
        duration_seconds=(float(raw["duration"]) if raw.get("duration") not in (None, "") else None),
        date_added_iso=(str(raw.get("dateAdded")).strip() if raw.get("dateAdded") else None),
    )


def sort_tracks_by_date_added(tracks: List[SourceTrack]) -> List[SourceTrack]:
    """Sort ascending by `dateAdded`; null dates are pushed to the end."""

    def key(t: SourceTrack) -> Tuple[int, str]:
        if t.date_added_iso:
            return (0, t.date_added_iso)
        return (1, t.persistent_id)

    return sorted(tracks, key=key)


def date_for_library_insert(track: SourceTrack, fallback_base: datetime, i: int) -> str:
    if track.date_added_iso:
        return track.date_added_iso
    return (fallback_base + timedelta(seconds=i)).isoformat().replace("+00:00", "Z")


def map_tracks_with_cache(
    tracks: List[SourceTrack],
    client: SpotifyClient,
    market: Optional[str],
    cache_path: Path,
    max_workers: int,
    logger: logging.Logger,
) -> Tuple[Dict[str, MappingResult], Dict[str, Any]]:
    """Resolve Spotify IDs for source tracks with persistent on-disk cache."""
    cache: Dict[str, Any] = load_json(cache_path, default={})
    mapping_by_pid: Dict[str, MappingResult] = {}

    key_to_tracks: Dict[str, List[SourceTrack]] = {}
    for track in tracks:
        key_to_tracks.setdefault(track_lookup_key(track), []).append(track)

    unresolved_keys = [k for k in key_to_tracks if k not in cache]
    logger.info(
        "Track mapping workload: %d unique keys (%d cache hits, %d misses)",
        len(key_to_tracks),
        len(key_to_tracks) - len(unresolved_keys),
        len(unresolved_keys),
    )

    if unresolved_keys:
        ensure_dir(cache_path.parent)

        def worker(key: str) -> Tuple[str, Dict[str, Any]]:
            representative = key_to_tracks[key][0]
            result = resolve_track_mapping(representative, client=client, market=market)
            payload = dataclasses.asdict(result)
            payload["_updated_at"] = utc_now().isoformat()
            return key, payload

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key = {executor.submit(worker, key): key for key in unresolved_keys}
            pending = set(future_to_key.keys())
            done_count = 0
            total = len(unresolved_keys)
            started = time.perf_counter()

            while pending:
                completed, pending = wait(
                    pending,
                    timeout=HEARTBEAT_SECONDS,
                    return_when=FIRST_COMPLETED,
                )
                if not completed:
                    elapsed = time.perf_counter() - started
                    logger.info(
                        "Track mapping in progress: %d/%d resolved (%.1f%%, %.0fs elapsed)",
                        done_count,
                        total,
                        (done_count / total * 100.0) if total else 100.0,
                        elapsed,
                    )
                    continue

                for future in completed:
                    key = future_to_key[future]
                    try:
                        cache_key, payload = future.result()
                        cache[cache_key] = payload
                    except Exception as exc:
                        logger.warning("Mapping failed for key=%s: %s", key, exc)
                        cache[key] = {
                            "spotify_id": None,
                            "spotify_uri": None,
                            "confidence": 0.0,
                            "reason": f"error:{exc}",
                            "query": None,
                            "_updated_at": utc_now().isoformat(),
                        }
                    done_count += 1
                    if done_count % 25 == 0 or done_count == total:
                        logger.info(
                            "Mapped %d/%d unresolved lookup keys (%.1f%%)",
                            done_count,
                            total,
                            (done_count / total * 100.0) if total else 100.0,
                        )

        save_json(cache_path, cache)

    for key, grouped_tracks in key_to_tracks.items():
        payload = cache.get(key) or {}
        result = MappingResult(
            spotify_id=payload.get("spotify_id"),
            spotify_uri=payload.get("spotify_uri"),
            confidence=float(payload.get("confidence") or 0.0),
            reason=str(payload.get("reason") or "unknown"),
            query=payload.get("query"),
        )
        for track in grouped_tracks:
            mapping_by_pid[track.persistent_id] = result

    stats = {
        "unique_lookup_keys": len(key_to_tracks),
        "cache_entries": len(cache),
        "cache_miss_count": len(unresolved_keys),
    }
    return mapping_by_pid, stats


def sync_library(
    source_tracks: List[SourceTrack],
    mapping_by_pid: Dict[str, MappingResult],
    client: SpotifyClient,
    dry_run: bool,
    logger: logging.Logger,
) -> Dict[str, Any]:
    logger.info("Loading existing Spotify saved tracks")
    existing_ids: set[str] = set()
    scanned = 0
    last_progress = time.perf_counter()
    for item in client.iter_saved_tracks():
        track = item.get("track") or {}
        tid = track.get("id")
        if tid:
            existing_ids.add(tid)
        scanned += 1
        now = time.perf_counter()
        if scanned % 500 == 0 or (now - last_progress) >= HEARTBEAT_SECONDS:
            logger.info("Spotify library scan progress: %d tracks inspected", scanned)
            last_progress = now
    logger.info("Existing Spotify library size: %d", len(existing_ids))

    ordered = sort_tracks_by_date_added(source_tracks)
    to_add: List[Tuple[SourceTrack, str]] = []
    unmatched = 0
    already_present = 0

    for track in ordered:
        mapping = mapping_by_pid.get(track.persistent_id)
        if not mapping or not mapping.spotify_id:
            unmatched += 1
            continue
        if mapping.spotify_id in existing_ids:
            already_present += 1
            continue
        to_add.append((track, mapping.spotify_id))

    logger.info(
        "Library sync plan: %d to add, %d already present, %d unmatched",
        len(to_add),
        already_present,
        unmatched,
    )

    inserted = 0
    fallback_base = utc_now()
    total_batches = (len(to_add) + 49) // 50 if to_add else 0
    for batch_index, batch in enumerate(chunked(to_add, 50), start=1):
        payload = [
            {
                "id": spotify_id,
                "added_at": date_for_library_insert(track, fallback_base, i),
            }
            for i, (track, spotify_id) in enumerate(batch)
        ]
        if dry_run:
            logger.info(
                "[dry-run] Would add library batch %d/%d (%d tracks)",
                batch_index,
                total_batches,
                len(batch),
            )
        else:
            client.save_tracks_with_timestamps(payload)
            inserted += len(batch)
            logger.info(
                "Added library batch %d/%d (%d tracks)",
                batch_index,
                total_batches,
                len(batch),
            )

    if dry_run:
        inserted = len(to_add)

    return {
        "source_track_count": len(source_tracks),
        "planned_additions": len(to_add),
        "inserted": inserted,
        "already_present": already_present,
        "unmatched": unmatched,
    }


def unique_playlist_name(base_name: str, existing_names: set[str]) -> str:
    if base_name not in existing_names:
        existing_names.add(base_name)
        return base_name
    i = 2
    while True:
        candidate = f"{base_name} ({i})"
        if candidate not in existing_names:
            existing_names.add(candidate)
            return candidate
        i += 1


def sync_playlists(
    source_playlists: List[Dict[str, Any]],
    library_tracks_by_pid: Dict[str, SourceTrack],
    mapping_by_pid: Dict[str, MappingResult],
    client: SpotifyClient,
    user_id: str,
    dry_run: bool,
    strategy: str,
    market: Optional[str],
    logger: logging.Logger,
) -> Dict[str, Any]:
    """Sync playlists with ordered content; supports several merge strategies."""
    existing = list(client.iter_user_playlists())
    existing_by_name: Dict[str, List[Dict[str, Any]]] = {}
    existing_names: set[str] = set()
    for p in existing:
        name = p.get("name") or ""
        existing_names.add(name)
        existing_by_name.setdefault(name, []).append(p)

    created = 0
    skipped = 0
    replaced = 0
    appended = 0
    playlist_reports: List[Dict[str, Any]] = []
    unmatched_tracks_total = 0

    total_playlists = len(source_playlists)
    for playlist_index, p in enumerate(source_playlists, start=1):
        name = str(p.get("name") or "").strip()
        if not name:
            continue
        logger.info(
            "Playlist %d/%d: '%s' (%d source tracks)",
            playlist_index,
            total_playlists,
            name,
            len(p.get("tracks") or []),
        )

        uris: List[str] = []
        unmatched_here = 0

        playlist_tracks = p.get("tracks") or []
        for track_index, raw in enumerate(playlist_tracks, start=1):
            pid = str(raw.get("persistentId") or "").strip()
            mapping = mapping_by_pid.get(pid)
            if mapping and mapping.spotify_uri:
                uris.append(mapping.spotify_uri)
                continue

            # Fallback: track appears only in playlist export and not in library map.
            fallback_track = SourceTrack(
                persistent_id=pid or f"playlist::{name}::{len(uris)}",
                database_id=(int(raw["databaseId"]) if raw.get("databaseId") not in (None, "") else None),
                name=str(raw.get("name") or "").strip(),
                artist=str(raw.get("artist") or "").strip(),
                album=str(raw.get("album") or "").strip(),
                duration_seconds=(float(raw["duration"]) if raw.get("duration") not in (None, "") else None),
                date_added_iso=(str(raw.get("dateAdded")).strip() if raw.get("dateAdded") else None),
            )
            if not fallback_track.name:
                unmatched_here += 1
                continue
            resolved = resolve_track_mapping(fallback_track, client=client, market=market)
            if resolved.spotify_uri:
                uris.append(resolved.spotify_uri)
            else:
                unmatched_here += 1
            if track_index % 100 == 0:
                logger.info(
                    "Playlist '%s' mapping progress: %d/%d tracks processed",
                    name,
                    track_index,
                    len(playlist_tracks),
                )

        unmatched_tracks_total += unmatched_here

        if not uris:
            skipped += 1
            playlist_reports.append(
                {
                    "name": name,
                    "status": "skipped-no-mapped-tracks",
                    "source_tracks": len(p.get("tracks") or []),
                    "mapped_tracks": 0,
                    "unmatched_tracks": unmatched_here,
                }
            )
            continue

        existing_candidates = existing_by_name.get(name, [])
        playlist_id: Optional[str] = None
        action = ""

        if strategy == "create":
            final_name = unique_playlist_name(name, existing_names)
            action = "create"
            if not dry_run:
                created_playlist = client.create_playlist(
                    user_id=user_id,
                    name=final_name,
                    description="Imported from Music.app by am2sp",
                    public=False,
                )
                playlist_id = created_playlist["id"]
            created += 1

        elif strategy == "create-missing":
            if existing_candidates:
                skipped += 1
                playlist_reports.append(
                    {
                        "name": name,
                        "status": "skipped-existing",
                        "source_tracks": len(p.get("tracks") or []),
                        "mapped_tracks": len(uris),
                        "unmatched_tracks": unmatched_here,
                    }
                )
                continue
            action = "create"
            if not dry_run:
                created_playlist = client.create_playlist(
                    user_id=user_id,
                    name=name,
                    description="Imported from Music.app by am2sp",
                    public=False,
                )
                playlist_id = created_playlist["id"]
            created += 1

        elif strategy in ("append", "replace"):
            if existing_candidates:
                playlist_id = existing_candidates[0].get("id")
            else:
                action = "create"
                if not dry_run:
                    created_playlist = client.create_playlist(
                        user_id=user_id,
                        name=name,
                        description="Imported from Music.app by am2sp",
                        public=False,
                    )
                    playlist_id = created_playlist["id"]
                created += 1

            if strategy == "replace":
                action = "replace"
                if dry_run:
                    logger.info("[dry-run] Would replace playlist '%s' with %d tracks", name, len(uris))
                else:
                    assert playlist_id
                    client.replace_playlist_items(playlist_id, uris)
                replaced += 1
            else:
                action = "append"
                if dry_run:
                    logger.info("[dry-run] Would append %d tracks to playlist '%s'", len(uris), name)
                else:
                    assert playlist_id
                    for batch in chunked(uris, 100):
                        client.add_playlist_items(playlist_id, batch)
                appended += 1

        else:
            raise ValueError(f"Unsupported playlist strategy: {strategy}")

        if action == "create":
            if dry_run:
                logger.info("[dry-run] Would create playlist '%s' with %d tracks", name, len(uris))
            else:
                assert playlist_id
                for batch in chunked(uris, 100):
                    client.add_playlist_items(playlist_id, batch)

        playlist_reports.append(
            {
                "name": name,
                "status": action or "processed",
                "source_tracks": len(p.get("tracks") or []),
                "mapped_tracks": len(uris),
                "unmatched_tracks": unmatched_here,
            }
        )

    return {
        "source_playlist_count": len(source_playlists),
        "created": created,
        "skipped": skipped,
        "replaced": replaced,
        "appended": appended,
        "unmatched_tracks_total": unmatched_tracks_total,
        "playlists": playlist_reports,
    }


def collect_source_tracks_and_playlists(raw: Dict[str, Any]) -> Tuple[List[SourceTrack], List[Dict[str, Any]]]:
    tracks = [as_source_track(t) for t in (raw.get("library") or [])]
    tracks = [t for t in tracks if t.persistent_id and t.name]
    playlists = raw.get("playlists") or []
    return tracks, playlists


def extract_command(args: argparse.Namespace) -> int:
    log_file = Path(args.log_file)
    logger = configure_logging(log_file=log_file, verbose=args.verbose)

    payload = run_music_jxa(
        {
            "include_smart_playlists": args.include_smart_playlists,
            "skip_empty_playlists": not args.include_empty_playlists,
            "limit_tracks": args.limit_tracks,
            "limit_playlists": args.limit_playlists,
        },
        logger=logger,
    )

    out_file = Path(args.output)
    ensure_dir(out_file.parent)
    out_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    logger.info(
        "Extracted %d library tracks and %d playlists -> %s",
        payload.get("libraryTrackCount", 0),
        payload.get("playlistCount", 0),
        out_file,
    )
    return 0


def sync_command(args: argparse.Namespace) -> int:
    started = time.perf_counter()
    timestamp = utc_now().strftime("%Y%m%d-%H%M%S")

    state_dir = Path(args.state_dir)
    ensure_dir(state_dir)
    ensure_dir(state_dir / "cache")
    ensure_dir(state_dir / "logs")
    ensure_dir(state_dir / "reports")

    log_file = Path(args.log_file or (state_dir / "logs" / f"sync-{timestamp}.log"))
    logger = configure_logging(log_file=log_file, verbose=args.verbose)

    report_file = Path(args.report_file or (state_dir / "reports" / f"report-{timestamp}.json"))
    cache_file = state_dir / "cache" / "track_search_cache.json"
    token_file = state_dir / "spotify_tokens.json"

    env_values = read_env_file(Path(args.env_file))
    client_id = args.spotify_client_id or env_values.get("SPOTIFY_CLIENT_ID")
    client_secret = args.spotify_client_secret or env_values.get("SPOTIFY_SECRET")
    redirect_uri = (
        args.spotify_redirect_uri
        or env_values.get("SPOTIFY_REDIRECT_URI")
        or DEFAULT_SPOTIFY_REDIRECT_URI
    )

    if not client_id:
        raise RuntimeError(
            "Spotify client ID missing. Set SPOTIFY_CLIENT_ID in .env or pass --spotify-client-id"
        )

    logger.info("Starting sync (dry_run=%s)", args.dry_run)
    logger.info("Spotify OAuth redirect URI: %s", redirect_uri)
    logger.info(
        "Phase 1/5: Extracting source data from Music.app (this can take a while for large libraries)"
    )

    extracted = run_music_jxa(
        {
            "include_smart_playlists": args.include_smart_playlists,
            "skip_empty_playlists": not args.include_empty_playlists,
            "limit_tracks": args.limit_tracks,
            "limit_playlists": args.limit_playlists,
        },
        logger=logger,
    )

    source_tracks, source_playlists = collect_source_tracks_and_playlists(extracted)
    library_by_pid = {t.persistent_id: t for t in source_tracks}
    logger.info(
        "Source extracted: %d tracks, %d playlists",
        len(source_tracks),
        len(source_playlists),
    )

    scopes = [
        "user-library-read",
        "user-library-modify",
        "playlist-read-private",
        "playlist-modify-public",
        "playlist-modify-private",
    ]

    auth = SpotifyAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        token_cache_path=token_file,
        no_browser=args.no_browser,
        callback_wait_seconds=args.oauth_callback_wait_seconds,
        logger=logger,
    )
    client = SpotifyClient(auth=auth, scopes=scopes, logger=logger)

    logger.info("Phase 2/5: Validating Spotify auth and profile")
    me = client.get_me()
    user_id = me.get("id")
    if not user_id:
        raise RuntimeError("Could not determine Spotify user ID from /me")
    market = args.search_market or me.get("country")
    logger.info("Spotify account: %s (market=%s)", user_id, market)

    logger.info("Phase 3/5: Mapping source tracks to Spotify catalog")
    mapping_by_pid, mapping_stats = map_tracks_with_cache(
        tracks=source_tracks,
        client=client,
        market=market,
        cache_path=cache_file,
        max_workers=args.max_workers,
        logger=logger,
    )

    matched = sum(1 for m in mapping_by_pid.values() if m.spotify_id)
    unmatched = len(mapping_by_pid) - matched
    logger.info("Mapping summary: matched=%d unmatched=%d", matched, unmatched)

    library_report: Dict[str, Any] = {}
    playlists_report: Dict[str, Any] = {}

    if not args.playlists_only:
        logger.info("Phase 4/5: Syncing main library in date-added order")
        library_report = sync_library(
            source_tracks=source_tracks,
            mapping_by_pid=mapping_by_pid,
            client=client,
            dry_run=args.dry_run,
            logger=logger,
        )

    if not args.library_only:
        logger.info("Phase 5/5: Syncing playlists")
        playlists_report = sync_playlists(
            source_playlists=source_playlists,
            library_tracks_by_pid=library_by_pid,
            mapping_by_pid=mapping_by_pid,
            client=client,
            user_id=user_id,
            dry_run=args.dry_run,
            strategy=args.playlist_strategy,
            market=market,
            logger=logger,
        )

    unmatched_examples = []
    for t in source_tracks:
        m = mapping_by_pid.get(t.persistent_id)
        if m and not m.spotify_id:
            unmatched_examples.append(
                {
                    "name": t.name,
                    "artist": t.artist,
                    "album": t.album,
                    "reason": m.reason,
                    "confidence": round(m.confidence, 2),
                }
            )
        if len(unmatched_examples) >= 30:
            break

    finished = utc_now()
    elapsed = time.perf_counter() - started
    report = {
        "generated_at": finished.isoformat(),
        "dry_run": args.dry_run,
        "source": {
            "library_track_count": len(source_tracks),
            "playlist_count": len(source_playlists),
        },
        "mapping": {
            "matched": matched,
            "unmatched": unmatched,
            **mapping_stats,
        },
        "library": library_report,
        "playlists": playlists_report,
        "api_stats": client.stats,
        "unmatched_examples": unmatched_examples,
        "log_file": str(log_file),
        "elapsed_seconds": round(elapsed, 2),
    }

    save_json(report_file, report)

    logger.info("Sync completed in %.1fs", elapsed)
    logger.info("Report: %s", report_file)
    logger.info("Log file: %s", log_file)

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="am2sp",
        description="Transfer Music.app library/playlists to Spotify with date-added ordering.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    extract_p = sub.add_parser("extract", help="Export library/playlists from Music.app to JSON")
    extract_p.add_argument("--output", default=".dev/exports/music-export.json")
    extract_p.add_argument("--include-smart-playlists", action="store_true")
    extract_p.add_argument("--include-empty-playlists", action="store_true")
    extract_p.add_argument("--limit-tracks", type=int, default=0)
    extract_p.add_argument("--limit-playlists", type=int, default=0)
    extract_p.add_argument("--log-file", default=".dev/logs/extract.log")
    extract_p.add_argument("--verbose", action="store_true")
    extract_p.set_defaults(func=extract_command)

    sync_p = sub.add_parser("sync", help="Run full transfer from Music.app to Spotify")
    sync_p.add_argument("--env-file", default=".env")
    sync_p.add_argument("--state-dir", default=".dev")
    sync_p.add_argument("--log-file", default="")
    sync_p.add_argument("--report-file", default="")
    sync_p.add_argument("--spotify-client-id", default="")
    sync_p.add_argument("--spotify-client-secret", default="")
    sync_p.add_argument("--spotify-redirect-uri", default="")
    sync_p.add_argument("--search-market", default="")
    sync_p.add_argument("--max-workers", type=int, default=6)
    sync_p.add_argument(
        "--playlist-strategy",
        choices=["create", "create-missing", "append", "replace"],
        default="create-missing",
    )
    sync_p.add_argument("--dry-run", action="store_true")
    sync_p.add_argument("--library-only", action="store_true")
    sync_p.add_argument("--playlists-only", action="store_true")
    sync_p.add_argument("--include-smart-playlists", action="store_true")
    sync_p.add_argument("--include-empty-playlists", action="store_true")
    sync_p.add_argument("--limit-tracks", type=int, default=0)
    sync_p.add_argument("--limit-playlists", type=int, default=0)
    sync_p.add_argument("--no-browser", action="store_true")
    sync_p.add_argument("--oauth-callback-wait-seconds", type=int, default=25)
    sync_p.add_argument("--verbose", action="store_true")
    sync_p.set_defaults(func=sync_command)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if getattr(args, "library_only", False) and getattr(args, "playlists_only", False):
        raise SystemExit("Use only one of --library-only / --playlists-only")

    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
