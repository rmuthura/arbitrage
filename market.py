#!/usr/bin/env python3
import warnings

# --- Silence urllib3's LibreSSL warning (urllib3 v2 expects OpenSSL 1.1.1+) ---
try:
    import urllib3
    try:
        from urllib3.exceptions import NotOpenSSLWarning
        warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
    except Exception:
        warnings.filterwarnings(
            "ignore",
            message=r".*urllib3 v2 only supports OpenSSL 1\.1\.1\+.*",
        )
except Exception:
    pass

import os, time, csv, math, re, requests
from collections import defaultdict
from dotenv import load_dotenv
import urllib.parse as _urlparse  # <<< added

# ================== Config ==================
BASE         = "https://api.the-odds-api.com/v4"
REGIONS      = "us,us2"
FORMAT       = "american"
BOOKS        = "betmgm,betrivers,draftkings,hardrockbet,fanduel"
TIMEOUT      = 30
BACKOFF_SEC  = 2

SPORTS        = ["americanfootball_nfl", "baseball_mlb"]
EVENTS_SAMPLE = None     # e.g. 10 to cap events per sport; None = all discovered
BANKROLL      = 100
SAVE_CSV      = True
CSV_PATH      = "player_prop_arbs.csv"

# Keep console tight:
PRINT_NEAR    = False  # don't print near-arbs
DEBUG_SKIPS   = False  # set True if you want to see why events are skipped

# ---------- Player markets (props only) ----------
NFL_PLAYER_KEYS = [
    "player_pass_yds", "player_rush_yds", "player_reception_yds",
    "player_receptions", "player_pass_attempts", "player_rush_attempts",
    "player_field_goals", "player_pats", "player_kicking_points",
    "player_tds_over",  # scoring props may vary by book – keep if 2-way
]
MLB_PLAYER_KEYS = [
    "batter_hits", "batter_total_bases", "batter_rbis", "batter_runs_scored",
    "batter_walks", "pitcher_strikeouts", "pitcher_outs",
    "pitcher_earned_runs", "pitcher_hits_allowed",
]
PLAYER_MARKET_KEYS = set(NFL_PLAYER_KEYS + MLB_PLAYER_KEYS)

# ================== Auth ==================
load_dotenv()
API_KEY = os.environ["ODDS_API_KEY"]

# ================== HTTP helpers ==================
def _params(extra=None, *, include_books=True):
    p = dict(
        apiKey=API_KEY,
        regions=REGIONS,
        oddsFormat=FORMAT,
        dateFormat="iso",
        includeLinks="true",  # <<— request deep links
    )
    if include_books and BOOKS:
        p["bookmakers"] = BOOKS
    if extra:
        p.update(extra)
    return p

def _get(url, params):
    while True:
        r = requests.get(url, params=params, timeout=TIMEOUT)
        if r.status_code == 429:
            if DEBUG_SKIPS: print(f"[rate-limit] 429 → sleeping {BACKOFF_SEC}s")
            time.sleep(BACKOFF_SEC)
            continue
        r.raise_for_status()
        return r

# ================== API wrappers ==================
def list_events_any_book(sport_key):
    """Find upcoming events (don’t filter by BOOKS so we don’t miss games)."""
    url = f"{BASE}/sports/{sport_key}/odds"
    r = _get(url, _params(include_books=False))
    events = r.json() or []
    out = []
    for g in events:
        eid = g.get("id")
        if not eid: continue
        out.append({
            "id": eid,
            "commence_time": g.get("commence_time"),
            "home": g.get("home_team"),
            "away": g.get("away_team"),
            "sport_key": sport_key,
        })
    return out

def fetch_event_odds_for_keys(sport_key, event_id, market_keys):
    """Fetch priced odds for the given player-prop market keys (restricted to BOOKS)."""
    url = f"{BASE}/sports/{sport_key}/events/{event_id}/odds"
    r = _get(url, _params({"markets": ",".join(market_keys)}))
    js = r.json() or {}
    return js.get("bookmakers", [])

# ================== Link normalization (Option 1) ==================
def _has_placeholders(url: str) -> bool:
    return "{" in (url or "") or "}" in (url or "")

def _normalize_book_domain(book: str, url: str) -> str:
    """Fix common templated/regional sportsbook links so they open universally."""
    if not url:
        return url
    try:
        parsed = _urlparse.urlsplit(url)
    except Exception:
        return url

    host = (parsed.netloc or "").lower()
    scheme = parsed.scheme or "https"

    # BetMGM often uses sports.{state}.betmgm.com
    if "betmgm.com" in host:
        host = host.replace("sports.{state}.betmgm.com", "sports.betmgm.com")
        host = host.replace("{state}.betmgm.com", "betmgm.com")
        if host.startswith("sports..betmgm.com"):
            host = host.replace("sports..", "sports.")

    # BetRivers sometimes {state}.betrivers.com
    if "betrivers.com" in host and "{state}" in host:
        host = host.replace("{state}.betrivers.com", "www.betrivers.com")

    return _urlparse.urlunsplit((
        scheme,
        host,
        parsed.path or "",
        parsed.query or "",
        parsed.fragment or "",
    ))

def _valid_link(url: str) -> bool:
    if not url or _has_placeholders(url):
        return False
    try:
        p = _urlparse.urlsplit(url)
        return bool(p.scheme and p.netloc)
    except Exception:
        return False

def best_deep_link(book: str, outcome_link: str, market_link: str, book_link: str) -> str:
    """Pick deepest valid link; normalize known domains; fallback outcome→market→book."""
    for raw in (outcome_link, market_link, book_link):
        if not raw:
            continue
        norm = _normalize_book_domain(book or "", raw)
        if _valid_link(norm):
            return norm
    return ""

# ================== Odds & parsing ==================
def am_to_dec(am):
    a = float(am)
    return 1.0 + (a / 100.0) if a > 0 else 1.0 + (100.0 / abs(a))

def fmt_am(am):
    a = int(round(float(am)))
    return f"+{a}" if a > 0 else f"{a}"

def fmt_point(p):
    if p in (None, "", "None"): return ""
    try:
        v = float(p)
    except Exception:
        return f"{p}"
    if abs(v - int(v)) < 1e-9: v = int(v)
    return f"{v}"

OVER_ALIASES  = {"over", "o", "o."}
UNDER_ALIASES = {"under", "u", "u."}

def _norm(s): return (s or "").strip().lower()

def _looks_like_name(s):
    tokens = s.split()
    if len(tokens) == 0 or len(tokens) > 5: return False
    caps = sum(1 for t in tokens if t[:1].isupper())
    return caps >= max(1, len(tokens)//2)

TEAM_PAREN_RE = re.compile(r"\(([^)]+)\)")

def guess_team_from_strings(player, name_field, desc_field, home, away):
    hay = " ".join([player or "", name_field or "", desc_field or ""])
    m = TEAM_PAREN_RE.search(hay)
    if m: return m.group(1).strip()
    for t in (home or "", away or ""):
        if t and t.lower() in hay.lower():
            return t
    return ""

def detect_role_and_player(outcome):
    """
    Extract ('Over'/'Under', player_name, point) from typical book outcome fields.
    Works for:
      • name='Over' & description='Travis Kelce'
      • name='Travis Kelce Over'
      • name='Over 69.5', description='Travis Kelce'
    """
    name = outcome.get("name")
    desc = outcome.get("description")
    point = outcome.get("point")

    role = None
    player = None

    for val in (name, desc):
        v = _norm(val)
        if v in OVER_ALIASES: role = "Over"
        if v in UNDER_ALIASES: role = "Under"

    if role:
        other = desc if _norm(name) in OVER_ALIASES.union(UNDER_ALIASES) else name
        if other and _norm(other) not in OVER_ALIASES.union(UNDER_ALIASES):
            player = other

    if not role or not player:
        both = " ".join([t for t in [name or "", desc or ""] if t])
        tnorm = _norm(both)
        m = re.search(r"\b(over|under)\b", tnorm)
        if m:
            role = "Over" if m.group(1) in OVER_ALIASES else "Under"
            parts = re.split(r"\b(over|under)\b", both, flags=re.I)
            if len(parts) >= 3:
                left = parts[0].strip(" -–—")
                right = parts[2].strip(" -–—")
                cand = right if len(right) >= len(left) else left
                if cand and _looks_like_name(cand): player = cand

    if not player:
        for val in (name, desc):
            if val and _looks_like_name(val) and _norm(val) not in OVER_ALIASES.union(UNDER_ALIASES):
                player = val
                other = desc if val is name else name
                if other:
                    if re.search(r"\bover\b", _norm(other)): role = "Over"
                    if re.search(r"\bunder\b", _norm(other)): role = "Under"

    if not role or not player:
        return None

    return role, player.strip(), point

# ================== Core ==================
def classify_two_way(odds_over_am, odds_under_am):
    """Return (is_arb, S, roi, stake_over, stake_under, payout, profit)."""
    oa = am_to_dec(odds_over_am); ua = am_to_dec(odds_under_am)
    S = (1.0/oa) + (1.0/ua)
    if S >= 1.0:
        return (False, S, 0.0, 0.0, 0.0, 0.0, 0.0)
    payout = BANKROLL / S
    roi = (1.0 / S) - 1.0
    stake_over  = BANKROLL / (oa * S)
    stake_under = BANKROLL / (ua * S)
    profit = payout - BANKROLL
    return (True, S, roi, stake_over, stake_under, payout, profit)

def scan_player_prop_arbs():
    csv_rows = []
    total_events = 0
    total_arbs = 0

    for sport_key in SPORTS:
        events = list_events_any_book(sport_key)
        if not events:
            if DEBUG_SKIPS: print(f"(no upcoming events for {sport_key})")
            continue
        if isinstance(EVENTS_SAMPLE, int):
            events = events[:EVENTS_SAMPLE]

        wanted_keys = NFL_PLAYER_KEYS if sport_key.startswith("americanfootball") else MLB_PLAYER_KEYS

        for ev in events:
            total_events += 1
            eid, away, home, etime = ev["id"], ev["away"], ev["home"], ev["commence_time"]

            bms = fetch_event_odds_for_keys(sport_key, eid, wanted_keys)
            if not bms:
                if DEBUG_SKIPS:
                    print(f"- Skipping {away} @ {home} (no bookmakers returned props for requested keys)")
                continue

            # (market_key, player_name, exact_line) -> best Over & best Under
            # tuple = (price_am, book, point, outcome_name, outcome_description, link)
            buckets = defaultdict(lambda: {"Over": None, "Under": None})
            props_seen = 0

            for bm in bms:
                book = (bm.get("title") or bm.get("key") or "UNKNOWN").strip()
                book_link = bm.get("link")  # event-level fallback
                for m in bm.get("markets", []):
                    mkey = (m.get("key") or "").strip()
                    if mkey not in PLAYER_MARKET_KEYS:
                        continue
                    props_seen += 1
                    market_link = m.get("link")  # market-level fallback
                    outs = m.get("outcomes") or []
                    for o in outs:
                        parsed = detect_role_and_player(o)
                        if not parsed:
                            continue
                        role, player, point = parsed
                        # >>> Option 1: deepest valid, normalized link (outcome→market→book)
                        best_link = best_deep_link(book, o.get("link"), market_link, book_link)
                        k = (mkey, player, str(point))  # pair only identical lines
                        tup = (float(o.get("price")), book, point, o.get("name"), o.get("description"), best_link)
                        prev = buckets[k][role]
                        if (prev is None) or (tup[0] > prev[0]):
                            buckets[k][role] = tup

            if props_seen == 0:
                if DEBUG_SKIPS:
                    print(f"- Skipping {away} @ {home} (books returned no matching player markets)")
                continue

            for (mkey, player, point_str), sides in buckets.items():
                over = sides["Over"]; under = sides["Under"]
                if not over or not under:
                    continue  # need both sides on the SAME line value

                is_arb, S, roi, stO, stU, payout, profit = classify_two_way(over[0], under[0])
                if not is_arb:
                    if PRINT_NEAR:
                        gap = (S - 1.0) * 100.0
                        print(f"{etime} | {away}@{home} | {mkey} | {player} @{fmt_point(point_str)} | "
                              f"Over {fmt_am(over[0])} {over[1]} | Under {fmt_am(under[0])} {under[1]} | {gap:.2f}% over")
                    continue

                total_arbs += 1
                line_txt = fmt_point(point_str)

                # ----- Compact multi-line console output per ARB (header, A, then B+ROI) -----
                header = f"{etime} | {away} @ {home} | {mkey} | {player} @{line_txt}"
                a_seg  = (
                    f"Over {fmt_am(over[0])} @{over[1]} "
                    f"stake ${stO:,.2f}" + (f" [{over[5]}]" if over[5] else "")
                )
                b_seg  = (
                    f"Under {fmt_am(under[0])} @{under[1]} "
                    f"stake ${stU:,.2f}" + (f" [{under[5]}]" if under[5] else "")
                    + f" | ROI {(roi*100):.2f}%"
                )
                print(header)
                print(a_seg)
                print(b_seg)

                if SAVE_CSV:
                    csv_rows.append({
                        "sport_key": sport_key,
                        "event_time": etime,
                        "matchup": f"{away} @ {home}",
                        "market_key": mkey,
                        "player": player,
                        "line": line_txt,
                        "over_am": fmt_am(over[0]),
                        "over_book": over[1],
                        "link_over": over[5] or "",
                        "stake_over": round(stO, 2),
                        "under_am": fmt_am(under[0]),
                        "under_book": under[1],
                        "link_under": under[5] or "",
                        "stake_under": round(stU, 2),
                        "S_sum": round(S, 6),
                        "ROI_pct": round(roi*100, 4),
                        "payout": round(payout, 2),
                        "profit": round(profit, 2),
                    })

    if SAVE_CSV and csv_rows:
        csv_rows.sort(key=lambda r: r["profit"], reverse=True)
        fields = list(csv_rows[0].keys())
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader(); w.writerows(csv_rows)
        print(f"\nSaved {len(csv_rows)} player-prop ARBs → {CSV_PATH}")

    # Summary
    print("\n=== SUMMARY (Player Props) ===")
    print(f"Events scanned: {total_events}")
    print(f"Player-prop ARBs found: {total_arbs}")
    if SAVE_CSV:
        print(f"CSV: {CSV_PATH} ({len(csv_rows)} rows)")
    print("================================\n")

if __name__ == "__main__":
    scan_player_prop_arbs()
