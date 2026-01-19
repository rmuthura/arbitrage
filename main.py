import os, time, csv, requests, math, re, warnings
from collections import defaultdict
from dotenv import load_dotenv
import urllib.parse as _urlparse

try:
    import urllib3
    from urllib3.exceptions import NotOpenSSLWarning
    warnings.filterwarnings("ignore", category=NotOpenSSLWarning)
except Exception:
    pass

BASE         = "https://api.the-odds-api.com/v4"
REGIONS      = "us,us2"
FORMAT       = "american"
BOOKS        = "betmgm,betrivers,draftkings,hardrockbet,fanduel"
TIMEOUT      = 30
BACKOFF_SEC  = 2

BANKROLL     = 100
MARKET_CHUNK = 25
SAVE_CSV     = True
CSV_PATH     = "two_way_arbs_upcoming_american.csv"

SPORTS = ["americanfootball_nfl", "baseball_mlb"]

# Console: show near-arbs (CSV still keeps ONLY ARBs)
NEAR_ARB_MAX_S = 1.02  # treat 1%–2% overround as “near”; console only

# Limit events if needed
MAX_EVENTS_PER_SPORT = None

# Discovery lookahead window (days ahead)
DAYS_FROM = 7

# Debug toggles
PRINT_SEEN_MARKET_KEYS = 0      # keep compact console
DEBUG_TOTAL_FIELD_GOALS = False # keep compact console

load_dotenv()
API_KEY = os.environ.get("ODDS_API_KEY", "")
if not API_KEY.strip():
    raise SystemExit("Missing ODDS_API_KEY in environment.")

# ================== HTTP helpers ==================
def _params(extra=None, *, include_books=True):
    p = dict(
        apiKey=API_KEY,
        regions=REGIONS,
        oddsFormat=FORMAT,
        dateFormat="iso",
        includeLinks="true",   # request deep links
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
            time.sleep(BACKOFF_SEC)
            continue
        r.raise_for_status()
        return r

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

    # BetMGM often returns sports.{state}.betmgm.com
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

# ================== API wrappers (robust discovery) ==================
def _extract_events_from_odds(js, sport_key):
    out = []
    for g in js or []:
        eid = g.get("id")
        if not eid:
            continue
        out.append({
            "id": eid,
            "commence_time": g.get("commence_time"),
            "home": g.get("home_team"),
            "away": g.get("away_team"),
            "sport_key": sport_key,
        })
    return out

def _extract_events_from_events(js, sport_key):
    out = []
    for g in js or []:
        eid = g.get("id")
        if not eid:
            continue
        out.append({
            "id": eid,
            "commence_time": g.get("commence_time"),
            "home": g.get("home_team"),
            "away": g.get("away_team"),
            "sport_key": sport_key,
        })
    return out

def list_events_any_book(sport_key):
    """
    Discover upcoming events.
      1) /odds with daysFrom (no bookmaker filter)
      2) Fallback: /events (schedule) with daysFrom
      3) Fallback: relax regions
    """
    # Try /odds first (no bookmaker restriction)
    try:
        url = f"{BASE}/sports/{sport_key}/odds"
        r = _get(url, {**_params(include_books=False), "daysFrom": DAYS_FROM})
        events = _extract_events_from_odds(r.json(), sport_key)
        if events:
            return events
    except Exception:
        pass

    # Fallback: /events schedule
    try:
        url = f"{BASE}/sports/{sport_key}/events"
        r = _get(url, {"apiKey": API_KEY, "dateFormat": "iso", "daysFrom": DAYS_FROM})
        events = _extract_events_from_events(r.json(), sport_key)
        if events:
            return events
    except Exception:
        pass

    # Last resort: relax regions entirely
    try:
        url = f"{BASE}/sports/{sport_key}/odds"
        r = _get(url, {"apiKey": API_KEY, "dateFormat": "iso", "oddsFormat": FORMAT, "daysFrom": DAYS_FROM})
        events = _extract_events_from_odds(r.json(), sport_key)
        return events
    except Exception:
        return []

def fetch_event_markets(sport_key, event_id):
    """Markets available for an event (no prices), using your BOOKS filter."""
    url = f"{BASE}/sports/{sport_key}/events/{event_id}/markets"
    r = _get(url, _params())
    return r.json().get("bookmakers", [])

def fetch_event_odds_for_keys(sport_key, event_id, market_keys):
    """Odds for specified market keys, using your BOOKS filter."""
    url = f"{BASE}/sports/{sport_key}/events/{event_id}/odds"
    r = _get(url, _params({"markets": ",".join(market_keys)}))
    return r.json().get("bookmakers", [])

# ================== Odds helpers ==================
def am_to_dec(am):
    a = float(am)
    return 1.0 + (a / 100.0) if a > 0 else 1.0 + (100.0 / abs(a))

def fmt_am(am):
    a = int(round(float(am)))
    return f"+{a}" if a > 0 else f"{a}"

def fmt_point(p):
    if p is None:
        return ""
    if abs(p - int(p)) < 1e-9:
        p = int(p)
    sign = "+" if float(p) > 0 else ""
    return f"{sign}{p}"

def is_spread_key(k: str) -> bool:
    return k is not None and "spread" in k

# ---------- helpers for sign-safe selection / fallback ----------
def _sign_point(v):
    try:
        x = float(v)
        return -1 if x < 0 else (1 if x > 0 else 0)
    except Exception:
        return 0

def _majority_sign(lines):
    tally = {-1: 0, 1: 0}
    for _, _, _, pt, _ in lines:
        s = _sign_point(pt)
        if s in tally:
            tally[s] += 1
    return -1 if tally[-1] > tally[1] else 1

def force_opposites(a_pt, b_pt):
    def to_float(x):
        try:
            return float(x)
        except Exception:
            return None
    af = to_float(a_pt); bf = to_float(b_pt)
    mag = None
    for v in (af, bf):
        if v is not None and math.isfinite(v) and abs(v) > 0:
            mag = abs(v); break
    if mag is None:
        mag = 1.0
    if af is None and bf is None:
        return (-mag, +mag)
    if af is None and bf is not None:
        return (-abs(bf), +abs(bf))
    if bf is None and af is not None:
        return (-abs(af), +abs(af))
    if not (af * bf < 0):
        return (-mag, +mag)
    return (af, bf)

def compose_bet_name(mkey, outcome_name, side_pt, shared_pt):
    if is_spread_key(mkey) and side_pt is not None:
        return f'{outcome_name} {fmt_point(side_pt)}'
    elif shared_pt is not None:
        return f'{outcome_name} {fmt_point(shared_pt)}'
    else:
        return outcome_name

# ================== Robust point + label parsing for totals/props ==================
def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None

_POINT_RE = re.compile(r"(-?\d+(?:\.\d+)?)")

def _parse_point_from_text(*texts):
    """Extract a numeric like 3.5 from name/description when point is missing."""
    for t in texts:
        if not t:
            continue
        m = _POINT_RE.search(str(t))
        if m:
            f = _to_float(m.group(1))
            if f is not None and math.isfinite(f):
                return f
    return None

def _norm_outcome_label(s: str) -> str:
    """'Over 3.5' -> 'over', 'Under 3.5' -> 'under', else lowercase trimmed."""
    x = (s or "").strip().lower()
    if x.startswith("over"):
        return "over"
    if x.startswith("under"):
        return "under"
    return x

def _get_outcome_point(mkey: str, outcome: dict):
    """
    For spreads, rely on structured 'point'.
    For totals/props, if 'point' is missing, parse from text.
    """
    if is_spread_key(mkey):
        return _to_float(outcome.get("point"))
    p = _to_float(outcome.get("point"))
    if p is not None and math.isfinite(p):
        return p
    return _parse_point_from_text(outcome.get("name"), outcome.get("description"))

# ================== (No whitelist) accept every market key ==================
def _market_allowed(key: str) -> bool:
    return bool(key)

# ================== Main scan (all upcoming) ==================
def classify_two_way(best_a, best_b):
    if not best_a or not best_b:
        return (None, 0, 0, (0,0), 0, 0)
    oa_am, ob_am = float(best_a[0]), float(best_b[0])
    if oa_am == 0 or ob_am == 0:
        return (None, 0, 0, (0,0), 0, 0)
    oa = am_to_dec(oa_am); ob = am_to_dec(ob_am)
    S = (1.0/oa) + (1.0/ob)
    roi = (1.0 / S) - 1.0
    if S < 1.0:
        status = "ARB"
    elif S <= NEAR_ARB_MAX_S:
        status = "NEAR"
    else:
        return (None, 0, S, (0,0), 0, 0)
    stake_a = BANKROLL / (oa * S)
    stake_b = BANKROLL / (ob * S)
    payout  = BANKROLL / S
    profit  = payout - BANKROLL
    return (status, roi, S, (stake_a, stake_b), payout, profit)

def scan_all_two_way_arbs_upcoming():
    csv_rows = []
    total_arbs = 0
    total_near = 0
    total_events_scanned = 0

    for sport_key in SPORTS:
        try:
            events = list_events_any_book(sport_key)
        except Exception:
            continue
        if not events:
            continue
        if MAX_EVENTS_PER_SPORT:
            events = events[:MAX_EVENTS_PER_SPORT]

        for ev in events:
            total_events_scanned += 1
            eid = ev["id"]

            # 1) discover ALL market keys for this event
            try:
                bms_markets = fetch_event_markets(sport_key, eid)
            except Exception:
                continue

            mk_set = set()
            for bm in bms_markets:
                for m in bm.get("markets", []):
                    k = m.get("key")
                    if _market_allowed(k):
                        mk_set.add(k)
            if not mk_set:
                continue

            if PRINT_SEEN_MARKET_KEYS:
                sample = sorted(mk_set)[:PRINT_SEEN_MARKET_KEYS]
                extra = max(0, len(mk_set) - len(sample))
                print(f"  · found {len(mk_set)} market keys (showing {len(sample)}): {', '.join(sample)}" + (f" … +{extra} more" if extra else ""))

            # 2) fetch odds in chunks across ALL those markets
            merged = defaultdict(lambda: {"a":[], "b":[], "labels":None, "mkey":None, "point":None})
            def add_line(gk, labels, mkey, group_point, side_label, price_am, book, outcome_point, link):
                a_label, b_label = labels
                bucket = "a" if side_label == a_label else "b"
                merged[gk][bucket].append((float(price_am), book, side_label, outcome_point, link))
                merged[gk]["labels"] = labels
                merged[gk]["mkey"]   = mkey
                merged[gk]["point"]  = group_point  # abs spread or shared total

            mk_list = sorted(mk_set)
            for i in range(0, len(mk_list), MARKET_CHUNK):
                chunk = mk_list[i:i+MARKET_CHUNK]
                try:
                    bms_odds = fetch_event_odds_for_keys(sport_key, eid, chunk)
                except Exception:
                    continue
                for bm in bms_odds:
                    book = (bm.get("title") or bm.get("key") or "UNKNOWN").strip()
                    book_link = bm.get("link")
                    for m in bm.get("markets", []):
                        mkey = m.get("key")
                        outs = m.get("outcomes") or []
                        if not outs or len(outs) != 2:
                            continue

                        # Robust points for each outcome
                        out_pts = [_get_outcome_point(mkey, o) for o in outs]

                        # Grouping point:
                        if is_spread_key(mkey):
                            try:
                                p1, p2 = out_pts
                            except Exception:
                                continue
                            if p1 is None or p2 is None or not (math.isfinite(p1) and math.isfinite(p2)):
                                continue
                            if not (abs(p1 + p2) < 1e-6):
                                continue
                            group_pt = abs(p1)
                        else:
                            uniq = {p for p in out_pts if p is not None and math.isfinite(p)}
                            group_pt = list(uniq)[0] if len(uniq) == 1 else None
                            if group_pt is None:
                                continue

                        labels = tuple(sorted(_norm_outcome_label(o.get("name") or "") for o in outs))
                        if not labels or any(lbl == "" for lbl in labels):
                            continue

                        market_link = m.get("link")
                        for o, o_pt in zip(outs, out_pts):
                            price = o.get("price")
                            name  = o.get("name")
                            if price is None or name is None:
                                continue
                            side_label = _norm_outcome_label(name)
                            # Use normalized, validated deep link
                            best_link = best_deep_link(book, o.get("link"), market_link, book_link)
                            add_line((mkey, group_pt, labels), labels, mkey, group_pt,
                                     side_label, price, book, o_pt, best_link)

            # 3) compute ARBs / NEAR (CSV keeps only ARBs)
            for gk, rec in merged.items():
                labels = rec["labels"]
                if not labels:
                    continue
                mkey = rec["mkey"]; group_point = rec["point"]

                if is_spread_key(mkey):
                    a_sig = _majority_sign(rec["a"])
                    b_sig = _majority_sign(rec["b"])
                    a_pool = [t for t in rec["a"] if _sign_point(t[3]) == a_sig] or rec["a"]
                    b_pool = [t for t in rec["b"] if _sign_point(t[3]) == b_sig] or rec["b"]
                else:
                    a_pool = rec["a"]; b_pool = rec["b"]

                a_best = max(a_pool, default=None, key=lambda x: x[0])
                b_best = max(b_pool, default=None, key=lambda x: x[0])

                status, roi, S, (sa, sb), payout, profit = classify_two_way(a_best, b_best)
                if not status:
                    continue

                a_am, a_book, a_name, a_pt, a_link = a_best if a_best else (None, None, None, None, None)
                b_am, b_book, b_name, b_pt, b_link = b_best if b_best else (None, None, None, None, None)

                # For spreads, ensure opposite signs; for totals/props, shared point already matched
                if is_spread_key(mkey):
                    ha, hb = force_opposites(a_pt, b_pt)
                    if not (ha * hb < 0):
                        continue
                    line_str = f"{mkey} ({fmt_point(ha)} / {fmt_point(hb)})"
                else:
                    line_str = f"{mkey}"
                    if group_point is not None:
                        line_str += f" @{fmt_point(float(group_point))}"

                a_full = compose_bet_name(mkey, a_name, a_pt, group_point)
                b_full = compose_bet_name(mkey, b_name, b_pt, group_point)

                if status == "ARB":
                    total_arbs += 1

                    # CSV row
                    if SAVE_CSV:
                        csv_rows.append({
                            "status": "ARB",
                            "sport_key": sport_key,
                            "event_time": ev["commence_time"],
                            "matchup": f"{ev['away']} @ {ev['home']}",
                            "market_key": mkey,
                            "point_A": a_pt,
                            "point_B": b_pt,
                            "shared_point": None if is_spread_key(mkey) else group_point,
                            "bet_A_name": a_name,
                            "bet_A_full": a_full,
                            "odds_A_american": fmt_am(a_am),
                            "book_A": a_book,
                            "link_A": a_link or "",
                            "stake_A": round(sa, 2),
                            "bet_B_name": b_name,
                            "bet_B_full": b_full,
                            "odds_B_american": fmt_am(b_am),
                            "book_B": b_book,
                            "link_B": b_link or "",
                            "stake_B": round(sb, 2),
                            "S_sum": round(S, 6),
                            "ROI_pct": round(roi*100, 4),
                            "payout": round(payout, 2),
                            "profit": round(profit, 2),
                        })

                    # ===== Compact one-liner console output per ARB =====
                    # ===== Compact multi-line console output per ARB (header, A, then B+ROI) =====
                    header = f"{ev['commence_time']} | {ev['away']} @ {ev['home']} | {line_str}"
                    a_seg = (
                            f"A: {a_full} {fmt_am(a_am)} @{a_book} "
                            f"stake ${sa:,.2f}" + (f" [{a_link}]" if a_link else "")
                    )
                    b_seg = (
                            f"B: {b_full} {fmt_am(b_am)} @{b_book} "
                            f"stake ${sb:,.2f}" + (f" [{b_link}]" if b_link else "")
                            + f" | ROI {(roi * 100):.2f}%"
                    )

                    print(header)
                    print(a_seg)
                    print(b_seg)
                elif status == "NEAR":
                    total_near += 1
                    # keep console clean: do not print NEAR rows

    # 4) CSV: ONLY profitable ARBs, sorted by profit desc
    if SAVE_CSV and csv_rows:
        csv_rows.sort(key=lambda r: r["profit"], reverse=True)
        fieldnames = list(csv_rows[0].keys())
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader(); w.writerows(csv_rows)
        print(f"\nSaved {len(csv_rows)} profitable ARBs → {CSV_PATH}")

    print(f"Done. Scanned {total_events_scanned} events. Found {total_arbs} ARBs (and {total_near} near-arbs not shown).")

if __name__ == "__main__":
    scan_all_two_way_arbs_upcoming()
