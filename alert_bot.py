import os, time, json, smtplib, requests, pandas as pd
from email.mime.text import MIMEText
from email.utils import formatdate
from dotenv import load_dotenv

# ---------- Config ----------
BASE       = "https://api.the-odds-api.com/v4"
SPORTS     = ["baseball_mlb", "americanfootball_nfl"]
REGIONS    = "us"
BOOKS      = "draftkings,fanduel"
FORMAT     = "american"
POLL_SECONDS = 30                     # how often to check
NEAR_ARB_MAX_SUM = 1.01              # alert if sum_probs <= this (<=1.0 is pure arb)
CACHE_PATH = "alert_cache.json"      # to avoid duplicate emails

load_dotenv()
API_KEY   = os.environ["ODDS_API_KEY"]
SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.environ["SMTP_PORT"])
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASS = os.environ["SMTP_PASS"]
EMAIL_FROM = os.environ["EMAIL_FROM"]
EMAIL_TO   = os.environ["EMAIL_TO"]

# ---------- Email ----------
def send_email(subject: str, body: str):
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Date"] = formatdate(localtime=True)
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())

# ---------- Odds helpers ----------
def discover_markets(sport_key):
    url = f"{BASE}/sports/{sport_key}/odds"
    params = dict(apiKey=API_KEY, regions=REGIONS, bookmakers=BOOKS, oddsFormat=FORMAT, dateFormat="iso")
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    mk = set()
    for g in r.json():
        for bm in g.get("bookmakers", []):
            for m in bm.get("markets", []):
                mk.add(m["key"])
    return sorted(mk)

def get_odds(sport_key, markets_csv):
    url = f"{BASE}/sports/{sport_key}/odds"
    params = dict(apiKey=API_KEY, regions=REGIONS, bookmakers=BOOKS,
                  markets=markets_csv, oddsFormat=FORMAT, dateFormat="iso")
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json(), r.headers.get("x-requests-remaining")

def american_to_prob(odds):
    if odds is None: return None
    return 100/(odds+100) if odds > 0 else (-odds)/(-odds+100)

def flatten(records):
    rows=[]
    for g in records:
        gid=g.get("id"); t=g.get("commence_time"); home=g.get("home_team"); away=g.get("away_team")
        for bm in g.get("bookmakers", []):
            book=bm.get("title")
            for m in bm.get("markets", []):
                key=m.get("key")
                for o in m.get("outcomes", []):
                    rows.append({
                        "game_id": gid, "time": t, "home": home, "away": away,
                        "book": book, "market": key, "name": o.get("name"),
                        "price": o.get("price"), "point": o.get("point")
                    })
    return pd.DataFrame(rows)

def arb_two_way(p_a, p_b):
    if p_a is None or p_b is None: return (False, None, None)
    s = p_a + p_b
    return (s < 1.0, s, 1.0 - s)

# ---------- Scanners ----------
def scan_h2h(df):
    out, near=[], []
    h = df[df.market=="h2h"]
    if h.empty: return out, near
    for gid, grp in h.groupby("game_id"):
        home = grp["home"].iloc[0]; away = grp["away"].iloc[0]; t = grp["time"].iloc[0]
        mp = {(r["name"], r["book"]): r["price"] for _,r in grp.iterrows()}
        pairs = [
            (f"{home} DK", mp.get((home,"DraftKings")), f"{away} FD", mp.get((away,"FanDuel"))),
            (f"{home} FD", mp.get((home,"FanDuel")),   f"{away} DK", mp.get((away,"DraftKings"))),
        ]
        for la, pa, lb, pb in pairs:
            ok, s, margin = arb_two_way(american_to_prob(pa), american_to_prob(pb))
            if s is None: continue
            rec = dict(kind="2way", type="H2H", gid=gid, time=t, matchup=f"{away} @ {home}",
                       legA=f"{la} {pa}", legB=f"{lb} {pb}", sum_probs=round(s,4),
                       margin=None if margin is None else round(margin,4))
            (out if ok else near).append(rec)
    return out, [r for r in near if r["sum_probs"] <= NEAR_ARB_MAX_SUM]

def scan_spreads(df):
    out, near=[], []
    s = df[df.market=="spreads"]
    if s.empty: return out, near
    for (gid, point), grp in s.groupby(["game_id","point"]):
        if pd.isna(point): continue
        home = grp["home"].iloc[0]; away = grp["away"].iloc[0]; t = grp["time"].iloc[0]
        mp = {(r["name"], r["book"]): r["price"] for _,r in grp.iterrows()}
        pairs = [
            (f"{home} DK {point}", mp.get((home,"DraftKings")), f"{away} FD {point}", mp.get((away,"FanDuel"))),
            (f"{home} FD {point}", mp.get((home,"FanDuel")),   f"{away} DK {point}", mp.get((away,"DraftKings"))),
        ]
        for la, pa, lb, pb in pairs:
            ok, sprob, margin = arb_two_way(american_to_prob(pa), american_to_prob(pb))
            if sprob is None: continue
            rec = dict(kind="2way", type=f"Spread {point}", gid=gid, time=t, matchup=f"{away} @ {home}",
                       legA=f"{la} {pa}", legB=f"{lb} {pb}", sum_probs=round(sprob,4),
                       margin=None if margin is None else round(margin,4))
            (out if ok else near).append(rec)
    return out, [r for r in near if r["sum_probs"] <= NEAR_ARB_MAX_SUM]

def scan_totals(df):
    out, near=[], []
    tdf = df[df.market=="totals"]
    if tdf.empty: return out, near
    for (gid, point), grp in tdf.groupby(["game_id","point"]):
        if pd.isna(point): continue
        home = grp["home"].iloc[0]; away = grp["away"].iloc[0]; t = grp["time"].iloc[0]
        mp = {(r["name"], r["book"]): r["price"] for _,r in grp.iterrows()}
        pairs = [
            (f"Over {point} DK", mp.get(("Over","DraftKings")), f"Under {point} FD", mp.get(("Under","FanDuel"))),
            (f"Over {point} FD", mp.get(("Over","FanDuel")),   f"Under {point} DK", mp.get(("Under","DraftKings"))),
        ]
        for la, pa, lb, pb in pairs:
            ok, sprob, margin = arb_two_way(american_to_prob(pa), american_to_prob(pb))
            if sprob is None: continue
            rec = dict(kind="2way", type=f"Total {point}", gid=gid, time=t, matchup=f"{away} @ {home}",
                       legA=f"{la} {pa}", legB=f"{lb} {pb}", sum_probs=round(sprob,4),
                       margin=None if margin is None else round(margin,4))
            (out if ok else near).append(rec)
    return out, [r for r in near if r["sum_probs"] <= NEAR_ARB_MAX_SUM]

def scan_generic_two_way(df):
    out, near=[], []
    cand = df[~df["market"].isin(["h2h","spreads","totals"])]
    if cand.empty: return out, near
    for (gid, market, point), grp in cand.groupby(["game_id","market","point"]):
        names = sorted(set(grp["name"]))
        if len(names) != 2: continue
        home = grp["home"].iloc[0]; away = grp["away"].iloc[0]; t = grp["time"].iloc[0]
        mp = {(r["name"], r["book"]): r["price"] for _, r in grp.iterrows()}
        a, b = names
        pairs = [
            (f"{a} DK", mp.get((a,"DraftKings")), f"{b} FD", mp.get((b,"FanDuel"))),
            (f"{a} FD", mp.get((a,"FanDuel")),   f"{b} DK", mp.get((b,"DraftKings"))),
        ]
        for la, pa, lb, pb in pairs:
            ok, sprob, margin = arb_two_way(american_to_prob(pa), american_to_prob(pb))
            if sprob is None: continue
            label = f"{market}" + ("" if pd.isna(point) else f" {point}")
            rec = dict(kind="2way", type=label, gid=gid, time=t, matchup=f"{away} @ {home}",
                       legA=f"{la} {pa}", legB=f"{lb} {pb}", sum_probs=round(sprob,4),
                       margin=None if margin is None else round(margin,4))
            (out if ok else near).append(rec)
    return out, [r for r in near if r["sum_probs"] <= NEAR_ARB_MAX_SUM]

def scan_n_way(df):
    out, near=[], []
    cand = df[~df["market"].isin(["h2h","spreads","totals"])]
    if cand.empty: return out, near
    for (gid, market, point), grp in cand.groupby(["game_id","market","point"]):
        names = sorted(set(grp["name"]))
        if len(names) < 3: continue
        by_outcome = {}
        for _, r in grp.iterrows():
            by_outcome.setdefault(r["name"], {})[r["book"]] = r["price"]
        picks = []
        total_prob = 0.0
        valid = True
        for nm, books in by_outcome.items():
            best_p = None; best_b=None; best_price=None
            for bk, price in books.items():
                p = american_to_prob(price)
                if p is None: continue
                if best_p is None or p < best_p:
                    best_p, best_b, best_price = p, bk, price
            if best_p is None:
                valid=False; break
            picks.append((nm, best_b, best_price, best_p))
            total_prob += best_p
        if not valid: continue
        home = grp["home"].iloc[0]; away = grp["away"].iloc[0]; t = grp["time"].iloc[0]
        label = f"{market}" + ("" if pd.isna(point) else f" {point}")
        rec = dict(kind="nway", type=f"{label} (N-way)", gid=gid, time=t, matchup=f"{away} @ {home}",
                   legs=[f"{nm} @ {bk} {pr}" for (nm,bk,pr,_) in picks],
                   sum_probs=round(total_prob,4), margin=round(1.0-total_prob,4))
        (out if total_prob < 1.0 else near).append(rec)
    return out, [r for r in near if r["sum_probs"] <= NEAR_ARB_MAX_SUM]

# ---------- Cache ----------
def load_cache():
    if not os.path.exists(CACHE_PATH): return set()
    with open(CACHE_PATH, "r") as f:
        try:
            return set(json.load(f))
        except Exception:
            return set()

def save_cache(keys):
    with open(CACHE_PATH, "w") as f:
        json.dump(sorted(list(keys)), f)

def key_for(rec):
    # stable identifier for dedupe
    if rec["kind"] == "nway":
        legs = "|".join(rec["legs"])
        return f"{rec['type']}::{rec['gid']}::{rec['sum_probs']}::{legs}"
    else:
        return f"{rec['type']}::{rec['gid']}::{rec['sum_probs']}::{rec['legA']}||{rec['legB']}"

# ---------- Main loop ----------
def run_once():
    alerts = []
    for sport in SPORTS:
        mk = discover_markets(sport)
        if not mk:
            continue
        markets_csv = ",".join(mk)
        records, remaining = get_odds(sport, markets_csv)
        df = flatten(records)
        if df.empty:
            continue
        a, n = scan_h2h(df);           alerts += a + n
        a, n = scan_spreads(df);       alerts += a + n
        a, n = scan_totals(df);        alerts += a + n
        a, n = scan_generic_two_way(df); alerts += a + n
        a, n = scan_n_way(df);         alerts += a + n
    return alerts

def format_alerts(alerts):
    lines = []
    for r in alerts:
        lines.append(f"[{r['type']}] {r['matchup']} @ {r['time']}")
        if r["kind"] == "nway":
            for lg in r["legs"]:
                lines.append(f"  • {lg}")
        else:
            lines.append(f"  • {r['legA']}")
            lines.append(f"  • {r['legB']}")
        lines.append(f"  sum_probs={r['sum_probs']}  margin={r.get('margin')}")
        lines.append("")
    return "\n".join(lines)

if __name__ == "__main__":
    print("Starting arbitrage watcher… (Ctrl+C to stop)")
    cache = load_cache()
    try:
        while True:
            try:
                found = run_once()
                # keep only unique new alerts
                new = []
                for r in found:
                    k = key_for(r)
                    if k not in cache:
                        cache.add(k)
                        new.append(r)
                if new:
                    subject = f"Arb/Near-Arb Found ({len(new)})"
                    body = format_alerts(sorted(new, key=lambda x: x["sum_probs"]))
                    print(body)
                    send_email(subject, body)
                else:
                    print("No new alerts.")
            except Exception as e:
                print("Error:", e)
            save_cache(cache)
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        print("Stopped.")
