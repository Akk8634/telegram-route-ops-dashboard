import os
import json
import hashlib
from datetime import datetime, timedelta
from dateutil import tz
from telethon import TelegramClient
from supabase import create_client

# ========== ENV ==========
TG_API_ID = int(os.environ.get("TG_API_ID"))
TG_API_HASH = os.environ.get("TG_API_HASH")
TG_SESSION = os.environ.get("TG_SESSION")  # first run may be empty
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# ========== CONFIG ==========
GROUPS = [
    # use exact usernames or IDs
    "group_username_1",
    "group_username_2",
    "group_username_3",
]

DASHBOARD_JSON_PATH = "dashboard/data.json"

# ========== HELPERS ==========
def last_completed_week_utc():
    """Return (week_key, start_utc, end_utc) for last Mon–Sun."""
    now = datetime.utcnow()
    # find last Monday 00:00
    weekday = now.weekday()  # Mon=0
    end = datetime(now.year, now.month, now.day) - timedelta(days=weekday+1)
    start = end - timedelta(days=6)
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
    week_key = f"{start.isocalendar().year}-W{start.isocalendar().week:02d}"
    return week_key, start, end

def hash_user(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]

def extract_route_and_bus(text: str):
    # TODO: regex + alias map
    route = None
    bus = None
    return route, bus

# ========== MAIN ==========
def main():
    week_key, start_utc, end_utc = last_completed_week_utc()

    # Supabase client
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Optional: check if this week already aggregated
    existing = sb.table("weekly_aggregate").select("week_key").eq("week_key", week_key).execute()
    if existing.data:
        print("Week already synced. Exiting safely.")
        return

    # Telegram client
    with TelegramClient(TG_SESSION or "session", TG_API_ID, TG_API_HASH) as client:
        rows = []
        for grp in GROUPS:
            for msg in client.iter_messages(grp, offset_date=end_utc):
                if not msg.date:
                    continue
                if msg.date < start_utc:
                    break
                if not msg.text:
                    continue

                route, bus = extract_route_and_bus(msg.text)
                rows.append({
                    "msg_id": msg.id,
                    "group_name": str(grp),
                    "date": msg.date.isoformat(),
                    "user_hash": hash_user(str(msg.sender_id)),
                    "text": msg.text,
                    "route_name": route,
                    "bus_no": bus,
                    "week_key": week_key
                })

        if rows:
            sb.table("messages_raw").upsert(rows).execute()

    # ====== AGGREGATION (minimal for Phase-1) ======
    agg = {}
    data = sb.table("messages_raw").select("route_name, text").eq("week_key", week_key).execute().data

    for r in data:
        route = r["route_name"] or "Unknown"
        agg.setdefault(route, {"total": 0, "issue": 0, "topics": {}})
        agg[route]["total"] += 1
        # simple rule-based issue count (Phase-1)
        txt = (r["text"] or "").lower()
        if any(k in txt for k in ["late", "delay", "breakdown", "issue", "problem"]):
            agg[route]["issue"] += 1

    # write weekly_aggregate
    for route, v in agg.items():
        sb.table("weekly_aggregate").upsert({
            "week_key": week_key,
            "route_name": route,
            "total_msgs": v["total"],
            "issue_msgs": v["issue"],
            "top_buses": [],
            "topic_breakup": {},
        }).execute()

    # ====== DASHBOARD JSON ======
    out = {
        "week": week_key,
        "last_sync": datetime.utcnow().isoformat() + "Z",
        "routes": [
            {
                "route": r,
                "total_msgs": v["total"],
                "issue_msgs": v["issue"],
                "top_buses": [],
                "topics": {}
            } for r, v in agg.items()
        ]
    }

    with open(DASHBOARD_JSON_PATH, "w") as f:
        json.dump(out, f, indent=2)

    print("Weekly sync completed.")

if __name__ == "__main__":
    main()
