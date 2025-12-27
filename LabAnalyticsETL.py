# ----------------------------
# User-configurable variables
# ----------------------------
import os                                         #access CPU
from datetime import datetime, timezone           #date handling
from typing import Dict, Any, List, Optional      #type hints
import hmac                                       #Convert names into pseudonyms for pirivacy  
import hashlib                                    #lock hashes requires a key for privacy

import numpy as np
import psycopg2                                   #Postgres database access
import psycopg2.extras

from googleapiclient.discovery import build       #Google Calendar API access
from googleapiclient.errors import HttpError      #Google Calendar API error handling

from google.oauth2.credentials import Credentials    #Google Calendar API credentials
from google_auth_oauthlib.flow import InstalledAppFlow  #Google Calendar API OAuth2 flow
from google.auth.transport.requests import Request  #Google Calendar API request handling
from googleapiclient.discovery import build         #Google Calendar API access

from zoneinfo import ZoneInfo   
SEATTLE_TZ = ZoneInfo("America/Los_Angeles")


# OUTPUT DB (Postgres)
DB_HOST = "localhost"
DB_PORT = 5432 #default port for Postgres
DB_NAME = "lab_analytics"
DB_USER = "postgres"
DB_PASSWORD = "***" # <-- CHANGE ME

# Calendar extraction window (UTC ISO)
TIME_MIN_UTC = "2020-01-01T00:00:00Z"
TIME_MAX_UTC = "2025-12-31T23:59:59Z"

# Calendars to pull (resource calendars strongly preferred for privacy)
CALENDAR_IDS = [
    "dnceh1hibnlamasd3nmr955kus@group.calendar.google.com",
]

# OAuth2 credentials (for Google Calendar API)
SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]  # Read-only access to calendars
OAUTH_CLIENT_SECRET_FILE = r"C:\Users\Jacob\Dropbox\Python\Lab Analytics\secrets\client_secret.json"     # Path to the client secret JSON file
OAUTH_TOKEN_FILE = r"C:\Users\Jacob\Dropbox\Python\Lab Analytics\secrets\token.json"                    # Path to the token JSON file

# Secret for stable HMAC hashing (store in env var in real usage)
HMAC_SECRET = os.environ.get("GCAL_HMAC_SECRET", "CHANGE_ME")

# ----------------------------
# Privacy utilities
# ----------------------------
def hmac_hash(value: Optional[str], secret: str) -> Optional[str]:
    if value is None:
        return None
    v = value.strip().encode("utf-8")
    key = secret.encode("utf-8")
    return hmac.new(key, v, hashlib.sha256).hexdigest()

def safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return default

# ----------------------------
# Google Calendar extract (placeholder)
# ----------------------------
def fetch_events_for_calendar(service, calendar_id: str, time_min_utc: str, time_max_utc: str):
    events = []
    page_token = None

    while True:
        resp = service.events().list(
            calendarId=calendar_id,
            timeMin=time_min_utc,
            timeMax=time_max_utc,
            singleEvents=True,      # expands recurring events into instances
            showDeleted=True,       # lets you see cancellations (status='cancelled')
            maxResults=2500,    
            pageToken=page_token
        ).execute()

        events.extend(resp.get("items", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return events


# ----------------------------
# Transform: normalize + de-identify + features
# ----------------------------
def parse_dt(event: Dict[str, Any], key: str) -> (datetime, bool, str):
    obj = event.get(key, {})
    tz = obj.get("timeZone") or event.get("start", {}).get("timeZone") or "UTC"

    if "dateTime" in obj:
        dt = datetime.fromisoformat(obj["dateTime"].replace("Z", "+00:00"))
        dt = dt.astimezone(SEATTLE_TZ)
        return dt, False, tz

    elif "date" in obj:
        # all-day → midnight local time
        dt = datetime.fromisoformat(obj["date"]).replace(tzinfo=SEATTLE_TZ)
        return dt, True, tz

    return datetime.now(SEATTLE_TZ), False, tz


def deid_event(event: Dict[str, Any], calendar_id: str, secret: str) -> Dict[str, Any]:
    start_ts, all_day, tz = parse_dt(event, "start")
    end_ts, _, _ = parse_dt(event, "end")

    organizer_email = (event.get("organizer") or {}).get("email")
    attendees = event.get("attendees") or []
    attendee_emails = [a.get("email") for a in attendees if a.get("email")]

    title = (event.get("summary") or "").strip()

    # De-identified canonical record (keeps raw title; no description)
    out = {
        "source_event_id_hash": hmac_hash(event.get("id"), secret),
        "calendar_id_hash": hmac_hash(calendar_id, secret),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "all_day": all_day,
        "created_ts": datetime.fromisoformat(event.get("created").replace("Z", "+00:00")).astimezone(timezone.utc) if event.get("created") else None,
        "updated_ts": datetime.fromisoformat(event.get("updated").replace("Z", "+00:00")).astimezone(timezone.utc) if event.get("updated") else None,
        "status": event.get("status"),
        "organizer_hash": hmac_hash(organizer_email, secret),
        "attendee_count": safe_int(len(attendee_emails)),
        "location_hash": hmac_hash(event.get("location"), secret),
        "recurrence_flag": bool(event.get("recurrence")),
        "timezone": tz,
        "title": title,
        "ingested_ts": datetime.now(SEATTLE_TZ),
    }

    # Add title-derived features onto the row dict so load_events can copy them into event_features
    out.update(extract_title_features(title))
    return out


import re

def extract_title_features(title: str) -> dict:
    t = (title or "").strip().lower()
    if not t:
        return {
            "title_len": 0,
            "has_temp_sweep": False,
            "mentions_wavelength_lightSource": False,
        }

    return {
        "title_len": len(t),
        "has_temp_sweep": any(k in t for k in ["temp", "temperature"]),
        "mentions_wavelength_lightSource": any(k in t for k in [" nm", "/d+nm", "laser", "led", "lamp"]),
    }



def featurize(deid: Dict[str, Any]) -> Dict[str, Any]:
    start_ts = deid["start_ts"]
    end_ts = deid["end_ts"]
    created_ts = deid.get("created_ts")
    updated_ts = deid.get("updated_ts")

    duration_min = (end_ts - start_ts).total_seconds() / 60.0
    lead_time_hr = (start_ts - created_ts).total_seconds() / 3600.0 if created_ts else None
    last_change_hr = (start_ts - updated_ts).total_seconds() / 3600.0 if updated_ts else None

    weekday = start_ts.weekday()  # Monday=0
    hour = start_ts.hour
    is_weekend = weekday >= 5

    # Lab hours placeholder (UTC): 8am–6pm
    is_after_hours = (hour < 8) or (hour >= 18)

    return {
        "duration_min": float(duration_min),
        "lead_time_hr": float(lead_time_hr) if lead_time_hr is not None else None,
        "weekday": int(weekday),
        "hour_of_day": int(hour),
        "is_weekend": bool(is_weekend),
        "is_after_hours": bool(is_after_hours),
        "is_last_minute_change": bool(last_change_hr is not None and last_change_hr < 12),
        "is_recurring": bool(deid["recurrence_flag"]),
        "success_label": None,
    }


# ----------------------------
# Load: Postgres upsert
# ----------------------------
DDL = r"""
CREATE TABLE IF NOT EXISTS raw_events_deid (
  event_pk UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  source_event_id_hash TEXT UNIQUE NOT NULL,
  calendar_id_hash TEXT NOT NULL,
  start_ts TIMESTAMPTZ NOT NULL,
  end_ts TIMESTAMPTZ NOT NULL,
  all_day BOOLEAN NOT NULL,
  created_ts TIMESTAMPTZ,
  updated_ts TIMESTAMPTZ,
  status TEXT,
  organizer_hash TEXT,
  attendee_count INT,
  location_hash TEXT,
  recurrence_flag BOOLEAN,
  timezone TEXT,
  title TEXT,
  ingested_ts TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS event_features (
  event_pk UUID PRIMARY KEY,
  duration_min DOUBLE PRECISION,
  lead_time_hr DOUBLE PRECISION,
  weekday INT,
  hour_of_day INT,
  is_weekend BOOLEAN,
  is_after_hours BOOLEAN,
  is_last_minute_change BOOLEAN,
  is_recurring BOOLEAN,
  title_len INT,
  has_temp_sweep BOOLEAN,
  mentions_wavelength_lightSource BOOLEAN,
  success_label BOOLEAN
);
"""


UPSERT_RAW = r"""
INSERT INTO raw_events_deid (
  source_event_id_hash, calendar_id_hash, start_ts, end_ts, all_day,
  created_ts, updated_ts, status, organizer_hash, attendee_count, location_hash,
  recurrence_flag, timezone, title, ingested_ts
) VALUES (
  %(source_event_id_hash)s, %(calendar_id_hash)s, %(start_ts)s, %(end_ts)s, %(all_day)s,
  %(created_ts)s, %(updated_ts)s, %(status)s, %(organizer_hash)s, %(attendee_count)s, %(location_hash)s,
  %(recurrence_flag)s, %(timezone)s, %(title)s, %(ingested_ts)s
)
ON CONFLICT (source_event_id_hash) DO UPDATE SET
  start_ts = EXCLUDED.start_ts,
  end_ts = EXCLUDED.end_ts,
  all_day = EXCLUDED.all_day,
  updated_ts = EXCLUDED.updated_ts,
  status = EXCLUDED.status,
  attendee_count = EXCLUDED.attendee_count,
  recurrence_flag = EXCLUDED.recurrence_flag,
  timezone = EXCLUDED.timezone,
  title = EXCLUDED.title,
  ingested_ts = EXCLUDED.ingested_ts
RETURNING event_pk;
"""


UPSERT_FEATURES = r"""
INSERT INTO event_features (
  event_pk, duration_min, lead_time_hr, weekday, hour_of_day, is_weekend,
  is_after_hours, is_last_minute_change, is_recurring,
  title_len, has_temp_sweep, mentions_wavelength_lightSource,
  success_label
) VALUES (
  %(event_pk)s, %(duration_min)s, %(lead_time_hr)s, %(weekday)s, %(hour_of_day)s, %(is_weekend)s,
  %(is_after_hours)s, %(is_last_minute_change)s, %(is_recurring)s,
  %(title_len)s, %(has_temp_sweep)s, %(mentions_wavelength_lightSource)s,
  %(success_label)s
)
ON CONFLICT (event_pk) DO UPDATE SET
  duration_min = EXCLUDED.duration_min,
  lead_time_hr = EXCLUDED.lead_time_hr,
  weekday = EXCLUDED.weekday,
  hour_of_day = EXCLUDED.hour_of_day,
  is_weekend = EXCLUDED.is_weekend,
  is_after_hours = EXCLUDED.is_after_hours,
  is_last_minute_change = EXCLUDED.is_last_minute_change,
  is_recurring = EXCLUDED.is_recurring,
  title_len = EXCLUDED.title_len,
  has_temp_sweep = EXCLUDED.has_temp_sweep,
  mentions_wavelength_lightSource = EXCLUDED.mentions_wavelength_lightSource,
  success_label = EXCLUDED.success_label;
"""


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

def run_ddl(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

def load_events(conn, deid_rows: List[Dict[str, Any]]):
    with conn.cursor() as cur:
        for row in deid_rows:
            cur.execute(UPSERT_RAW, row)
            event_pk = cur.fetchone()[0]

            feats = featurize(row)
            feats["event_pk"] = event_pk

            # Copy title-derived features into event_features
            feats["title_len"] = row.get("title_len")
            feats["has_temp_sweep"] = row.get("has_temp_sweep")
            feats["mentions_wavelength_lightSource"] = row.get("mentions_wavelength_lightSource")

            cur.execute(UPSERT_FEATURES, feats)
    conn.commit()




def get_calendar_service_oauth():
    creds = None

    if os.path.exists(OAUTH_TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(OAUTH_TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                OAUTH_CLIENT_SECRET_FILE,
                SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open(OAUTH_TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return build("calendar", "v3", credentials=creds)


# ----------------------------
# Orchestrate
# ----------------------------
def main():
    service = get_calendar_service_oauth()  # or get_calendar_service_service_account()
    all_deid = []
    for cal_id in CALENDAR_IDS:
        events = fetch_events_for_calendar(service, cal_id, TIME_MIN_UTC, TIME_MAX_UTC)
        for e in events:
            all_deid.append(deid_event(e, cal_id, HMAC_SECRET))

    conn = get_conn()
    try:
        run_ddl(conn)
        load_events(conn, all_deid)
    finally:
        conn.close()

if __name__ == "__main__":
    main()


