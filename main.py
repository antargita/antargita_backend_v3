from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List
from sqlalchemy import create_engine, Column, String, Integer, JSON, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, Session
import httpx
import asyncio
from datetime import datetime

# -------------------- CONFIG --------------------

DATABASE_URL = "sqlite:///./app.db"
INGEST_API_URL = "https://laughing-broccoli-hywo.onrender.com/ingest"

# -------------------- DB SETUP --------------------

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# -------------------- TABLES --------------------

class User(Base):
    __tablename__ = "users"
    id = Column(String, primary_key=True)
    email = Column(String)
    display_name = Column(String)


class DiscoveryTable(Base):
    __tablename__ = "discovery"
    user_id = Column(String, ForeignKey("users.id"), primary_key=True)
    data = Column(JSON)


class UsageTable(Base):
    __tablename__ = "usage"
    id = Column(String, primary_key=True)
    user_id = Column(String)
    report_date = Column(String)
    data = Column(JSON)


class JournalTable(Base):
    __tablename__ = "journals"
    id = Column(String, primary_key=True)
    user_id = Column(String)
    data = Column(JSON)


class ReelTable(Base):
    __tablename__ = "reels"
    id = Column(String, primary_key=True)
    user_id = Column(String)
    date = Column(String)
    count = Column(Integer)


Base.metadata.create_all(bind=engine)

# -------------------- APP --------------------

app = FastAPI()

# -------------------- MODELS --------------------

class Discovery(BaseModel):
    categories: List[str]
    problems: List[str]
    onboarding_complete: bool


class UserDiscoveryPayload(BaseModel):
    schema_version: int
    client_timestamp_ms: int
    user_id: str
    email: str
    display_name: str
    discovery: Discovery


class AppUsage(BaseModel):
    package_name: str
    app_label: str
    category: str
    foreground_ms: int
    session_count: int


class UsagePayload(BaseModel):
    schema_version: int
    report_date: str
    timezone: str
    total_foreground_ms: int
    user_id: str
    app_usage: List[AppUsage]


class JournalEntry(BaseModel):
    id: str
    title: str
    body: str
    created_at_ms: int
    updated_at_ms: int


class JournalPayload(BaseModel):
    schema_version: int
    client_timestamp_ms: int
    user_id: str
    entries: List[JournalEntry]


class ReelDay(BaseModel):
    date: str
    count: int


class ReelPayload(BaseModel):
    schema_version: int
    client_timestamp_ms: int
    user_id: str
    daily_counts: List[ReelDay]

# -------------------- HELPERS --------------------

def ms_to_iso(ms):
    return datetime.utcfromtimestamp(ms / 1000).isoformat()


async def send_to_vector_db(entry, user_id):
    async with httpx.AsyncClient(timeout=5.0) as client:
        payload = {
            "entry_id": entry["id"],
            "user_id": user_id,
            "content": entry["body"],
            "date_time": ms_to_iso(entry["updated_at_ms"])
        }
        try:
            await client.post(INGEST_API_URL, json=payload)
        except Exception as e:
            print("Vector DB error:", e)

# -------------------- ROUTES --------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/v1/user-discovery")
def user_discovery(payload: UserDiscoveryPayload, db: Session = Depends(get_db)):
    user = db.get(User, payload.user_id)

    if not user:
        user = User(id=payload.user_id, email=payload.email, display_name=payload.display_name)
        db.add(user)
    else:
        user.email = payload.email
        user.display_name = payload.display_name

    discovery = db.get(DiscoveryTable, payload.user_id)

    if not discovery:
        discovery = DiscoveryTable(user_id=payload.user_id, data=payload.discovery.dict())
        db.add(discovery)
    else:
        discovery.data = payload.discovery.dict()

    db.commit()
    return {"status": "saved"}


@app.post("/v1/usage")
def usage(payload: UsagePayload, db: Session = Depends(get_db)):
    usage = UsageTable(
        id=f"{payload.user_id}_{payload.report_date}",
        user_id=payload.user_id,
        report_date=payload.report_date,
        data=payload.dict()
    )
    db.merge(usage)
    db.commit()
    return {"status": "saved"}


@app.post("/v1/journal")
async def journal(payload: JournalPayload, db: Session = Depends(get_db)):
    tasks = []

    for entry in payload.entries:
        journal = JournalTable(
            id=entry.id,
            user_id=payload.user_id,
            data=entry.dict()
        )
        db.merge(journal)

        tasks.append(send_to_vector_db(entry.dict(), payload.user_id))

    db.commit()
    await asyncio.gather(*tasks)

    return {"status": "synced", "count": len(payload.entries)}


@app.post("/v1/reel-counter")
def reel_counter(payload: ReelPayload, db: Session = Depends(get_db)):
    for day in payload.daily_counts:
        reel = ReelTable(
            id=f"{payload.user_id}_{day.date}",
            user_id=payload.user_id,
            date=day.date,
            count=day.count
        )
        db.merge(reel)

    db.commit()
    return {"status": "saved"}

# -------------------- GET APIs --------------------

@app.get("/v1/user/{user_id}")
def get_user(user_id: str, db: Session = Depends(get_db)):
    user = db.get(User, user_id)
    discovery = db.get(DiscoveryTable, user_id)

    if not user:
        raise HTTPException(404, "User not found")

    return {
        "user_id": user.id,
        "email": user.email,
        "display_name": user.display_name,
        "discovery": discovery.data if discovery else None
    }


@app.get("/v1/journal/{user_id}")
def get_journals(user_id: str, db: Session = Depends(get_db)):
    journals = db.query(JournalTable).filter(JournalTable.user_id == user_id).all()
    return sorted([j.data for j in journals], key=lambda x: x["created_at_ms"], reverse=True)


@app.get("/v1/journal/{user_id}/date/{date}")
def get_journals_by_date(user_id: str, date: str, db: Session = Depends(get_db)):
    journals = db.query(JournalTable).filter(JournalTable.user_id == user_id).all()

    result = []
    for j in journals:
        created_ms = j.data["created_at_ms"]
        entry_date = datetime.utcfromtimestamp(created_ms / 1000).strftime("%Y-%m-%d")
        if entry_date == date:
            result.append(j.data)

    return result


@app.get("/v1/journal/{user_id}/range")
def get_journals_range(user_id: str, start_date: str, end_date: str, db: Session = Depends(get_db)):
    journals = db.query(JournalTable).filter(JournalTable.user_id == user_id).all()

    result = []
    for j in journals:
        created_ms = j.data["created_at_ms"]
        entry_date = datetime.utcfromtimestamp(created_ms / 1000).strftime("%Y-%m-%d")
        if start_date <= entry_date <= end_date:
            result.append(j.data)

    return result


@app.get("/v1/journal/{user_id}/latest")
def get_latest_journals(user_id: str, limit: int = 10, db: Session = Depends(get_db)):
    journals = db.query(JournalTable).filter(JournalTable.user_id == user_id).all()
    sorted_journals = sorted(journals, key=lambda j: j.data["created_at_ms"], reverse=True)
    return [j.data for j in sorted_journals[:limit]]


@app.get("/v1/usage/{user_id}")
def get_usage(user_id: str, db: Session = Depends(get_db)):
    usage = db.query(UsageTable).filter(UsageTable.user_id == user_id).all()
    return [u.data for u in usage]


@app.get("/v1/reel-counter/{user_id}")
def get_reels(user_id: str, db: Session = Depends(get_db)):
    reels = db.query(ReelTable).filter(ReelTable.user_id == user_id).all()
    return [{"date": r.date, "count": r.count} for r in reels]


@app.get("/v1/insights/{user_id}")
def get_insights(user_id: str, db: Session = Depends(get_db)):
    usage_records = db.query(UsageTable).filter(UsageTable.user_id == user_id).all()

    total_time = 0
    app_time = {}

    for record in usage_records:
        data = record.data
        total_time += data.get("total_foreground_ms", 0)

        for app in data.get("app_usage", []):
            label = app["app_label"]
            app_time[label] = app_time.get(label, 0) + app["foreground_ms"]

    top_apps = sorted(app_time.items(), key=lambda x: x[1], reverse=True)[:5]

    return {
        "total_screen_time": total_time,
        "top_apps": top_apps
    }
