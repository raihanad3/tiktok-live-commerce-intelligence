"""
TikPulse - Serving Layer REST API & Continuous Stream Engine (F-10, F-12, F-16)
Framework: FastAPI + PostgreSQL (dengan SQLite ACID Fallback) + Real-Time TikTokLive WebSocket Scraper
Fitur:
  - Scraping Real-Time Langsung dari TikTok Live Room (via TikTokLiveClient Webcast WebSocket)
  - Penanganan Status Akun Offline yang Akurat & Transparan ("Toko Sedang Tidak Live")
  - Opsi Simulasi Demo Terkontrol (hanya berjalan bila diaktifkan secara eksplisit)
  - Isolasi Penuh Data per Toko / Streamer (Session-Scoped Analytics)
  - Penyesuaian Otomatis Produk Sesuai Niche Toko (Sepatu, Baju, Hijab, Distro, Sport, Skincare, dll)
  - Live Comments Waterfall Stream (GET /api/v1/live/comments)
  - REST API Serving Marts & Role-Based Security
"""

import os
import sys
import time
import uuid
import random
import asyncio
import joblib
import sqlite3
import pandas as pd
from fastapi import FastAPI, HTTPException, Header, Query, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timezone

# TikTokLive Scraper integration
try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import CommentEvent, GiftEvent, DisconnectEvent, LiveEndEvent
    TIKTOK_LIVE_AVAILABLE = True
except ImportError:
    TIKTOK_LIVE_AVAILABLE = False

# Pastikan src ada di path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipeline.bronze_to_silver import BronzeToSilverProcessor, SILVER_OUTPUT_DIR
from pipeline.silver_to_gold import aggregate_silver_to_gold, init_gold_tables
from common.store_catalog import get_store_catalog, get_store_comments, detect_store_niche, save_custom_store_catalog

app = FastAPI(
    title="TikPulse Live Commerce Serving API",
    description="REST API Serving Layer & Real-Time Stream Engine TikTok Live",
    version="1.0.0"
)

STATIC_HTML_PATH = os.path.join(os.path.dirname(__file__), "static", "index.html")

@app.get("/", include_in_schema=False)
@app.get("/dashboard", include_in_schema=False)
def serve_dashboard():
    """Menyajikan antarmuka visual Live Host Cockpit Web UI."""
    if os.path.exists(STATIC_HTML_PATH):
        return FileResponse(STATIC_HTML_PATH)
    return {"message": "TikPulse API is running. Visit /docs for Swagger UI."}

# Konfigurasi Database
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "tikpulse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "tikpulse_admin")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "tikpulse_secure_password_2026")
SQLITE_DB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "gold", "tikpulse_gold.db")
MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "ml", "model_intent.joblib")

# Inisialisasi Processor & Model
processor = BronzeToSilverProcessor()
ml_model = processor.model

# Global State untuk Continuous Streaming & Live Scraping
active_streamer = "@tokobajumurah"
active_session_id = f"sess_{int(time.time())}_tokobajumurah"
active_niche = detect_store_niche(active_streamer)
is_streaming_active = True
is_simulation_enabled = False       # Default False: Tidak membuat komentar palsu
stream_mode = "OFFLINE"             # "REALTIME_TIKTOK_LIVE" | "OFFLINE" | "SIMULATION_FALLBACK"
is_real_live_connected = False
tiktok_client_instance = None
recent_comments_buffer = []

IS_POSTGRES_AVAILABLE = None

class DatabaseContext:
    def __init__(self):
        self.conn = None
        self.db_type = "SQLITE"

    def __enter__(self):
        global IS_POSTGRES_AVAILABLE
        if IS_POSTGRES_AVAILABLE is None:
            try:
                import psycopg2
                from psycopg2.extras import RealDictCursor
                self.conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASS,
                    cursor_factory=RealDictCursor,
                    connect_timeout=1
                )
                self.db_type = "POSTGRES"
                IS_POSTGRES_AVAILABLE = True
            except Exception:
                IS_POSTGRES_AVAILABLE = False
                self.conn = sqlite3.connect(SQLITE_DB_PATH)
                self.conn.row_factory = sqlite3.Row
                self.db_type = "SQLITE"
        elif IS_POSTGRES_AVAILABLE:
            try:
                import psycopg2
                from psycopg2.extras import RealDictCursor
                self.conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                    dbname=POSTGRES_DB,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASS,
                    cursor_factory=RealDictCursor,
                    connect_timeout=1
                )
                self.db_type = "POSTGRES"
            except Exception:
                IS_POSTGRES_AVAILABLE = False
                self.conn = sqlite3.connect(SQLITE_DB_PATH)
                self.conn.row_factory = sqlite3.Row
                self.db_type = "SQLITE"
        else:
            self.conn = sqlite3.connect(SQLITE_DB_PATH)
            self.conn.row_factory = sqlite3.Row
            self.db_type = "SQLITE"
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def execute_query(self, query: str, params: tuple = ()):
        cur = self.conn.cursor()
        if self.db_type == "SQLITE":
            query = query.replace("%s", "?")
        cur.execute(query, params)
        return cur

def verify_access(
    x_user_role: Optional[str] = Header(None, alias="X-User-Role"),
    role_header: Optional[str] = Header(None, alias="role"),
    required_role: str = "viewer"
):
    role = x_user_role or role_header or "viewer"
    if not role:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Header peran wajib disertakan.")
    if required_role == "admin" and role.lower() != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Akses ditolak: Hanya peran 'admin' yang diizinkan.")
    return role.lower()

SAMPLE_BUYERS = ["Anisa_Style", "Kevin_Sneaker", "Citra_Dewi", "Rian_Fashion", "Bambang_Keren", "Siti_Bandung", "Dedi_Distro", "Maya_Kurnia", "Fajar_Surabaya", "Jessica_99"]

last_aggregate_time = 0

def ingest_and_process_event(raw_event: dict):
    """Memproses raw event lewat pipeline Bronze -> Silver -> Gold dan update buffer."""
    global recent_comments_buffer, active_streamer, active_session_id, last_aggregate_time
    try:
        processed = processor.process_single_event(raw_event)
        if processed:
            recent_comments_buffer.insert(0, processed)
            if len(recent_comments_buffer) > 40:
                recent_comments_buffer.pop()

            clean_name = active_streamer.replace("@", "").replace("/", "").replace("\\", "")
            parquet_filename = f"silver_{clean_name}_{active_session_id}.parquet"
            parquet_path = os.path.join(SILVER_OUTPUT_DIR, parquet_filename)
            df_rec = pd.DataFrame([processed])
            if os.path.exists(parquet_path):
                try:
                    df_old = pd.read_parquet(parquet_path)
                    df_combined = pd.concat([df_old, df_rec], ignore_index=True)
                    df_combined.tail(300).to_parquet(parquet_path, engine="pyarrow", index=False)
                except Exception:
                    df_rec.to_parquet(parquet_path, engine="pyarrow", index=False)
            else:
                df_rec.to_parquet(parquet_path, engine="pyarrow", index=False)

            now_t = time.time()
            if now_t - last_aggregate_time > 1.2:
                last_aggregate_time = now_t
                aggregate_silver_to_gold()
            return processed
    except Exception as e:
        print(f"[INGEST ERROR] {e}")
    return None

async def try_connect_tiktok_live(username: str) -> bool:
    """Mencoba menghubungkan TikTokLiveClient ke room live streaming TikTok yang sedang berlangsung."""
    global tiktok_client_instance, is_real_live_connected, stream_mode
    if not TIKTOK_LIVE_AVAILABLE:
        print("[TIKTOK LIVE] Library TikTokLive tidak terpasang.")
        return False

    clean_user = username.lstrip("@").strip()
    if not clean_user:
        return False

    try:
        client = TikTokLiveClient(unique_id=clean_user)

        @client.on(CommentEvent)
        async def on_live_comment(event: CommentEvent):
            if not is_streaming_active:
                return
            try:
                comm_text = ""
                if hasattr(event, "comment") and event.comment:
                    comm_text = str(event.comment)
                elif hasattr(event, "content") and event.content:
                    comm_text = str(event.content)

                user_nick = "Pengguna_TikTok"
                user_id = f"u_{random.randint(1000, 9999)}"
                if hasattr(event, "user") and event.user:
                    user_nick = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "TikTok_User")
                    user_id = str(getattr(event.user, "id", None) or getattr(event.user, "user_id", user_id))

                raw = {
                    "event_id": f"evt_{uuid.uuid4().hex[:10]}",
                    "session_id": active_session_id,
                    "streamer": active_streamer,
                    "event_type": "comment",
                    "event_time": datetime.now(timezone.utc).isoformat(),
                    "data": {
                        "user_id": user_id,
                        "nickname": user_nick,
                        "comment_text": comm_text
                    }
                }
                ingest_and_process_event(raw)
            except Exception as e:
                print(f"[TIKTOK COMMENT INGEST ERROR] {e}")

        @client.on(GiftEvent)
        async def on_live_gift(event: GiftEvent):
            if not is_streaming_active:
                return
            try:
                if getattr(event, "streaking", False):
                    return

                gift_name = "Gift"
                diamonds = 1
                if hasattr(event, "gift") and event.gift:
                    gift_name = getattr(event.gift, "name", "Gift")
                    diamonds = getattr(event.gift, "diamond_count", 1)

                user_nick = "Sultan_TikTok"
                user_id = f"u_{random.randint(1000, 9999)}"
                if hasattr(event, "user") and event.user:
                    user_nick = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "Sultan_Live")
                    user_id = str(getattr(event.user, "id", None) or getattr(event.user, "user_id", user_id))

                raw = {
                    "event_id": f"evt_{uuid.uuid4().hex[:10]}",
                    "session_id": active_session_id,
                    "streamer": active_streamer,
                    "event_type": "gift",
                    "event_time": datetime.now(timezone.utc).isoformat(),
                    "data": {
                        "user_id": user_id,
                        "nickname": user_nick,
                        "gift_name": gift_name,
                        "diamond_count": diamonds,
                        "repeat_count": getattr(event, "repeat_count", 1)
                    }
                }
                ingest_and_process_event(raw)
            except Exception as e:
                print(f"[TIKTOK GIFT INGEST ERROR] {e}")

        @client.on(DisconnectEvent)
        async def on_disconnect(event: DisconnectEvent):
            global is_real_live_connected, stream_mode
            print(f"[TIKTOK LIVE] Terputus dari live @{clean_user}. Status beralih ke OFFLINE.")
            is_real_live_connected = False
            stream_mode = "OFFLINE"

        @client.on(LiveEndEvent)
        async def on_live_end(event: LiveEndEvent):
            global is_real_live_connected, stream_mode
            print(f"[TIKTOK LIVE] Siaran live @{clean_user} telah usai.")
            is_real_live_connected = False
            stream_mode = "OFFLINE"

        # Cek apakah akun sedang live dengan timeout 4.0 detik
        is_live = await asyncio.wait_for(client.is_live(), timeout=4.0)
        if not is_live:
            print(f"[TIKTOK CHECK] Akun @{clean_user} saat ini tidak sedang siaran live.")
            return False

        # Mulai mendengarkan koneksi websocket live secara asinkron
        await client.start()
        tiktok_client_instance = client
        is_real_live_connected = True
        stream_mode = "REALTIME_TIKTOK_LIVE"
        print(f"[TIKTOK LIVE] Berhasil tersambung ke WebSocket live stream @{clean_user}!")
        return True

    except Exception as e:
        print(f"[TIKTOK LIVE ATTEMPT] Tidak dapat terhubung ke @{clean_user}: {e}")
        is_real_live_connected = False
        stream_mode = "OFFLINE"
        return False

async def continuous_stream_worker():
    """Worker background yang HANYA memancarkan data jika mode simulasi demo diaktifkan secara eksplisit."""
    global is_streaming_active, active_streamer, active_session_id, is_real_live_connected, is_simulation_enabled

    while True:
        # HANYA generate komentar jika mode simulasi demo diaktifkan dan TIDAK live asli
        if is_streaming_active and is_simulation_enabled and not is_real_live_connected:
            try:
                now_iso = datetime.now(timezone.utc).isoformat()
                evt_id = f"evt_{uuid.uuid4().hex[:10]}"

                is_gift = random.random() < 0.15
                if is_gift:
                    gift_name, diamond = random.choice([("Rose", 25), ("Paus", 2150), ("Cap", 99), ("Doughnut", 30)])
                    user = random.choice(SAMPLE_BUYERS)
                    raw_event = {
                        "event_id": evt_id,
                        "session_id": active_session_id,
                        "streamer": active_streamer,
                        "event_type": "gift",
                        "event_time": now_iso,
                        "data": {
                            "user_id": f"u_{random.randint(1000, 9999)}",
                            "nickname": user,
                            "gift_name": gift_name,
                            "diamond_count": diamond,
                            "repeat_count": 1
                        }
                    }
                else:
                    comments_pool = get_store_comments(active_streamer, niche_override=active_niche)
                    comm = random.choice(comments_pool)
                    user = random.choice(SAMPLE_BUYERS)
                    raw_event = {
                        "event_id": evt_id,
                        "session_id": active_session_id,
                        "streamer": active_streamer,
                        "event_type": "comment",
                        "event_time": now_iso,
                        "data": {
                            "user_id": f"u_{random.randint(1000, 9999)}",
                            "nickname": user,
                            "comment_text": comm
                        }
                    }

                ingest_and_process_event(raw_event)

            except Exception as e:
                print(f"[STREAM WORKER ERROR] {e}")

        await asyncio.sleep(random.uniform(2.0, 3.5))

@app.on_event("startup")
async def startup_event():
    # Pastikan skema tabel Gold sudah siap
    try:
        with DatabaseContext() as db:
            init_gold_tables(db.conn, db.db_type)
    except Exception as e:
        print(f"[STARTUP WARN] Inisialisasi Gold Tables: {e}")

    # Default startup: sistem dalam kondisi bersih tanpa data palsu
    asyncio.create_task(continuous_stream_worker())

@app.on_event("shutdown")
async def shutdown_event():
    global tiktok_client_instance
    if tiktok_client_instance:
        try:
            await tiktok_client_instance.disconnect(close_client=True)
        except Exception:
            pass

# ========================================================
# REST API ENDPOINTS
# ========================================================

@app.post("/api/v1/admin/reset-session", tags=["Admin"])
def admin_reset_session(
    role: Optional[str] = Header("viewer", alias="role"),
    x_user_role: Optional[str] = Header(None, alias="X-User-Role")
):
    """Endpoint khusus peran Admin untuk mereset sesi live streaming (F-16)."""
    user_role = x_user_role or role or "viewer"
    if user_role.lower() != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Akses ditolak: Hanya peran 'admin' yang diizinkan.")
    global recent_comments_buffer
    recent_comments_buffer.clear()
    return {"status": "SUCCESS", "message": "Sesi streaming berhasil direset oleh Admin."}

class SwitchSessionRequest(BaseModel):
    username: str
    niche_override: Optional[str] = None

@app.post("/api/v1/session/switch", tags=["Session"])
async def switch_session(req: SwitchSessionRequest, role: str = Header("viewer")):
    """Beralih ke akun TikTok Live lain. Jika akun sedang live, otomatis menyerap komentar asli secara real-time. Jika offline, sistem menampilkan status Toko Sedang Tidak Live tanpa data palsu."""
    global active_streamer, active_session_id, recent_comments_buffer, active_niche
    global tiktok_client_instance, is_real_live_connected, stream_mode, is_simulation_enabled
    verify_access(role, "viewer")

    clean_user = req.username.strip()
    if not clean_user.startswith("@"):
        clean_user = f"@{clean_user}"

    # Disconnect koneksi TikTok Live sebelumnya jika ada
    if tiktok_client_instance:
        try:
            await tiktok_client_instance.disconnect(close_client=True)
        except Exception:
            pass
        tiktok_client_instance = None
    is_real_live_connected = False
    is_simulation_enabled = False  # Reset simulasi saat ganti toko

    active_streamer = clean_user
    active_session_id = f"sess_{int(time.time())}_{clean_user.replace('@', '')}"
    recent_comments_buffer.clear()

    # Cek apakah user menggunakan manual niche override
    niche_override_used = False
    from common.store_catalog import STORE_NICHES, normalize_niche_key
    norm_override = normalize_niche_key(req.niche_override)
    if norm_override and norm_override in STORE_NICHES:
        active_niche = norm_override
        niche_override_used = True
    else:
        active_niche = detect_store_niche(active_streamer)

    # Coba hubungkan langsung ke siaran live TikTok asli
    connected_real = await try_connect_tiktok_live(clean_user)

    if connected_real:
        stream_mode = "REALTIME_TIKTOK_LIVE"
        msg = f"🟢 Berhasil terhubung LANGSUNG ke siaran TikTok Live {active_streamer}! Komentar dan saweran mengalir 100% Real-Time dari penonton asli."
    else:
        stream_mode = "OFFLINE"
        msg = f"🔴 Toko {active_streamer} sedang tidak Live di TikTok. Menunggu siaran langsung dimulai."

    return {
        "status": "SUCCESS" if connected_real else "OFFLINE",
        "streamer": active_streamer,
        "session_id": active_session_id,
        "stream_mode": stream_mode,
        "is_real_live": connected_real,
        "niche": active_niche,
        "niche_override_used": niche_override_used,
        "new_events_processed": len(recent_comments_buffer),
        "message": msg
    }

@app.post("/api/v1/stream/simulation-toggle", tags=["Session"])
def toggle_simulation(role: str = Header("viewer")):
    """Mengaktifkan / Menonaktifkan simulasi demo interaktif (khusus keperluan presentasi saat toko offline)."""
    global is_simulation_enabled, stream_mode
    verify_access(role, "viewer")
    is_simulation_enabled = not is_simulation_enabled
    if is_simulation_enabled and not is_real_live_connected:
        stream_mode = "SIMULATION_FALLBACK"
    elif not is_real_live_connected:
        stream_mode = "OFFLINE"
    return {
        "is_simulation_enabled": is_simulation_enabled,
        "stream_mode": stream_mode,
        "status": "SIMULATION_ENABLED" if is_simulation_enabled else "SIMULATION_DISABLED"
    }

@app.post("/api/v1/stream/toggle", tags=["Session"])
def toggle_stream(role: str = Header("viewer")):
    """Menyalakan / Menjeda aliran streaming."""
    global is_streaming_active
    verify_access(role, "viewer")
    is_streaming_active = not is_streaming_active
    return {
        "is_streaming_active": is_streaming_active,
        "status": "STREAMING_RESUMED" if is_streaming_active else "STREAMING_PAUSED"
    }

@app.get("/api/v1/live/comments", tags=["Stream"])
def get_live_comments(role: str = Header("viewer")):
    """Mengambil aliran komentar live terbaru khusus toko aktif."""
    verify_access(role, "viewer")
    return {
        "streamer": active_streamer,
        "session_id": active_session_id,
        "stream_mode": stream_mode,
        "is_real_live": is_real_live_connected,
        "is_streaming": is_streaming_active,
        "is_simulation_enabled": is_simulation_enabled,
        "total_buffered": len(recent_comments_buffer),
        "comments": recent_comments_buffer[:20]
    }

@app.get("/api/v1/niches/available", tags=["System"])
def get_available_niches(role: str = Header("viewer")):
    """Mengambil daftar niche yang tersedia untuk manual override."""
    verify_access(role, "viewer")
    from common.store_catalog import STORE_NICHES
    niches_info = []
    for key, data in STORE_NICHES.items():
        niches_info.append({
            "key": key,
            "category": data["category"],
            "keywords": data["keywords"][:5],
            "total_products": len(data["products"]),
            "total_comments": len(data.get("comments", []))
        })
    return {
        "total_niches": len(niches_info),
        "niches": niches_info,
        "note": "Gunakan 'key' sebagai parameter niche_override saat switch session"
    }

@app.get("/api/v1/health", tags=["System"])
def health_check():
    return {
        "status": "HEALTHY",
        "service": "tikpulse-serving-api",
        "streamer": active_streamer,
        "session_id": active_session_id,
        "active_niche": active_niche,
        "stream_mode": stream_mode,
        "is_real_live": is_real_live_connected,
        "is_simulation_enabled": is_simulation_enabled,
        "is_streaming_active": is_streaming_active,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/api/v1/live/summary", tags=["Analytics Marts"])
def get_live_summary(role: str = Header("viewer")):
    """Mengambil ringkasan metrik live KHUSUS sesi toko yang sedang aktif."""
    verify_access(role, "viewer")
    try:
        with DatabaseContext() as db:
            cur = db.execute_query(
                "SELECT * FROM mart_live_summary WHERE session_id = %s ORDER BY last_updated DESC LIMIT 1;",
                (active_session_id,)
            )
            row = cur.fetchone()

            if not row or len(recent_comments_buffer) == 0:
                return {
                    "data": {
                        "session_id": active_session_id,
                        "streamer": active_streamer,
                        "stream_mode": stream_mode,
                        "is_real_live": is_real_live_connected,
                        "is_simulation_enabled": is_simulation_enabled,
                        "niche": detect_store_niche(active_streamer),
                        "total_comments": len([c for c in recent_comments_buffer if c.get("event_type") == "comment"]),
                        "total_gift_revenue_idr": sum(c.get("gift_value_idr", 0) for c in recent_comments_buffer if c.get("event_type") == "gift"),
                        "purchase_intent_count": len([c for c in recent_comments_buffer if c.get("ml_intent") == "INTENT_BUY"]),
                        "question_count": len([c for c in recent_comments_buffer if c.get("ml_intent") == "QUESTION"]),
                        "complaint_count": len([c for c in recent_comments_buffer if c.get("ml_intent") == "COMPLAINT"])
                    },
                    "stream_mode": stream_mode,
                    "is_real_live": is_real_live_connected,
                    "is_simulation_enabled": is_simulation_enabled,
                    "message": "Toko sedang tidak live." if not is_real_live_connected else "Siaran live aktif."
                }
            res_dict = dict(row)
            res_dict["niche"] = detect_store_niche(active_streamer)
            res_dict["stream_mode"] = stream_mode
            res_dict["is_real_live"] = is_real_live_connected
            res_dict["is_simulation_enabled"] = is_simulation_enabled
            return {
                "data": res_dict,
                "stream_mode": stream_mode,
                "is_real_live": is_real_live_connected,
                "is_simulation_enabled": is_simulation_enabled,
                "is_streaming": is_streaming_active,
                "freshness": "REAL_TIME" if is_real_live_connected else "STANDBY",
                "retrieved_at": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@app.get("/api/v1/products/demand", tags=["Analytics Marts"])
def get_product_demand(
    limit: int = Query(5, ge=1, le=50),
    role: str = Header("viewer")
):
    """Mengambil leaderboard permintaan produk KHUSUS sesi toko yang sedang aktif."""
    verify_access(role, "viewer")
    try:
        with DatabaseContext() as db:
            cur = db.execute_query("""
                SELECT etalase_no, product_name, price_idr, request_count, high_intent_count, last_requested_at
                FROM mart_product_demand
                WHERE session_id = %s
                ORDER BY request_count DESC, high_intent_count DESC
                LIMIT %s;
            """, (active_session_id, limit))
            rows = [dict(r) for r in cur.fetchall()]

            # Fallback 1: Hitung dari recent_comments_buffer jika ada event yang masuk
            if not rows and recent_comments_buffer:
                counts = {}
                for c in recent_comments_buffer:
                    et = c.get("extracted_etalase")
                    if et:
                        pname = c.get("matched_product_name") or f"Etalase #{et}"
                        price = c.get("matched_price_idr") or 85000
                        if et not in counts:
                            counts[et] = {"etalase_no": et, "product_name": pname, "price_idr": price, "request_count": 0, "high_intent_count": 0}
                        counts[et]["request_count"] += 1
                        if c.get("ml_intent") == "INTENT_BUY":
                            counts[et]["high_intent_count"] += 1
                rows = sorted(counts.values(), key=lambda x: x["request_count"], reverse=True)[:limit]

            # Fallback 2: Jika belum ada permintaan live, tampilkan katalog toko aktif dengan 0 permintaan (Jujur & Akurat)
            if not rows:
                cat_df = get_store_catalog(active_streamer, niche_override=active_niche)
                for _, r in cat_df.head(limit).iterrows():
                    rows.append({
                        "etalase_no": int(r["etalase_no"]),
                        "product_name": str(r["product_name"]),
                        "price_idr": int(r["price_idr"]),
                        "request_count": 0,
                        "high_intent_count": 0,
                        "last_requested_at": None
                    })

            return {
                "total_products": len(rows),
                "streamer": active_streamer,
                "session_id": active_session_id,
                "stream_mode": stream_mode,
                "is_real_live": is_real_live_connected,
                "niche": active_niche,
                "leaderboard": rows,
                "query_time": datetime.now(timezone.utc).isoformat()
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@app.get("/api/v1/gifters/leaderboard", tags=["Analytics Marts"])
def get_top_gifters(limit: int = Query(5, ge=1, le=20), role: str = Header("viewer")):
    """Mengambil leaderboard saweran Sultan KHUSUS sesi toko yang sedang aktif."""
    verify_access(role, "viewer")
    try:
        with DatabaseContext() as db:
            cur = db.execute_query("""
                SELECT nickname, total_gift_idr, gift_count, last_gift_at
                FROM mart_top_gifters
                WHERE session_id = %s
                ORDER BY total_gift_idr DESC
                LIMIT %s;
            """, (active_session_id, limit))
            rows = [dict(r) for r in cur.fetchall()]

            # Fallback jika belum masuk mart tapi ada di buffer
            if not rows and recent_comments_buffer:
                gift_map = {}
                for c in recent_comments_buffer:
                    if c.get("event_type") == "gift":
                        nick = c.get("nickname", "Sultan_Anonim")
                        val = c.get("gift_value_idr", 0)
                        if nick not in gift_map:
                            gift_map[nick] = {"nickname": nick, "total_gift_idr": 0, "gift_count": 0, "last_gift_at": c.get("event_time")}
                        gift_map[nick]["total_gift_idr"] += val
                        gift_map[nick]["gift_count"] += 1
                rows = sorted(gift_map.values(), key=lambda x: x["total_gift_idr"], reverse=True)[:limit]

            # Jika offline dan belum ada saweran asli, jangan tampilkan data palsu
            if not rows:
                rows = []

            return {
                "streamer": active_streamer,
                "session_id": active_session_id,
                "stream_mode": stream_mode,
                "is_real_live": is_real_live_connected,
                "top_sultans": rows
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

class PredictionRequest(BaseModel):
    comment_text: str = Field(..., min_length=2, max_length=500)
    streamer_id: Optional[str] = Field(None)

class PredictionResponse(BaseModel):
    comment_text: str
    predicted_intent: str
    confidence_score: float
    model_version: str
    predicted_at: str

@app.post("/api/v1/ml/predict", response_model=PredictionResponse, tags=["Machine Learning"])
def predict_comment_intent(req: PredictionRequest, role: str = Header("viewer")):
    verify_access(role, "viewer")
    if not ml_model:
        raise HTTPException(status_code=503, detail="Model ML belum tersedia di server.")

    if not req.comment_text.strip():
        raise HTTPException(status_code=400, detail="Teks komentar tidak boleh kosong.")

    try:
        intent = ml_model.predict([req.comment_text])[0]
        score = ml_model.predict_proba([req.comment_text]).max() if hasattr(ml_model, "predict_proba") else 0.88
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal melakukan inferensi: {str(e)}")

    now_iso = datetime.now(timezone.utc).isoformat()
    return PredictionResponse(
        comment_text=req.comment_text,
        predicted_intent=intent,
        confidence_score=round(float(score), 4),
        model_version="v1.0-tfidf-logreg",
        predicted_at=now_iso
    )

class CatalogProductItem(BaseModel):
    etalase_no: int
    product_name: str
    price_idr: int
    sku: Optional[str] = None
    category: Optional[str] = None
    stock: Optional[int] = 50

class UpdateCatalogRequest(BaseModel):
    streamer: Optional[str] = None
    products: List[CatalogProductItem]

@app.get("/api/v1/catalog", tags=["Store Catalog"])
def get_catalog_endpoint(streamer: Optional[str] = Query(None), niche_override: Optional[str] = Query(None), role: str = Header("viewer")):
    """Mengambil katalog produk resmi atau kustom toko aktif."""
    verify_access(role, "viewer")
    target_streamer = streamer or active_streamer
    target_niche = niche_override or (active_niche if target_streamer == active_streamer else detect_store_niche(target_streamer))
    df = get_store_catalog(target_streamer, niche_override=target_niche)
    products = df.to_dict(orient="records") if not df.empty else []
    return {
        "streamer": target_streamer,
        "niche": target_niche,
        "total_products": len(products),
        "products": products
    }

@app.post("/api/v1/catalog/update", tags=["Store Catalog"])
def update_catalog_endpoint(req: UpdateCatalogRequest, role: str = Header("viewer")):
    """Menyimpan pembaruan katalog produk toko dan memperbarui mart produk secara instan."""
    verify_access(role, "viewer")
    target_streamer = req.streamer or active_streamer
    items = [p.model_dump() if hasattr(p, "model_dump") else p.dict() for p in req.products]
    save_custom_store_catalog(target_streamer, items)

    # Sinkronisasi ke mart_product_demand di database
    try:
        with DatabaseContext() as db:
            for p in req.products:
                db.execute_query("""
                    UPDATE mart_product_demand
                    SET product_name = %s, price_idr = %s
                    WHERE session_id = %s AND etalase_no = %s;
                """, (p.product_name, p.price_idr, active_session_id, p.etalase_no))
            if hasattr(db.conn, "commit"):
                db.conn.commit()
    except Exception as e:
        print(f"[CATALOG SYNC ERROR] {e}")

    # Sinkronisasi buffer komentar langsung di memori
    for c in recent_comments_buffer:
        et = c.get("extracted_etalase")
        if et:
            for p in req.products:
                if p.etalase_no == et:
                    c["matched_product_name"] = p.product_name
                    c["matched_price_idr"] = p.price_idr

    return {
        "status": "success",
        "message": f"Katalog untuk {target_streamer} berhasil diperbarui ({len(items)} produk).",
        "streamer": target_streamer,
        "total_updated": len(items)
    }

