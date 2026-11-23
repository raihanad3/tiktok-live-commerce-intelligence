"""
TikPulse - Serving Layer REST API & Continuous Stream Engine (F-10, F-12, F-16)
Framework: FastAPI + PostgreSQL (dengan SQLite WAL Fallback) + Real-Time TikTokLive WebSocket Scraper + SSE Push
Fitur:
  - Scraping Real-Time Langsung dari TikTok Live Room (via TikTokLiveClient Webcast WebSocket)
  - Ekstraksi Produk Etalase Real-Time OecLiveShoppingEvent (v1 pop_product/snap_shot + v2 CommerceInfo)
  - Zero-Blocking In-Memory Session State + Micro-Batch Background Persistence (Parquet & Gold Marts)
  - Server-Sent Events (SSE) Push Stream (/api/v1/live/stream) berlatensi sub-10ms
  - Isolasi Penuh Data per Toko / Streamer (Session-Scoped Analytics)
  - REST API Serving Marts & Role-Based Security
"""

import os
import sys
import json
import time
import uuid
import random
import re
import asyncio
import sqlite3
import pandas as pd
from collections import deque, OrderedDict
from fastapi import FastAPI, HTTPException, Header, Query, status
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime, timezone

# TikTokLive Scraper integration
try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import CommentEvent, GiftEvent, DisconnectEvent, LiveEndEvent, OecLiveShoppingEvent
    try:
        from TikTokLive.events import RoomUserSeqEvent
    except ImportError:
        RoomUserSeqEvent = None
    TIKTOK_LIVE_AVAILABLE = True
except ImportError:
    TIKTOK_LIVE_AVAILABLE = False
    RoomUserSeqEvent = None

# Pastikan src ada di path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from pipeline.bronze_to_silver import BronzeToSilverProcessor, SILVER_OUTPUT_DIR
from pipeline.silver_to_gold import (
    aggregate_silver_to_gold,
    init_gold_tables,
    upsert_session_gold_snapshot,
    _configure_sqlite_connection,
)
from common.store_catalog import (
    get_store_catalog,
    get_store_comments,
    detect_store_niche,
    save_custom_store_catalog,
    load_custom_store_catalog,
    has_custom_store_catalog,
    delete_custom_store_catalog,
)

app = FastAPI(
    title="TikPulse Live Commerce Serving API",
    description="REST API Serving Layer & Real-Time Stream Engine TikTok Live",
    version="1.1.0"
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

# In-Memory Session State (O(1) update tanpa disk/DB I/O di hot path)
recent_comments_buffer: deque = deque(maxlen=200)
session_summary: dict = {
    "total_comments": 0,
    "total_gift_revenue_idr": 0,
    "purchase_intent_count": 0,
    "question_count": 0,
    "complaint_count": 0,
    "viewer_count": 0,
    "last_updated": None,
}
session_product_demand: dict[int, dict] = {}
session_top_gifters: dict[str, dict] = {}
live_captured_products: dict[int, dict] = {}
current_pinned_etalase: Optional[int] = None
live_room_title: Optional[str] = None
room_has_commerce_goods: bool = False
room_commerce_restricted: bool = False
pending_silver_batch: list[dict] = []
silver_batch_seq: int = 0
session_dirty_flag: bool = False
latency_samples_ms: deque = deque(maxlen=100)
sse_subscribers: set[asyncio.Queue] = set()
gift_streak_tracker: OrderedDict = OrderedDict()

IS_POSTGRES_AVAILABLE = None

def reset_active_session_state():
    """Mengosongkan seluruh state in-memory untuk sesi aktif saat ganti toko atau reset."""
    global current_pinned_etalase, live_room_title, room_has_commerce_goods, room_commerce_restricted, silver_batch_seq, session_dirty_flag
    recent_comments_buffer.clear()
    session_summary.update({
        "total_comments": 0,
        "total_gift_revenue_idr": 0,
        "purchase_intent_count": 0,
        "question_count": 0,
        "complaint_count": 0,
        "viewer_count": 0,
        "last_updated": None,
    })
    session_product_demand.clear()
    session_top_gifters.clear()
    live_captured_products.clear()
    current_pinned_etalase = None
    live_room_title = None
    room_has_commerce_goods = False
    room_commerce_restricted = False
    pending_silver_batch.clear()
    silver_batch_seq = 0
    session_dirty_flag = False
    latency_samples_ms.clear()
    gift_streak_tracker.clear()

def get_avg_latency_ms() -> float:
    if not latency_samples_ms:
        return 0.0
    return round(sum(latency_samples_ms) / len(latency_samples_ms), 2)

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
                self.conn = sqlite3.connect(SQLITE_DB_PATH, timeout=5.0)
                _configure_sqlite_connection(self.conn)
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
                self.conn = sqlite3.connect(SQLITE_DB_PATH, timeout=5.0)
                _configure_sqlite_connection(self.conn)
                self.conn.row_factory = sqlite3.Row
                self.db_type = "SQLITE"
        else:
            self.conn = sqlite3.connect(SQLITE_DB_PATH, timeout=5.0)
            _configure_sqlite_connection(self.conn)
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

def _parse_price_idr(price_val) -> int:
    """Mengubah string/angka harga (misal 'Rp 149.000', '149000.00', '149,000.00') menjadi integer IDR."""
    if price_val is None or price_val == "":
        return 0
    if isinstance(price_val, (int, float)):
        return int(round(price_val))
    s = str(price_val).strip()
    if not s:
        return 0
    # Hapus desimal sen seperti .00 atau ,00 di akhir
    s = re.sub(r'[.,]00$', '', s)
    digits = re.sub(r'[^\d]', '', s)
    return int(digits) if digits else 0

def extract_oec_shopping_products(event) -> tuple[list[dict], Optional[int]]:
    """
    Mengekstrak produk etalase nyata dari WebcastOecLiveShoppingMessage (v1 + v2).
    Mengembalikan (list_produk, nomor_etalase_yang_sedang_dipin).
    """
    by_etalase: dict[int, dict] = {}
    pinned_etalase: Optional[int] = None

    def _merge_item(et_no: int, title: str, price_idr: int, pid: str = "", image_url: str = "", is_pinned: bool = False):
        if et_no <= 0 or not title:
            return
        clean_title = str(title).strip()
        if not clean_title:
            return
        existing = by_etalase.get(et_no)
        if not existing:
            by_etalase[et_no] = {
                "etalase_no": et_no,
                "product_name": clean_title,
                "price_idr": price_idr,
                "product_id": str(pid or ""),
                "image_url": str(image_url or ""),
                "source": "TIKTOK_LIVE_PIN",
                "is_pinned": is_pinned,
            }
        else:
            if price_idr > 0 and existing["price_idr"] == 0:
                existing["price_idr"] = price_idr
            if pid and not existing["product_id"]:
                existing["product_id"] = str(pid)
            if image_url and not existing["image_url"]:
                existing["image_url"] = str(image_url)
            if is_pinned:
                existing["is_pinned"] = True

    # 1. Payload v1: live_product_number + pop_product
    raw_prod_no = getattr(event, "live_product_number", None)
    try:
        v1_et_no = int(raw_prod_no) if raw_prod_no else 0
    except (TypeError, ValueError):
        v1_et_no = 0

    pop = getattr(event, "pop_product", None)
    if pop:
        pop_title = getattr(pop, "title", None)
        pop_price = _parse_price_idr(getattr(pop, "price", None))
        pop_pid = getattr(pop, "product_id", "")
        pop_img = getattr(pop, "image_url", "")
        if pop_title:
            target_et = v1_et_no if v1_et_no > 0 else 1
            _merge_item(target_et, pop_title, pop_price, str(pop_pid or ""), str(pop_img or ""), is_pinned=True)
            pinned_etalase = target_et

    # 2. Payload v1 fallback: product_snap_shot
    snap = getattr(event, "product_snap_shot", None)
    if snap:
        snap_title = getattr(snap, "title", None)
        snap_pid = getattr(snap, "product_id", "")
        if snap_title:
            target_et = v1_et_no if v1_et_no > 0 else (pinned_etalase or 1)
            _merge_item(target_et, snap_title, 0, str(snap_pid or ""), "", is_pinned=(pinned_etalase == target_et))
            if pinned_etalase is None:
                pinned_etalase = target_et

    # 3. Payload v2: oec_live_shopping_message_v2.commerce_info
    v2 = getattr(event, "oec_live_shopping_message_v2", None)
    commerce = getattr(v2, "commerce_info", None) if v2 else None
    if commerce:
        pop_card = getattr(commerce, "pop_card_info", None)
        pinned_pid = getattr(pop_card, "pop_product_id", None) if pop_card else None

        prod_map = getattr(commerce, "product_info", None) or {}
        items_iter = prod_map.values() if isinstance(prod_map, dict) else prod_map
        for item in items_iter:
            bag_idx_obj = getattr(item, "bag_index", None)
            bag_val = getattr(bag_idx_obj, "value", None) if bag_idx_obj is not None else None
            try:
                et_no = int(bag_val) if bag_val else 0
            except (TypeError, ValueError):
                et_no = 0

            title_obj = getattr(item, "title", None)
            if isinstance(title_obj, str):
                item_title = title_obj
            elif title_obj is not None:
                item_title = getattr(title_obj, "text", None)
            else:
                item_title = None

            item_pid = getattr(item, "product_id", None)

            # Ekstrak harga jika objek price tersedia di varian protobuf
            item_price = 0
            for price_attr in ("price_info", "price", "price_item"):
                p_obj = getattr(item, price_attr, None)
                if p_obj is not None:
                    for sub_attr in ("sale_price_decimal", "sale_price_format", "origin_price_decimal", "origin_price_format"):
                        raw_p = getattr(p_obj, sub_attr, None)
                        if raw_p:
                            item_price = _parse_price_idr(raw_p)
                            if item_price > 0:
                                break
                    if item_price == 0 and isinstance(p_obj, (str, int, float)):
                        item_price = _parse_price_idr(p_obj)
                if item_price > 0:
                    break

            is_item_pinned = bool(pinned_pid is not None and item_pid is not None and str(item_pid) == str(pinned_pid))
            if et_no > 0 and item_title:
                _merge_item(et_no, item_title, item_price, str(item_pid or ""), "", is_pinned=is_item_pinned)
                if is_item_pinned:
                    pinned_etalase = et_no

    products = sorted(by_etalase.values(), key=lambda x: x["etalase_no"])
    return products, pinned_etalase

def extract_room_info_products(room_info: dict) -> list[dict]:
    """
    Mencari metadata produk belanja yang mungkin dikirim di dalam payload room_info TikTok.
    Berjalan 100% gratis dari endpoint room/info tanpa EulerStream berbayar.
    """
    if not isinstance(room_info, dict):
        return []

    found: dict[int, dict] = {}
    fallback_idx = 1

    def _scan(node, depth: int = 0):
        nonlocal fallback_idx
        if depth > 6:
            return
        if isinstance(node, list):
            for elem in node:
                _scan(elem, depth + 1)
            return
        if not isinstance(node, dict):
            return

        title = node.get("title") or node.get("product_name") or node.get("name")
        pid = node.get("product_id") or node.get("id_str")
        has_commerce_shape = bool(
            title
            and isinstance(title, str)
            and any(k in node for k in ("product_id", "bag_index", "live_product_number", "price", "sale_price", "pop_product_id", "open_url"))
        )
        if has_commerce_shape:
            raw_et = node.get("bag_index") or node.get("live_product_number") or node.get("etalase_no") or node.get("index")
            if isinstance(raw_et, dict):
                raw_et = raw_et.get("value")
            try:
                et_no = int(raw_et) if raw_et else 0
            except (TypeError, ValueError):
                et_no = 0
            if et_no <= 0:
                et_no = fallback_idx
                fallback_idx += 1

            raw_price = node.get("price") or node.get("sale_price") or node.get("min_price") or 0
            if isinstance(raw_price, dict):
                raw_price = raw_price.get("sale_price_decimal") or raw_price.get("sale_price_format") or raw_price.get("origin_price_decimal") or 0
            price_idr = _parse_price_idr(raw_price)

            found[et_no] = {
                "etalase_no": et_no,
                "product_name": title.strip(),
                "price_idr": price_idr,
                "product_id": str(pid or ""),
                "source": "TIKTOK_LIVE_PIN",
                "is_pinned": bool(node.get("is_pinned") or "pop_product" in str(node.keys()).lower()),
            }

        for k, v in node.items():
            if isinstance(v, (dict, list)) and (
                depth < 2 or any(w in str(k).lower() for w in ("commerce", "oec", "shop", "prod", "bag", "pop", "ec_", "goods", "item"))
            ):
                _scan(v, depth + 1)

    _scan(room_info, 0)
    return sorted(found.values(), key=lambda x: x["etalase_no"])

def build_active_live_catalog(streamer: str) -> dict[int, dict]:
    """
    Membangun katalog aktif untuk mode REALTIME_TIKTOK_LIVE:
    HANYA gabungan custom catalog user (bila ada) + produk pin live asli TikTok (prioritas tertinggi).
    TIDAK PERNAH memasukkan produk dummy / tebakan hash MD5!
    """
    catalog: dict[int, dict] = {}
    for item in load_custom_store_catalog(streamer):
        et = int(item["etalase_no"])
        catalog[et] = {
            "etalase_no": et,
            "product_name": item["product_name"],
            "price_idr": int(item.get("price_idr", 0) or 0),
            "keywords": item.get("keywords", item["product_name"].lower()),
            "source": "CUSTOM_CATALOG",
            "is_pinned": False,
        }
    for et, item in live_captured_products.items():
        catalog[int(et)] = {
            "etalase_no": int(et),
            "product_name": item["product_name"],
            "price_idr": int(item.get("price_idr", 0) or 0),
            "keywords": item["product_name"].lower(),
            "source": "TIKTOK_LIVE_PIN",
            "is_pinned": (current_pinned_etalase == int(et)),
        }
    return catalog

def broadcast_sse_event(event_type: str, payload: dict):
    """Mengirim event real-time ke seluruh subscriber SSE tanpa blocking."""
    if not sse_subscribers:
        return
    message = f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
    for q in list(sse_subscribers):
        try:
            q.put_nowait(message)
        except asyncio.QueueFull:
            try:
                q.get_nowait()
                q.put_nowait(message)
            except Exception:
                pass
        except Exception:
            pass

def compute_gift_delta(
    tracker: OrderedDict,
    user_id: str,
    gift_id: str,
    group_id: str,
    repeat_count: int,
    is_streakable: bool,
    repeat_end: bool,
    now_ts: Optional[float] = None,
    max_entries: int = 2000,
) -> int:
    """
    Menghitung selisih (delta) jumlah gift yang baru dikirim agar gift combo (Mawar, dll.)
    langsung masuk real-time tanpa menunggu repeat_end=1 dan tetap anti double-count.
    """
    repeat_count = max(1, int(repeat_count or 1))
    now_ts = time.monotonic() if now_ts is None else now_ts
    has_group = bool(group_id and str(group_id).strip() not in ("", "0", "None"))

    if not is_streakable:
        if has_group:
            key = f"non:{group_id}:{user_id}:{gift_id}"
            if key in tracker:
                return 0
            tracker[key] = (repeat_count, now_ts)
            while len(tracker) > max_entries:
                tracker.popitem(last=False)
        return repeat_count

    key = f"grp:{group_id}:{user_id}:{gift_id}" if has_group else f"usr:{user_id}:{gift_id}"
    prev_count, prev_ts = tracker.get(key, (0, 0.0))

    if not has_group and (now_ts - prev_ts > 8.0 or repeat_count < prev_count):
        prev_count = 0

    delta = repeat_count - prev_count
    if delta <= 0:
        return 0

    tracker[key] = (repeat_count, now_ts)
    tracker.move_to_end(key)
    while len(tracker) > max_entries:
        tracker.popitem(last=False)
    return delta

def ingest_and_process_event(raw_event: dict):
    """Memproses raw event di memori dalam <5ms tanpa Parquet/SQLite I/O di hot path."""
    global session_dirty_flag
    t0 = time.perf_counter()
    try:
        is_real_mode = (stream_mode == "REALTIME_TIKTOK_LIVE")
        live_cat = build_active_live_catalog(active_streamer) if is_real_mode else None

        processed = processor.process_single_event(
            raw_event,
            live_catalog=live_cat,
            strict_real_mode=is_real_mode,
        )
        if not processed:
            return None

        # Update buffer komentar O(1)
        recent_comments_buffer.appendleft(processed)

        # Update summary O(1)
        now_iso = processed.get("event_time") or datetime.now(timezone.utc).isoformat()
        session_summary["last_updated"] = now_iso
        evt_type = processed.get("event_type")

        if evt_type == "comment":
            session_summary["total_comments"] += 1
            intent = processed.get("ml_intent")
            if intent == "INTENT_BUY":
                session_summary["purchase_intent_count"] += 1
            elif intent == "QUESTION":
                session_summary["question_count"] += 1
            elif intent == "COMPLAINT":
                session_summary["complaint_count"] += 1

            et = processed.get("extracted_etalase")
            if et:
                et_int = int(et)
                pname = processed.get("matched_product_name") or f"Etalase #{et_int}"
                price = int(processed.get("matched_price_idr") or 0)
                psource = processed.get("product_source") or "CHAT_DETECTED"
                row = session_product_demand.get(et_int)
                if not row:
                    row = {
                        "etalase_no": et_int,
                        "product_name": pname,
                        "price_idr": price,
                        "request_count": 0,
                        "high_intent_count": 0,
                        "last_requested_at": now_iso,
                        "source": psource,
                        "is_pinned": (current_pinned_etalase == et_int),
                    }
                    session_product_demand[et_int] = row
                else:
                    # Perbarui jika sumber lebih otoritatif (pin live atau custom)
                    if psource in ("TIKTOK_LIVE_PIN", "CUSTOM_CATALOG") or row.get("source") == "CHAT_DETECTED":
                        row["product_name"] = pname
                        if price > 0:
                            row["price_idr"] = price
                        if psource in ("TIKTOK_LIVE_PIN", "CUSTOM_CATALOG"):
                            row["source"] = psource
                row["request_count"] += 1
                if intent == "INTENT_BUY":
                    row["high_intent_count"] += 1
                row["last_requested_at"] = now_iso

        elif evt_type == "gift":
            val_idr = int(processed.get("gift_value_idr") or 0)
            session_summary["total_gift_revenue_idr"] += val_idr
            uid = str(processed.get("user_id") or processed.get("nickname") or "anon")
            nick = str(processed.get("nickname") or "Sultan_TikTok")
            g_entry = session_top_gifters.get(uid)
            if not g_entry:
                session_top_gifters[uid] = {
                    "user_id": uid,
                    "nickname": nick,
                    "total_gift_idr": val_idr,
                    "gift_count": 1,
                    "last_gift_at": now_iso,
                }
            else:
                g_entry["nickname"] = nick
                g_entry["total_gift_idr"] += val_idr
                g_entry["gift_count"] += 1
                g_entry["last_gift_at"] = now_iso

        proc_ms = round((time.perf_counter() - t0) * 1000, 2)
        processed["process_latency_ms"] = proc_ms
        latency_samples_ms.append(proc_ms)

        pending_silver_batch.append(processed)
        session_dirty_flag = True

        broadcast_sse_event("live_event", {
            "session_id": active_session_id,
            "streamer": active_streamer,
            "stream_mode": stream_mode,
            "event": processed,
            "summary": dict(session_summary),
            "avg_latency_ms": get_avg_latency_ms(),
        })
        return processed
    except Exception as e:
        print(f"[INGEST ERROR] {e}")
    return None

def _flush_silver_and_gold_sync(batch: list[dict], snapshot: dict, clean_name: str, batch_seq: int):
    """Menulis micro-batch Parquet Silver & mengupsert Gold Marts di thread terpisah."""
    try:
        if batch:
            sess_id = snapshot.get("session_id", "session")
            parquet_filename = f"silver_{clean_name}_{sess_id}_{batch_seq:04d}.parquet"
            parquet_path = os.path.join(SILVER_OUTPUT_DIR, parquet_filename)
            pd.DataFrame(batch).to_parquet(parquet_path, engine="pyarrow", index=False)
        upsert_session_gold_snapshot(snapshot)
    except Exception as e:
        print(f"[BACKGROUND FLUSH ERROR] {e}")

async def background_persistence_worker():
    """Flusher asinkron yang memindahkan seluruh disk & DB I/O keluar dari event loop utama."""
    global silver_batch_seq, session_dirty_flag
    last_flush = time.monotonic()

    while True:
        await asyncio.sleep(0.5)
        now = time.monotonic()
        elapsed = now - last_flush
        has_batch = len(pending_silver_batch) >= 50 or (len(pending_silver_batch) > 0 and elapsed >= 2.0)
        needs_snapshot = session_dirty_flag and elapsed >= 2.0

        if not has_batch and not needs_snapshot:
            continue

        batch = pending_silver_batch[:]
        pending_silver_batch.clear()
        session_dirty_flag = False
        silver_batch_seq += 1
        seq = silver_batch_seq
        last_flush = now

        clean_name = active_streamer.replace("@", "").replace("/", "").replace("\\", "") or "store"
        snapshot = {
            "session_id": active_session_id,
            "streamer": active_streamer,
            "summary": dict(session_summary),
            "product_demand": [dict(v) for v in session_product_demand.values()],
            "top_gifters": [dict(v) for v in session_top_gifters.values()],
        }

        await asyncio.to_thread(_flush_silver_and_gold_sync, batch, snapshot, clean_name, seq)

async def force_disconnect_tiktok_client():
    """Memutus paksa koneksi TikTokLiveClient tanpa memicu run_until_complete crash di asyncio loop aktif."""
    global tiktok_client_instance, is_real_live_connected
    client = tiktok_client_instance
    tiktok_client_instance = None
    is_real_live_connected = False
    if not client:
        return
    try:
        if hasattr(client, "remove_all_listeners"):
            client.remove_all_listeners()
    except Exception:
        pass
    try:
        task = getattr(client, "_event_loop_task", None)
        if task and not task.done():
            task.cancel()
    except Exception:
        pass
    try:
        await asyncio.wait_for(client.disconnect(close_client=False), timeout=2.0)
    except Exception:
        try:
            if hasattr(client, "cancel"):
                client.cancel()
        except Exception:
            pass
    try:
        web_obj = getattr(client, "_web", None) or getattr(client, "web", None)
        if web_obj and hasattr(web_obj, "close"):
            await asyncio.wait_for(web_obj.close(), timeout=1.5)
    except Exception:
        pass

async def try_connect_tiktok_live(username: str) -> bool:
    """Mencoba menghubungkan TikTokLiveClient ke room live streaming TikTok yang sedang berlangsung."""
    global tiktok_client_instance, is_real_live_connected, stream_mode, current_pinned_etalase, live_room_title, session_dirty_flag
    if not TIKTOK_LIVE_AVAILABLE:
        print("[TIKTOK LIVE] Library TikTokLive tidak terpasang.")
        return False

    clean_user = username.lstrip("@").strip()
    if not clean_user:
        return False

    try:
        client = TikTokLiveClient(unique_id=clean_user)
        # Set locale ke Indonesia (100% gratis, tidak butuh EulerStream API key berbayar)
        # agar TikTok mengirimkan payload commerce / TikTok Shop wilayah ID.
        try:
            client.web.params.update({
                "region": "ID",
                "priority_region": "ID",
                "tz_name": "Asia/Jakarta",
                "app_language": "id",
                "webcast_language": "id",
                "browser_language": "id-ID",
            })
            client.web.headers["Accept-Language"] = "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7"
            tt_sess = os.getenv("TIKTOK_SESSION_ID", "").strip()
            if tt_sess:
                client.web.set_session(tt_sess)
        except Exception as cfg_err:
            print(f"[TIKTOK CONFIG WARN] {cfg_err}")

        last_room_stats_push = 0.0

        @client.on(CommentEvent)
        async def on_live_comment(event: CommentEvent):
            if not is_streaming_active or client is not tiktok_client_instance:
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

                msg_id = getattr(getattr(event, "common", None), "msg_id", None)
                evt_id = f"tt_{msg_id}" if msg_id else f"evt_{uuid.uuid4().hex[:10]}"

                raw = {
                    "event_id": evt_id,
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
            if not is_streaming_active or client is not tiktok_client_instance:
                return
            try:
                gift_name = "Gift"
                diamonds = 1
                gift_obj = getattr(event, "gift", None)
                if gift_obj:
                    gift_name = getattr(gift_obj, "name", "Gift")
                    diamonds = getattr(gift_obj, "diamond_count", 1)

                user_nick = "Sultan_TikTok"
                user_id = f"u_{random.randint(1000, 9999)}"
                if hasattr(event, "user") and event.user:
                    user_nick = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "Sultan_Live")
                    user_id = str(getattr(event.user, "id", None) or getattr(event.user, "user_id", user_id))

                gift_id = str(getattr(event, "gift_id", None) or getattr(gift_obj, "id", "0"))
                group_id = str(getattr(event, "group_id", "") or "")
                repeat_count = int(getattr(event, "repeat_count", 0) or getattr(event, "combo_count", 0) or 1)
                repeat_end = bool(getattr(event, "repeat_end", 0))

                is_streakable = getattr(gift_obj, "streakable", None) if gift_obj else None
                if is_streakable is None:
                    is_streakable = (getattr(gift_obj, "type", 0) == 1) if gift_obj else False

                delta = compute_gift_delta(
                    gift_streak_tracker,
                    user_id=user_id,
                    gift_id=gift_id,
                    group_id=group_id,
                    repeat_count=repeat_count,
                    is_streakable=bool(is_streakable),
                    repeat_end=repeat_end,
                )
                if delta <= 0:
                    return

                msg_id = getattr(getattr(event, "common", None), "msg_id", None)
                evt_id = f"ttg_{msg_id}_{repeat_count}" if msg_id else f"evt_{uuid.uuid4().hex[:10]}"

                raw = {
                    "event_id": evt_id,
                    "session_id": active_session_id,
                    "streamer": active_streamer,
                    "event_type": "gift",
                    "event_time": datetime.now(timezone.utc).isoformat(),
                    "data": {
                        "user_id": user_id,
                        "nickname": user_nick,
                        "gift_name": gift_name,
                        "diamond_count": diamonds,
                        "repeat_count": delta,
                        "combo_total": repeat_count,
                    }
                }
                ingest_and_process_event(raw)
            except Exception as e:
                print(f"[TIKTOK GIFT INGEST ERROR] {e}")

        @client.on(OecLiveShoppingEvent)
        async def on_oec_shopping(event: OecLiveShoppingEvent):
            """Menangkap produk etalase asli (v1 & v2) saat host pin/update keranjang di TikTok Live."""
            global current_pinned_etalase, session_dirty_flag
            if not is_streaming_active or client is not tiktok_client_instance:
                return
            try:
                act_type = getattr(event, "action_type", None)
                lpn = getattr(event, "live_product_number", None)
                has_pop = bool(getattr(event, "pop_product", None))
                has_v2 = bool(getattr(event, "oec_live_shopping_message_v2", None))
                print(f"[TIKTOK OEC EVENT] action_type={act_type} live_product_number={lpn} has_pop={has_pop} has_v2={has_v2}")

                products, pinned_no = extract_oec_shopping_products(event)
                if not products:
                    return

                if pinned_no is not None:
                    current_pinned_etalase = pinned_no
                    for row in session_product_demand.values():
                        row["is_pinned"] = (row["etalase_no"] == pinned_no)

                for prod in products:
                    et = int(prod["etalase_no"])
                    existing_cap = live_captured_products.get(et)
                    if existing_cap and prod["price_idr"] == 0 and existing_cap.get("price_idr", 0) > 0:
                        prod["price_idr"] = existing_cap["price_idr"]
                    live_captured_products[et] = prod

                    demand_row = session_product_demand.get(et)
                    if demand_row:
                        demand_row["product_name"] = prod["product_name"]
                        if prod["price_idr"] > 0:
                            demand_row["price_idr"] = prod["price_idr"]
                        demand_row["source"] = "TIKTOK_LIVE_PIN"
                        demand_row["is_pinned"] = (current_pinned_etalase == et)
                    else:
                        session_product_demand[et] = {
                            "etalase_no": et,
                            "product_name": prod["product_name"],
                            "price_idr": prod["price_idr"],
                            "request_count": 0,
                            "high_intent_count": 0,
                            "last_requested_at": None,
                            "source": "TIKTOK_LIVE_PIN",
                            "is_pinned": (current_pinned_etalase == et),
                        }

                for c in recent_comments_buffer:
                    et = c.get("extracted_etalase")
                    if et and int(et) in live_captured_products:
                        cap = live_captured_products[int(et)]
                        c["matched_product_name"] = cap["product_name"]
                        c["matched_price_idr"] = cap["price_idr"]
                        c["product_source"] = "TIKTOK_LIVE_PIN"

                session_dirty_flag = True
                pinned_item = live_captured_products.get(current_pinned_etalase) if current_pinned_etalase else products[0]
                print(f"[TIKTOK SHOPPING PIN] Etalase #{pinned_item['etalase_no']}: {pinned_item['product_name']} (Rp {pinned_item['price_idr']:,})")
                broadcast_sse_event("product_pin", {
                    "session_id": active_session_id,
                    "pinned_etalase": current_pinned_etalase,
                    "pinned_product": pinned_item,
                    "captured_count": len(live_captured_products),
                })
            except Exception as err:
                print(f"[TIKTOK SHOPPING INGEST ERROR] {err}")

        if RoomUserSeqEvent is not None:
            @client.on(RoomUserSeqEvent)
            async def on_room_users(event):
                nonlocal last_room_stats_push
                if not is_streaming_active or client is not tiktok_client_instance:
                    return
                try:
                    viewer_total = int(getattr(event, "total", 0) or getattr(event, "total_user", 0) or 0)
                    session_summary["viewer_count"] = viewer_total
                    now_m = time.monotonic()
                    if now_m - last_room_stats_push >= 1.0:
                        last_room_stats_push = now_m
                        broadcast_sse_event("room_stats", {
                            "session_id": active_session_id,
                            "viewer_count": viewer_total,
                        })
                except Exception:
                    pass

        @client.on(DisconnectEvent)
        async def on_disconnect(event: DisconnectEvent):
            global is_real_live_connected, stream_mode
            if client is not tiktok_client_instance:
                return
            print(f"[TIKTOK LIVE] Terputus dari live @{clean_user}. Status beralih ke OFFLINE.")
            is_real_live_connected = False
            stream_mode = "OFFLINE"

        @client.on(LiveEndEvent)
        async def on_live_end(event: LiveEndEvent):
            global is_real_live_connected, stream_mode
            if client is not tiktok_client_instance:
                return
            print(f"[TIKTOK LIVE] Siaran live @{clean_user} telah usai.")
            is_real_live_connected = False
            stream_mode = "OFFLINE"

        # Cek apakah akun sedang live dengan timeout 5.0 detik (1x roundtrip)
        is_live = await asyncio.wait_for(client.is_live(), timeout=5.0)
        if not is_live:
            print(f"[TIKTOK CHECK] Akun @{clean_user} saat ini tidak sedang siaran live.")
            return False

        # Pasang referensi client aktif sebelum start() agar initial events langsung diterima
        tiktok_client_instance = client
        await asyncio.wait_for(
            client.start(fetch_live_check=False, fetch_room_info=False, compress_ws_events=True),
            timeout=8.0
        )
        is_real_live_connected = True
        stream_mode = "REALTIME_TIKTOK_LIVE"
        print(f"[TIKTOK LIVE] Berhasil tersambung ke WebSocket live stream @{clean_user}!")

        # Ambil metadata room (judul, penonton awal, & produk commerce bila tersedia) secara non-blocking
        async def _fetch_room_meta():
            global live_room_title, current_pinned_etalase, room_has_commerce_goods, room_commerce_restricted, session_dirty_flag
            try:
                if client is not tiktok_client_instance:
                    return
                info = await asyncio.wait_for(client.web.fetch_room_info(), timeout=4.0)
                if client is not tiktok_client_instance:
                    return
                if isinstance(info, dict):
                    if info.get("title"):
                        live_room_title = str(info["title"])
                    if info.get("user_count"):
                        session_summary["viewer_count"] = int(info["user_count"])

                    room_has_commerce_goods = bool(info.get("has_commerce_goods"))
                    c_info = info.get("commerce_info") if isinstance(info.get("commerce_info"), dict) else {}
                    room_products = extract_room_info_products(info)
                    room_commerce_restricted = bool(
                        room_has_commerce_goods
                        and c_info.get("commerce_permission", 0) == 0
                        and not room_products
                    )
                    print(f"[TIKTOK ROOM INFO] title={live_room_title!r} viewers={session_summary['viewer_count']} has_commerce_goods={room_has_commerce_goods} commerce_permission={c_info.get('commerce_permission')} restricted={room_commerce_restricted}")
                    if room_products:
                        for prod in room_products:
                            et = int(prod["etalase_no"])
                            if et not in live_captured_products:
                                live_captured_products[et] = prod
                            if et not in session_product_demand:
                                session_product_demand[et] = {
                                    "etalase_no": et,
                                    "product_name": prod["product_name"],
                                    "price_idr": prod["price_idr"],
                                    "request_count": 0,
                                    "high_intent_count": 0,
                                    "last_requested_at": None,
                                    "source": "TIKTOK_LIVE_PIN",
                                    "is_pinned": prod.get("is_pinned", False),
                                }
                            if prod.get("is_pinned") and current_pinned_etalase is None:
                                current_pinned_etalase = et
                        session_dirty_flag = True
                        pinned_item = live_captured_products.get(current_pinned_etalase) or room_products[0]
                        print(f"[TIKTOK ROOM SHOP] Menangkap {len(room_products)} produk awal dari room_info!")
                        broadcast_sse_event("product_pin", {
                            "session_id": active_session_id,
                            "pinned_etalase": current_pinned_etalase,
                            "pinned_product": pinned_item,
                            "captured_count": len(live_captured_products),
                        })

                    broadcast_sse_event("room_stats", {
                        "session_id": active_session_id,
                        "viewer_count": session_summary["viewer_count"],
                        "room_title": live_room_title,
                    })
            except Exception as meta_err:
                print(f"[TIKTOK ROOM INFO WARN] {meta_err}")

        asyncio.create_task(_fetch_room_meta())
        return True

    except Exception as e:
        if tiktok_client_instance is client:
            tiktok_client_instance = None
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

    asyncio.create_task(background_persistence_worker())
    asyncio.create_task(continuous_stream_worker())

@app.on_event("shutdown")
async def shutdown_event():
    await force_disconnect_tiktok_client()

# ========================================================
# REST API & SSE ENDPOINTS
# ========================================================

@app.get("/api/v1/live/stream", tags=["Stream"])
async def stream_live_events(role: str = Header("viewer")):
    """Server-Sent Events (SSE) push stream untuk update chat, gift, dan pin produk sub-10ms."""
    verify_access(role, "viewer")
    queue: asyncio.Queue = asyncio.Queue(maxsize=256)
    sse_subscribers.add(queue)

    async def event_generator():
        try:
            init_payload = {
                "session_id": active_session_id,
                "streamer": active_streamer,
                "stream_mode": stream_mode,
                "is_real_live": is_real_live_connected,
                "summary": dict(session_summary),
                "pinned_etalase": current_pinned_etalase,
                "avg_latency_ms": get_avg_latency_ms(),
            }
            yield f"event: session_state\ndata: {json.dumps(init_payload, ensure_ascii=False)}\n\n"

            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            sse_subscribers.discard(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

@app.post("/api/v1/admin/reset-session", tags=["Admin"])
def admin_reset_session(
    role: Optional[str] = Header("viewer", alias="role"),
    x_user_role: Optional[str] = Header(None, alias="X-User-Role")
):
    """Endpoint khusus peran Admin untuk mereset sesi live streaming (F-16)."""
    user_role = x_user_role or role or "viewer"
    if user_role.lower() != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Akses ditolak: Hanya peran 'admin' yang diizinkan.")
    reset_active_session_state()
    return {"status": "SUCCESS", "message": "Sesi streaming berhasil direset oleh Admin."}

class SwitchSessionRequest(BaseModel):
    username: str
    niche_override: Optional[str] = None

@app.post("/api/v1/session/switch", tags=["Session"])
async def switch_session(req: SwitchSessionRequest, role: str = Header("viewer")):
    """Beralih ke akun TikTok Live lain. Jika akun sedang live, otomatis menyerap komentar dan etalase asli secara real-time."""
    global active_streamer, active_session_id, active_niche
    global tiktok_client_instance, is_real_live_connected, stream_mode, is_simulation_enabled
    verify_access(role, "viewer")

    clean_user = req.username.strip()
    if not clean_user.startswith("@"):
        clean_user = f"@{clean_user}"

    # Putus paksa koneksi TikTok Live sebelumnya agar tidak stuck di room ramai
    await force_disconnect_tiktok_client()
    is_simulation_enabled = False  # Reset simulasi saat ganti toko
    is_streaming_active = True

    active_streamer = clean_user
    active_session_id = f"sess_{int(time.time())}_{clean_user.replace('@', '')}"
    reset_active_session_state()

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
        msg = f"🟢 Berhasil terhubung LANGSUNG ke siaran TikTok Live {active_streamer}! Komentar, saweran, dan pin etalase mengalir 100% Real-Time."
    else:
        stream_mode = "OFFLINE"
        msg = f"🔴 Toko {active_streamer} sedang tidak Live di TikTok. Menunggu siaran langsung dimulai."

    broadcast_sse_event("session_state", {
        "session_id": active_session_id,
        "streamer": active_streamer,
        "stream_mode": stream_mode,
        "is_real_live": connected_real,
        "summary": dict(session_summary),
        "pinned_etalase": current_pinned_etalase,
        "avg_latency_ms": get_avg_latency_ms(),
    })

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

@app.post("/api/v1/session/disconnect", tags=["Session"])
async def disconnect_session(role: str = Header("viewer")):
    """Memutus koneksi live streaming TikTok secara penuh (Tombol OFF) dan mengembalikan status ke OFFLINE."""
    global stream_mode, is_simulation_enabled, is_real_live_connected
    verify_access(role, "viewer")

    await force_disconnect_tiktok_client()
    is_simulation_enabled = False
    is_real_live_connected = False
    stream_mode = "OFFLINE"

    broadcast_sse_event("session_state", {
        "session_id": active_session_id,
        "streamer": active_streamer,
        "stream_mode": stream_mode,
        "is_real_live": False,
        "summary": dict(session_summary),
        "pinned_etalase": current_pinned_etalase,
        "avg_latency_ms": get_avg_latency_ms(),
    })

    return {
        "status": "DISCONNECTED",
        "streamer": active_streamer,
        "session_id": active_session_id,
        "stream_mode": stream_mode,
        "is_real_live": False,
        "message": "⏹️ Koneksi live diputus. Sistem dalam posisi standby (OFFLINE)."
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
    """Mengambil aliran komentar live terbaru khusus toko aktif dari buffer in-memory."""
    verify_access(role, "viewer")
    comments_list = list(recent_comments_buffer)[:50]
    return {
        "streamer": active_streamer,
        "session_id": active_session_id,
        "stream_mode": stream_mode,
        "is_real_live": is_real_live_connected,
        "is_streaming": is_streaming_active,
        "is_simulation_enabled": is_simulation_enabled,
        "viewer_count": session_summary.get("viewer_count", 0),
        "avg_latency_ms": get_avg_latency_ms(),
        "total_buffered": len(recent_comments_buffer),
        "comments": comments_list
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
        "viewer_count": session_summary.get("viewer_count", 0),
        "avg_latency_ms": get_avg_latency_ms(),
        "live_captured_products_count": len(live_captured_products),
        "sse_clients": len(sse_subscribers),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/api/v1/live/summary", tags=["Analytics Marts"])
def get_live_summary(role: str = Header("viewer")):
    """Mengambil ringkasan metrik live KHUSUS sesi toko aktif dari state in-memory (<1ms)."""
    verify_access(role, "viewer")
    try:
        avg_lat = get_avg_latency_ms()
        has_memory_data = (
            session_summary["total_comments"] > 0
            or session_summary["total_gift_revenue_idr"] > 0
            or len(recent_comments_buffer) > 0
        )

        if has_memory_data or stream_mode != "OFFLINE":
            data_dict = {
                "session_id": active_session_id,
                "streamer": active_streamer,
                "stream_mode": stream_mode,
                "is_real_live": is_real_live_connected,
                "is_simulation_enabled": is_simulation_enabled,
                "niche": active_niche,
                "total_comments": session_summary["total_comments"],
                "total_gift_revenue_idr": session_summary["total_gift_revenue_idr"],
                "purchase_intent_count": session_summary["purchase_intent_count"],
                "question_count": session_summary["question_count"],
                "complaint_count": session_summary["complaint_count"],
                "viewer_count": session_summary["viewer_count"],
                "avg_latency_ms": avg_lat,
                "pinned_etalase": current_pinned_etalase,
                "live_room_title": live_room_title,
                "last_updated": session_summary["last_updated"],
            }
            return {
                "data": data_dict,
                "stream_mode": stream_mode,
                "is_real_live": is_real_live_connected,
                "is_simulation_enabled": is_simulation_enabled,
                "is_streaming": is_streaming_active,
                "avg_latency_ms": avg_lat,
                "viewer_count": session_summary["viewer_count"],
                "freshness": "REAL_TIME" if is_real_live_connected else ("SIMULATION" if is_simulation_enabled else "STANDBY"),
                "retrieved_at": datetime.now(timezone.utc).isoformat()
            }

        # Fallback ke Gold DB bila belum ada data di memory dan mode OFFLINE
        with DatabaseContext() as db:
            cur = db.execute_query(
                "SELECT * FROM mart_live_summary WHERE session_id = %s ORDER BY last_updated DESC LIMIT 1;",
                (active_session_id,)
            )
            row = cur.fetchone()
            if row:
                res_dict = dict(row)
                res_dict.update({
                    "niche": active_niche,
                    "stream_mode": stream_mode,
                    "is_real_live": is_real_live_connected,
                    "is_simulation_enabled": is_simulation_enabled,
                    "viewer_count": session_summary["viewer_count"],
                    "avg_latency_ms": avg_lat,
                    "pinned_etalase": current_pinned_etalase,
                    "live_room_title": live_room_title,
                })
                return {
                    "data": res_dict,
                    "stream_mode": stream_mode,
                    "is_real_live": is_real_live_connected,
                    "is_simulation_enabled": is_simulation_enabled,
                    "is_streaming": is_streaming_active,
                    "avg_latency_ms": avg_lat,
                    "viewer_count": session_summary["viewer_count"],
                    "freshness": "STANDBY",
                    "retrieved_at": datetime.now(timezone.utc).isoformat()
                }

        return {
            "data": {
                "session_id": active_session_id,
                "streamer": active_streamer,
                "stream_mode": stream_mode,
                "is_real_live": is_real_live_connected,
                "is_simulation_enabled": is_simulation_enabled,
                "niche": active_niche,
                "total_comments": 0,
                "total_gift_revenue_idr": 0,
                "purchase_intent_count": 0,
                "question_count": 0,
                "complaint_count": 0,
                "viewer_count": session_summary["viewer_count"],
                "avg_latency_ms": avg_lat,
                "pinned_etalase": current_pinned_etalase,
                "live_room_title": live_room_title,
            },
            "stream_mode": stream_mode,
            "is_real_live": is_real_live_connected,
            "is_simulation_enabled": is_simulation_enabled,
            "avg_latency_ms": avg_lat,
            "viewer_count": session_summary["viewer_count"],
            "message": "Toko sedang tidak live." if not is_real_live_connected else "Siaran live aktif."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@app.get("/api/v1/products/demand", tags=["Analytics Marts"])
def get_product_demand(
    limit: int = Query(5, ge=1, le=50),
    role: str = Header("viewer")
):
    """Mengambil leaderboard permintaan produk KHUSUS sesi aktif dengan sumber data yang jujur & akurat."""
    verify_access(role, "viewer")
    try:
        demand_rows_map: dict[int, dict] = {}

        if stream_mode == "REALTIME_TIKTOK_LIVE":
            # Mode Real Live: HANYA pakai produk dari pin TikTok Live asli + custom catalog user + chat terdeteksi
            live_cat = build_active_live_catalog(active_streamer)
            for et, info in live_cat.items():
                demand_rows_map[et] = {
                    "etalase_no": et,
                    "product_name": info["product_name"],
                    "price_idr": int(info.get("price_idr", 0) or 0),
                    "request_count": 0,
                    "high_intent_count": 0,
                    "last_requested_at": None,
                    "source": info.get("source", "TIKTOK_LIVE_PIN"),
                    "is_pinned": (current_pinned_etalase == et),
                }

            for et, d in session_product_demand.items():
                existing = demand_rows_map.get(et)
                if existing:
                    existing["request_count"] = d["request_count"]
                    existing["high_intent_count"] = d["high_intent_count"]
                    existing["last_requested_at"] = d["last_requested_at"]
                else:
                    demand_rows_map[et] = dict(d)

            sorted_demand = sorted(
                demand_rows_map.values(),
                key=lambda x: (
                    int(x.get("request_count", 0)),
                    int(x.get("high_intent_count", 0)),
                    1 if x.get("is_pinned") else 0,
                    -int(x.get("etalase_no", 999))
                ),
                reverse=True
            )
            final_leaderboard = list(sorted_demand[:limit])

            # Pastikan item yang sedang di-pin selalu terlihat di leaderboard
            if current_pinned_etalase and current_pinned_etalase in demand_rows_map:
                if not any(r["etalase_no"] == current_pinned_etalase for r in final_leaderboard):
                    pinned_row = demand_rows_map[current_pinned_etalase]
                    if len(final_leaderboard) >= limit and final_leaderboard:
                        final_leaderboard[-1] = pinned_row
                    else:
                        final_leaderboard.append(pinned_row)

            catalog_mode = "REAL_LIVE_ONLY"

        elif stream_mode == "SIMULATION_FALLBACK":
            # Mode Simulasi Demo: Boleh pad dari template niche agar demo presentasi penuh
            cat_df = get_store_catalog(active_streamer, niche_override=active_niche)
            has_custom = has_custom_store_catalog(active_streamer)
            cat_source = "CUSTOM_CATALOG" if has_custom else "SIMULATION_CATALOG"
            if not cat_df.empty:
                for _, r in cat_df.iterrows():
                    et = int(r["etalase_no"])
                    demand_rows_map[et] = {
                        "etalase_no": et,
                        "product_name": str(r["product_name"]),
                        "price_idr": int(r["price_idr"]),
                        "request_count": 0,
                        "high_intent_count": 0,
                        "last_requested_at": None,
                        "source": cat_source,
                        "is_pinned": False,
                    }
            for et, d in session_product_demand.items():
                if et in demand_rows_map:
                    demand_rows_map[et]["request_count"] = d["request_count"]
                    demand_rows_map[et]["high_intent_count"] = d["high_intent_count"]
                    demand_rows_map[et]["last_requested_at"] = d["last_requested_at"]
                else:
                    demand_rows_map[et] = dict(d)

            sorted_demand = sorted(
                demand_rows_map.values(),
                key=lambda x: (int(x.get("request_count", 0)), int(x.get("high_intent_count", 0)), -int(x.get("etalase_no", 999))),
                reverse=True
            )
            final_leaderboard = list(sorted_demand[:limit])
            catalog_mode = "SIMULATION_TEMPLATE"

        else:
            # Mode OFFLINE: Gunakan custom catalog user (bila ada) + data sesi yang ada di memory/DB tanpa katalog palsu
            for item in load_custom_store_catalog(active_streamer):
                et = int(item["etalase_no"])
                demand_rows_map[et] = {
                    "etalase_no": et,
                    "product_name": item["product_name"],
                    "price_idr": int(item.get("price_idr", 0) or 0),
                    "request_count": 0,
                    "high_intent_count": 0,
                    "last_requested_at": None,
                    "source": "CUSTOM_CATALOG",
                    "is_pinned": False,
                }
            for et, d in session_product_demand.items():
                if et in demand_rows_map:
                    demand_rows_map[et]["request_count"] = d["request_count"]
                    demand_rows_map[et]["high_intent_count"] = d["high_intent_count"]
                    demand_rows_map[et]["last_requested_at"] = d["last_requested_at"]
                else:
                    demand_rows_map[et] = dict(d)

            if not demand_rows_map:
                with DatabaseContext() as db:
                    cur = db.execute_query("""
                        SELECT etalase_no, product_name, price_idr, request_count, high_intent_count, last_requested_at
                        FROM mart_product_demand
                        WHERE session_id = %s
                        ORDER BY request_count DESC, high_intent_count DESC
                        LIMIT %s;
                    """, (active_session_id, limit))
                    for r in cur.fetchall():
                        d = dict(r)
                        et = int(d["etalase_no"])
                        d["source"] = "CUSTOM_CATALOG" if has_custom_store_catalog(active_streamer) else "DB_SESSION"
                        d["is_pinned"] = False
                        demand_rows_map[et] = d

            sorted_demand = sorted(
                demand_rows_map.values(),
                key=lambda x: (int(x.get("request_count", 0)), int(x.get("high_intent_count", 0)), -int(x.get("etalase_no", 999))),
                reverse=True
            )
            final_leaderboard = list(sorted_demand[:limit])
            catalog_mode = "OFFLINE_CUSTOM_ONLY"

        pinned_product = live_captured_products.get(current_pinned_etalase) if current_pinned_etalase else None
        return {
            "total_products": len(final_leaderboard),
            "streamer": active_streamer,
            "session_id": active_session_id,
            "stream_mode": stream_mode,
            "catalog_mode": catalog_mode,
            "is_real_live": is_real_live_connected,
            "is_simulation_enabled": is_simulation_enabled,
            "live_captured_count": len(live_captured_products),
            "current_pinned_etalase": current_pinned_etalase,
            "pinned_product": pinned_product,
            "has_commerce_goods": room_has_commerce_goods,
            "commerce_restricted": room_commerce_restricted,
            "niche": active_niche,
            "leaderboard": final_leaderboard,
            "query_time": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@app.get("/api/v1/gifters/leaderboard", tags=["Analytics Marts"])
def get_top_gifters(limit: int = Query(5, ge=1, le=20), role: str = Header("viewer")):
    """Mengambil leaderboard saweran Sultan KHUSUS sesi toko aktif dari memory (<1ms) atau DB."""
    verify_access(role, "viewer")
    try:
        if session_top_gifters:
            rows = sorted(
                session_top_gifters.values(),
                key=lambda x: x["total_gift_idr"],
                reverse=True
            )[:limit]
        else:
            with DatabaseContext() as db:
                cur = db.execute_query("""
                    SELECT nickname, total_gift_idr, gift_count, last_gift_at
                    FROM mart_top_gifters
                    WHERE session_id = %s
                    ORDER BY total_gift_idr DESC
                    LIMIT %s;
                """, (active_session_id, limit))
                rows = [dict(r) for r in cur.fetchall()]

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
    intent_tags: List[str] = Field(default_factory=list)
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
        intent, score, tags = processor.classify_comment_intent(req.comment_text)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gagal melakukan inferensi: {str(e)}")

    now_iso = datetime.now(timezone.utc).isoformat()
    return PredictionResponse(
        comment_text=req.comment_text,
        predicted_intent=intent,
        intent_tags=tags,
        confidence_score=round(float(score), 4),
        model_version="v2.0-hybrid-multitag",
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
def get_catalog_endpoint(
    streamer: Optional[str] = Query(None),
    niche_override: Optional[str] = Query(None),
    mode: Optional[str] = Query(None, description="Gunakan 'live' untuk hanya memuat produk pin live & katalog kustom tanpa template dummy."),
    role: str = Header("viewer")
):
    """Mengambil katalog produk toko (prioritas: pin live TikTok -> katalog kustom -> template)."""
    verify_access(role, "viewer")
    target_streamer = streamer or active_streamer
    target_niche = niche_override or (active_niche if target_streamer == active_streamer else detect_store_niche(target_streamer))

    if mode == "live":
        merged = {}
        for item in load_custom_store_catalog(target_streamer):
            et = int(item["etalase_no"])
            merged[et] = {**item, "source": "CUSTOM_CATALOG"}
        if target_streamer.lower().lstrip("@") == active_streamer.lower().lstrip("@"):
            for et, item in live_captured_products.items():
                merged[int(et)] = {**item, "source": "TIKTOK_LIVE_PIN"}
        products = sorted(merged.values(), key=lambda x: x["etalase_no"])
        return {
            "streamer": target_streamer,
            "niche": target_niche,
            "mode": "live",
            "total_products": len(products),
            "products": products
        }

    df = get_store_catalog(target_streamer, niche_override=target_niche)
    has_custom = has_custom_store_catalog(target_streamer)
    default_source = "CUSTOM_CATALOG" if has_custom else "TEMPLATE_CATALOG"
    products = df.to_dict(orient="records") if not df.empty else []
    for p in products:
        p.setdefault("source", default_source)

    if (
        stream_mode == "REALTIME_TIKTOK_LIVE"
        and target_streamer.lower().lstrip("@") == active_streamer.lower().lstrip("@")
        and live_captured_products
    ):
        by_et = {int(p["etalase_no"]): p for p in products}
        for et, live_p in live_captured_products.items():
            by_et[int(et)] = {**live_p, "source": "TIKTOK_LIVE_PIN"}
        products = sorted(by_et.values(), key=lambda x: int(x["etalase_no"]))

    return {
        "streamer": target_streamer,
        "niche": target_niche,
        "total_products": len(products),
        "products": products
    }

@app.post("/api/v1/catalog/update", tags=["Store Catalog"])
def update_catalog_endpoint(req: UpdateCatalogRequest, role: str = Header("viewer")):
    """Menyimpan pembaruan katalog produk toko dan memperbarui state in-memory & DB secara instan."""
    global session_dirty_flag
    verify_access(role, "viewer")
    target_streamer = req.streamer or active_streamer
    items = [p.model_dump() if hasattr(p, "model_dump") else p.dict() for p in req.products]
    save_custom_store_catalog(target_streamer, items)

    # Sinkronisasi ke in-memory demand & comment buffer
    if target_streamer.lower().lstrip("@") == active_streamer.lower().lstrip("@"):
        for p in req.products:
            if p.etalase_no in session_product_demand:
                session_product_demand[p.etalase_no]["product_name"] = p.product_name
                session_product_demand[p.etalase_no]["price_idr"] = p.price_idr
                session_product_demand[p.etalase_no]["source"] = "CUSTOM_CATALOG"
        session_dirty_flag = True

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
                    c["product_source"] = "CUSTOM_CATALOG"

    broadcast_sse_event("catalog_updated", {
        "streamer": target_streamer,
        "total_updated": len(items),
    })

    return {
        "status": "success",
        "message": f"Katalog untuk {target_streamer} berhasil diperbarui ({len(items)} produk).",
        "streamer": target_streamer,
        "total_updated": len(items)
    }

@app.delete("/api/v1/catalog/custom", tags=["Store Catalog"])
def delete_custom_catalog_endpoint(streamer: Optional[str] = Query(None), role: str = Header("viewer")):
    """Menghapus file katalog kustom tersimpan untuk suatu akun agar kembali bersih."""
    verify_access(role, "viewer")
    target_streamer = streamer or active_streamer
    removed = delete_custom_store_catalog(target_streamer)
    broadcast_sse_event("catalog_updated", {"streamer": target_streamer, "deleted": removed})
    return {
        "status": "success",
        "streamer": target_streamer,
        "deleted": removed,
        "message": f"Katalog kustom untuk {target_streamer} {'berhasil dihapus' if removed else 'memang tidak ada'}."
    }
