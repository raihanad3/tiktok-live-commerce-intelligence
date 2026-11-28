"""
TikPulse - Medallion Silver to Gold Aggregator (F-06, F-07, F-10)
Fungsi:
  Mengubah data transaksi Silver Parquet menjadi 4 Analytical Marts dengan isolasi penuh per sesi siaran:
    1. mart_live_summary     : Ringkasan metrik live stream per sesi (Total chat, saweran Rp, sentimen)
    2. mart_product_demand   : Peringkat etalase per sesi (Isolasi per streamer)
    3. mart_top_gifters      : Leaderboard penonton per sesi (Isolasi per streamer)
    4. mart_niche_benchmark  : Agregasi Batch lintas-toko per niche (Rate Niat Beli, Jam Emas WIB, Topik & Komplain Pasar)
"""

import os
import re
import sys
import json
import glob
import sqlite3
from collections import Counter
import pandas as pd
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.store_catalog import detect_store_niche

# Konfigurasi Database PostgreSQL
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "tikpulse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "tikpulse_admin")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "tikpulse_secure_password_2026")

SILVER_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "silver")
GOLD_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "gold")
SQLITE_DB_PATH = os.path.join(GOLD_DIR, "tikpulse_gold.db")
os.makedirs(GOLD_DIR, exist_ok=True)

IS_POSTGRES_AVAILABLE = None

def get_db_connection():
    """Mencoba koneksi PostgreSQL, jika offline beralih cepat ke SQLite ACID storage (dicache agar bebas latensi)."""
    global IS_POSTGRES_AVAILABLE
    if IS_POSTGRES_AVAILABLE is None:
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASS,
                connect_timeout=1
            )
            IS_POSTGRES_AVAILABLE = True
            return conn, "POSTGRES"
        except Exception:
            IS_POSTGRES_AVAILABLE = False
            conn = sqlite3.connect(SQLITE_DB_PATH)
            return conn, "SQLITE"
    elif IS_POSTGRES_AVAILABLE:
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASS,
                connect_timeout=1
            )
            return conn, "POSTGRES"
        except Exception:
            IS_POSTGRES_AVAILABLE = False
            conn = sqlite3.connect(SQLITE_DB_PATH)
            return conn, "SQLITE"
    else:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        return conn, "SQLITE"

def init_gold_tables(conn, db_type="SQLITE"):
    """Inisialisasi skema tabel analitik Gold Mart dengan session-scoped keys."""
    cur = conn.cursor()
    
    if db_type == "POSTGRES":
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_live_summary (
                session_id VARCHAR(100) PRIMARY KEY,
                streamer VARCHAR(100),
                total_comments INT DEFAULT 0,
                total_gift_revenue_idr BIGINT DEFAULT 0,
                purchase_intent_count INT DEFAULT 0,
                question_count INT DEFAULT 0,
                complaint_count INT DEFAULT 0,
                last_updated TIMESTAMP WITH TIME ZONE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_product_demand (
                session_id VARCHAR(100),
                streamer VARCHAR(100),
                etalase_no INT,
                product_name VARCHAR(255),
                price_idr INT,
                request_count INT DEFAULT 0,
                high_intent_count INT DEFAULT 0,
                last_requested_at TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (session_id, etalase_no)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_top_gifters (
                session_id VARCHAR(100),
                streamer VARCHAR(100),
                user_id VARCHAR(100),
                nickname VARCHAR(100),
                total_gift_idr BIGINT DEFAULT 0,
                gift_count INT DEFAULT 0,
                last_gift_at TIMESTAMP WITH TIME ZONE,
                PRIMARY KEY (session_id, user_id)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_niche_benchmark (
                niche VARCHAR(50) PRIMARY KEY,
                total_comments INT DEFAULT 0,
                total_sessions INT DEFAULT 0,
                intent_buy_rate_pct DOUBLE PRECISION DEFAULT 0.0,
                question_rate_pct DOUBLE PRECISION DEFAULT 0.0,
                complaint_rate_pct DOUBLE PRECISION DEFAULT 0.0,
                golden_hour_wib VARCHAR(50),
                top_keywords TEXT,
                top_complaints TEXT,
                last_updated TIMESTAMP WITH TIME ZONE
            );
        """)
    else:
        # SQLite Syntax - Pastikan skema memiliki session_id
        cur.execute("PRAGMA table_info(mart_product_demand);")
        cols_demand = [r[1] for r in cur.fetchall()]
        if cols_demand and "session_id" not in cols_demand:
            cur.execute("DROP TABLE IF EXISTS mart_product_demand;")

        cur.execute("PRAGMA table_info(mart_top_gifters);")
        cols_gift = [r[1] for r in cur.fetchall()]
        if cols_gift and "session_id" not in cols_gift:
            cur.execute("DROP TABLE IF EXISTS mart_top_gifters;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_live_summary (
                session_id TEXT PRIMARY KEY,
                streamer TEXT,
                total_comments INTEGER DEFAULT 0,
                total_gift_revenue_idr INTEGER DEFAULT 0,
                purchase_intent_count INTEGER DEFAULT 0,
                question_count INTEGER DEFAULT 0,
                complaint_count INTEGER DEFAULT 0,
                last_updated TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_product_demand (
                session_id TEXT,
                streamer TEXT,
                etalase_no INTEGER,
                product_name TEXT,
                price_idr INTEGER,
                request_count INTEGER DEFAULT 0,
                high_intent_count INTEGER DEFAULT 0,
                last_requested_at TEXT,
                PRIMARY KEY (session_id, etalase_no)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_top_gifters (
                session_id TEXT,
                streamer TEXT,
                user_id TEXT,
                nickname TEXT,
                total_gift_idr INTEGER DEFAULT 0,
                gift_count INTEGER DEFAULT 0,
                last_gift_at TEXT,
                PRIMARY KEY (session_id, user_id)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mart_niche_benchmark (
                niche TEXT PRIMARY KEY,
                total_comments INTEGER DEFAULT 0,
                total_sessions INTEGER DEFAULT 0,
                intent_buy_rate_pct REAL DEFAULT 0.0,
                question_rate_pct REAL DEFAULT 0.0,
                complaint_rate_pct REAL DEFAULT 0.0,
                golden_hour_wib TEXT,
                top_keywords TEXT,
                top_complaints TEXT,
                last_updated TEXT
            );
        """)

    conn.commit()

def aggregate_silver_to_gold():
    """Membaca file Parquet Silver dan mengupdate Marts di Database dengan isolasi ketat per sesi."""
    parquet_files = glob.glob(os.path.join(SILVER_DIR, "*.parquet"))
    if not parquet_files:
        return

    df_list = []
    for f in parquet_files:
        try:
            df_list.append(pd.read_parquet(f))
        except Exception:
            pass

    if not df_list:
        return

    df_all = pd.concat(df_list, ignore_index=True)
    if df_all.empty:
        return

    conn, db_type = get_db_connection()
    init_gold_tables(conn, db_type)
    cur = conn.cursor()

    # 1. Update mart_product_demand (Dikelompokkan per session_id dan etalase_no)
    df_demand = df_all[df_all["extracted_etalase"].notna()].copy()
    if not df_demand.empty:
        for (session_id, etalase), group in df_demand.groupby(["session_id", "extracted_etalase"]):
            etalase_int = int(etalase)
            streamer = str(group["streamer"].dropna().iloc[0]) if not group["streamer"].dropna().empty else "unknown"
            prod_name = group["matched_product_name"].dropna().iloc[0] if not group["matched_product_name"].dropna().empty else f"Etalase #{etalase_int}"
            price = int(group["matched_price_idr"].dropna().iloc[0]) if not group["matched_price_idr"].dropna().empty else 0
            req_count = len(group)
            intent_count = len(group[group["ml_intent"] == "INTENT_BUY"])
            last_req = str(group["event_time"].max())

            if db_type == "POSTGRES":
                cur.execute("""
                    INSERT INTO mart_product_demand (session_id, streamer, etalase_no, product_name, price_idr, request_count, high_intent_count, last_requested_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (session_id, etalase_no) DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        price_idr = EXCLUDED.price_idr,
                        request_count = EXCLUDED.request_count,
                        high_intent_count = EXCLUDED.high_intent_count,
                        last_requested_at = EXCLUDED.last_requested_at;
                """, (session_id, streamer, etalase_int, prod_name, price, req_count, intent_count, last_req))
            else:
                cur.execute("""
                    INSERT INTO mart_product_demand (session_id, streamer, etalase_no, product_name, price_idr, request_count, high_intent_count, last_requested_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (session_id, etalase_no) DO UPDATE SET
                        product_name = excluded.product_name,
                        price_idr = excluded.price_idr,
                        request_count = excluded.request_count,
                        high_intent_count = excluded.high_intent_count,
                        last_requested_at = excluded.last_requested_at;
                """, (session_id, streamer, etalase_int, prod_name, price, req_count, intent_count, last_req))

    # 2. Update mart_top_gifters (Dikelompokkan per session_id dan user_id)
    df_gifts = df_all[df_all["event_type"] == "gift"].copy()
    if not df_gifts.empty:
        for (session_id, uid), group in df_gifts.groupby(["session_id", "user_id"]):
            streamer = str(group["streamer"].dropna().iloc[0]) if not group["streamer"].dropna().empty else "unknown"
            nick = str(group["nickname"].iloc[0])
            tot_idr = int(group["gift_value_idr"].sum())
            cnt = len(group)
            last_at = str(group["event_time"].max())

            if db_type == "POSTGRES":
                cur.execute("""
                    INSERT INTO mart_top_gifters (session_id, streamer, user_id, nickname, total_gift_idr, gift_count, last_gift_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (session_id, user_id) DO UPDATE SET
                        total_gift_idr = EXCLUDED.total_gift_idr,
                        gift_count = EXCLUDED.gift_count,
                        last_gift_at = EXCLUDED.last_gift_at;
                """, (session_id, streamer, str(uid), nick, tot_idr, cnt, last_at))
            else:
                cur.execute("""
                    INSERT INTO mart_top_gifters (session_id, streamer, user_id, nickname, total_gift_idr, gift_count, last_gift_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (session_id, user_id) DO UPDATE SET
                        total_gift_idr = excluded.total_gift_idr,
                        gift_count = excluded.gift_count,
                        last_gift_at = excluded.last_gift_at;
                """, (session_id, streamer, str(uid), nick, tot_idr, cnt, last_at))

    # 3. Update mart_live_summary per sesi
    now_str = datetime.now(timezone.utc).isoformat()
    for session_id, df_session in df_all.groupby("session_id"):
        streamer = str(df_session["streamer"].dropna().iloc[-1]) if not df_session["streamer"].dropna().empty else "unknown"
        total_comments = len(df_session[df_session["event_type"] == "comment"])
        total_rev = int(df_session["gift_value_idr"].sum())
        buy_cnt = len(df_session[df_session["ml_intent"] == "INTENT_BUY"])
        quest_cnt = len(df_session[df_session["ml_intent"] == "QUESTION"])
        comp_cnt = len(df_session[df_session["ml_intent"] == "COMPLAINT"])

        if db_type == "POSTGRES":
            cur.execute("""
                INSERT INTO mart_live_summary (session_id, streamer, total_comments, total_gift_revenue_idr, purchase_intent_count, question_count, complaint_count, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (session_id) DO UPDATE SET
                    streamer = EXCLUDED.streamer,
                    total_comments = EXCLUDED.total_comments,
                    total_gift_revenue_idr = EXCLUDED.total_gift_revenue_idr,
                    purchase_intent_count = EXCLUDED.purchase_intent_count,
                    question_count = EXCLUDED.question_count,
                    complaint_count = EXCLUDED.complaint_count,
                    last_updated = NOW();
            """, (session_id, streamer, total_comments, total_rev, buy_cnt, quest_cnt, comp_cnt))
        else:
            cur.execute("""
                INSERT INTO mart_live_summary (session_id, streamer, total_comments, total_gift_revenue_idr, purchase_intent_count, question_count, complaint_count, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (session_id) DO UPDATE SET
                    streamer = excluded.streamer,
                    total_comments = excluded.total_comments,
                    total_gift_revenue_idr = excluded.total_gift_revenue_idr,
                    purchase_intent_count = excluded.purchase_intent_count,
                    question_count = excluded.question_count,
                    complaint_count = excluded.complaint_count,
                    last_updated = excluded.last_updated;
            """, (session_id, streamer, total_comments, total_rev, buy_cnt, quest_cnt, comp_cnt, now_str))

    # 4. Update mart_niche_benchmark (Batch Competitor & Market Intelligence lintas sesi per niche)
    df_comments_all = df_all[df_all["event_type"] == "comment"].copy()
    if not df_comments_all.empty:
        df_comments_all["niche"] = df_comments_all["streamer"].fillna("@unknown").apply(lambda s: detect_store_niche(str(s)))
        stopwords = {
            "kak", "kakak", "ka", "min", "mimin", "host", "tolong", "dong", "spill", "sepil",
            "etalase", "etls", "eta", "nomor", "no", "checkout", "co", "ready", "ga", "gak",
            "ngga", "nggak", "ada", "yg", "yang", "bisa", "nih", "ya", "ini", "itu", "mau",
            "saya", "aku", "sudah", "udah", "sdh", "udh", "di", "ke", "dari", "untuk", "buat",
            "banget", "apa", "nya", "dan", "atau", "kalau", "kalo", "aja", "sih", "deh", "dong"
        }

        for niche_key, grp in df_comments_all.groupby("niche"):
            tot_c = len(grp)
            tot_sess = int(grp["session_id"].nunique())
            buy_rate = round(float(len(grp[grp["ml_intent"] == "INTENT_BUY"]) / max(tot_c, 1) * 100.0), 1)
            quest_rate = round(float(len(grp[grp["ml_intent"] == "QUESTION"]) / max(tot_c, 1) * 100.0), 1)
            comp_rate = round(float(len(grp[grp["ml_intent"] == "COMPLAINT"]) / max(tot_c, 1) * 100.0), 1)

            # Hitung Golden Hour WIB (UTC+7)
            golden_hour = "19:00 - 21:00 WIB"
            try:
                times = pd.to_datetime(grp["event_time"], errors="coerce", utc=True).dropna()
                if not times.empty:
                    wib_hours = (times + timedelta(hours=7)).dt.hour
                    buy_mask = grp.loc[times.index, "ml_intent"] == "INTENT_BUY"
                    target_hours = wib_hours[buy_mask] if buy_mask.any() else wib_hours
                    peak_h = int(target_hours.mode().iloc[0])
                    golden_hour = f"{peak_h:02d}:00 - {(peak_h + 1) % 24:02d}:00 WIB"
            except Exception:
                pass

            # Ekstrak Top Keywords lintas room dalam niche
            kw_counter = Counter()
            for txt in grp["comment_text"].dropna().astype(str):
                tokens = re.findall(r"[a-zA-Z]{3,}", txt.lower())
                for tok in tokens:
                    if tok not in stopwords:
                        kw_counter[tok] += 1
            top_kws = [w for w, _ in kw_counter.most_common(6)]

            # Ekstrak contoh keluhan utama di niche
            comp_texts = grp[grp["ml_intent"] == "COMPLAINT"]["comment_text"].dropna().astype(str).unique().tolist()[:3]

            top_kws_json = json.dumps(top_kws, ensure_ascii=False)
            top_comp_json = json.dumps(comp_texts, ensure_ascii=False)

            if db_type == "POSTGRES":
                cur.execute("""
                    INSERT INTO mart_niche_benchmark (
                        niche, total_comments, total_sessions, intent_buy_rate_pct,
                        question_rate_pct, complaint_rate_pct, golden_hour_wib,
                        top_keywords, top_complaints, last_updated
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (niche) DO UPDATE SET
                        total_comments = EXCLUDED.total_comments,
                        total_sessions = EXCLUDED.total_sessions,
                        intent_buy_rate_pct = EXCLUDED.intent_buy_rate_pct,
                        question_rate_pct = EXCLUDED.question_rate_pct,
                        complaint_rate_pct = EXCLUDED.complaint_rate_pct,
                        golden_hour_wib = EXCLUDED.golden_hour_wib,
                        top_keywords = EXCLUDED.top_keywords,
                        top_complaints = EXCLUDED.top_complaints,
                        last_updated = NOW();
                """, (niche_key, tot_c, tot_sess, buy_rate, quest_rate, comp_rate, golden_hour, top_kws_json, top_comp_json))
            else:
                cur.execute("""
                    INSERT INTO mart_niche_benchmark (
                        niche, total_comments, total_sessions, intent_buy_rate_pct,
                        question_rate_pct, complaint_rate_pct, golden_hour_wib,
                        top_keywords, top_complaints, last_updated
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (niche) DO UPDATE SET
                        total_comments = excluded.total_comments,
                        total_sessions = excluded.total_sessions,
                        intent_buy_rate_pct = excluded.intent_buy_rate_pct,
                        question_rate_pct = excluded.question_rate_pct,
                        complaint_rate_pct = excluded.complaint_rate_pct,
                        golden_hour_wib = excluded.golden_hour_wib,
                        top_keywords = excluded.top_keywords,
                        top_complaints = excluded.top_complaints,
                        last_updated = excluded.last_updated;
                """, (niche_key, tot_c, tot_sess, buy_rate, quest_rate, comp_rate, golden_hour, top_kws_json, top_comp_json, now_str))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    aggregate_silver_to_gold()
