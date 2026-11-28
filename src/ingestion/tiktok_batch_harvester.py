"""
TikPulse - Multi-Room TikTok Live Batch Harvester (F-02, F-04, F-06, F-11)
Fungsi:
  1. Memanen interaksi asli (CommentEvent, GiftEvent, OecLiveShoppingEvent) secara paralel
     dari banyak akun TikTok Commerce Indonesia yang sedang live.
  2. Menyimpan data mentah ke Bronze Layer (JSONL + MD5 Checksum + Manifest).
  3. Mengonversi ke Silver Parquet melalui BronzeToSilverProcessor (Deduplikasi + Quality Gate).
  4. Memberi label otomatis (Weak Supervision) untuk memperkaya dataset latih ML di
     data/batch/harvested_labeled_comments.csv dan memperbarui Gold Niche Benchmark.
"""

import os
import re
import sys
import json
import uuid
import time
import glob
import hashlib
import asyncio
import argparse
from datetime import datetime, timezone
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.store_catalog import detect_store_niche
from pipeline.bronze_to_silver import BronzeToSilverProcessor, SILVER_OUTPUT_DIR

try:
    from TikTokLive import TikTokLiveClient
    from TikTokLive.events import CommentEvent, GiftEvent, OecLiveShoppingEvent
    TIKTOK_LIVE_AVAILABLE = True
except ImportError:
    TIKTOK_LIVE_AVAILABLE = False

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BRONZE_HARVEST_DIR = os.path.join(BASE_DIR, "data", "bronze", "harvest")
BATCH_LABELED_CSV = os.path.join(BASE_DIR, "data", "batch", "harvested_labeled_comments.csv")

os.makedirs(BRONZE_HARVEST_DIR, exist_ok=True)
os.makedirs(os.path.dirname(BATCH_LABELED_CSV), exist_ok=True)

# Daftar akun TikTok Commerce populer per niche untuk panen data batch lintas toko
DEFAULT_HARVEST_TARGETS = {
    "skincare": [
        "@somethincofficial",
        "@skintificofficial",
        "@wardahofficial",
        "@originoteofficial",
        "@glad2glowofficial",
        "@scarlett_whitening",
    ],
    "shoes": [
        "@aerostreet",
        "@compass",
        "@ventelashoes",
    ],
    "fashion": [
        "@erigostore",
        "@jiniso.id",
        "@mayoutfit",
    ],
    "sports": [
        "@raysportindo",
        "@ortuseight",
    ],
    "gadgets": [
        "@samsungindonesia",
        "@xiaomi.indonesia",
    ],
}


def calculate_file_md5(file_path: str) -> str:
    """Menghitung MD5 checksum untuk verifikasi integritas batch."""
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def auto_label_intent(comment_text: str) -> tuple[str, float]:
    """
    Weak Supervision Labeler untuk komentar TikTok Live berbahasa Indonesia.
    Mengembalikan (label, confidence_score).
    """
    if not comment_text or not str(comment_text).strip():
        return "CASUAL", 0.0

    t = str(comment_text).lower().strip()

    complaint_patterns = [
        r"\b(kecewa|rusak|robek|penipu|palsu|bohong|cacat|pecah|bocor|retur|lelet)\b",
        r"(belum dikirim|ga dikirim|gak dikirim|lama banget|ga dibalas|gak dibalas|jelek banget)",
    ]
    for pat in complaint_patterns:
        if re.search(pat, t):
            return "COMPLAINT", 0.90

    buy_patterns = [
        r"\b(co|checkout|order|keep|ambil|beli|bungkus|fix)\b",
        r"\b(mau|udah|udh|sdh)\s+(co|beli|order|checkout)\b",
        r"\b(spill|sepil)\b.*\b(no|nomor|eta|etls|etalase|[0-9]{1,2})\b",
        r"\b(bisa cod|cod gak|cod ga|diskon|promo|gratis ongkir|ready stock|masih ada stok)\b",
    ]
    for pat in buy_patterns:
        if re.search(pat, t):
            return "INTENT_BUY", 0.88

    question_patterns = [
        r"\?",
        r"\b(ukuran|size|bb|tb|muat|bahan|warna|shade|varian|cocok|aman|bumil|busui|ori)\b",
        r"\b(kirim dari|berapa hari|tahan berapa|spesifikasi|ongkir|tebel|tipis|nerawang)\b",
        r"\b(spill|sepil|no|nomor|etalase|eta)\b",
    ]
    for pat in question_patterns:
        if re.search(pat, t):
            return "QUESTION", 0.82

    return "CASUAL", 0.75


def append_labeled_comments_batch(records: list[dict], source_tag: str = "tiktok_harvest") -> int:
    """
    Menambahkan komentar terlabel secara deduplikatif ke data/batch/harvested_labeled_comments.csv
    sebagai korpus training Batch ML.
    """
    if not records:
        return 0

    existing_texts = set()
    if os.path.exists(BATCH_LABELED_CSV):
        try:
            df_existing = pd.read_csv(BATCH_LABELED_CSV)
            if "text" in df_existing.columns:
                existing_texts = set(df_existing["text"].dropna().astype(str).str.lower().str.strip())
        except Exception:
            df_existing = pd.DataFrame()
    else:
        df_existing = pd.DataFrame()

    new_rows = []
    for rec in records:
        text = str(rec.get("comment_text", "")).strip()
        if len(text) < 3:
            continue
        norm = text.lower()
        if norm in existing_texts:
            continue
        existing_texts.add(norm)
        label, conf = auto_label_intent(text)
        new_rows.append({
            "text": text,
            "label": label,
            "confidence": conf,
            "streamer": rec.get("streamer", "unknown"),
            "niche": rec.get("niche", "fashion"),
            "source": source_tag,
            "harvested_at": rec.get("event_time", datetime.now(timezone.utc).isoformat()),
        })

    if not new_rows:
        return 0

    df_new = pd.DataFrame(new_rows)
    if not df_existing.empty:
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    df_combined.to_csv(BATCH_LABELED_CSV, index=False, encoding="utf-8")
    return len(new_rows)


def bootstrap_labeled_csv_from_silver() -> int:
    """
    Mengindeks komentar nyata yang sudah tersimpan di arsip Silver Parquet
    ke data/batch/harvested_labeled_comments.csv (mencegah duplikasi).
    """
    parquet_files = glob.glob(os.path.join(SILVER_OUTPUT_DIR, "*.parquet"))
    if not parquet_files:
        return 0

    collected = []
    for path in parquet_files:
        try:
            df = pd.read_parquet(path)
            if df.empty or "comment_text" not in df.columns:
                continue
            df_c = df[df["event_type"] == "comment"].dropna(subset=["comment_text"])
            for _, row in df_c.iterrows():
                streamer = str(row.get("streamer", "@unknown"))
                collected.append({
                    "comment_text": str(row["comment_text"]),
                    "streamer": streamer,
                    "niche": detect_store_niche(streamer),
                    "event_time": str(row.get("event_time", datetime.now(timezone.utc).isoformat())),
                })
        except Exception:
            continue

    return append_labeled_comments_batch(collected, source_tag="silver_archive_bootstrap")


async def harvest_single_room(
    username: str,
    niche: str,
    duration_sec: int,
    shared_events: list,
    semaphore: asyncio.Semaphore = None,
) -> dict:
    """Menyambung ke 1 room live TikTok dan memanen event selama duration_sec detik."""
    clean_user = username.lstrip("@").strip()
    display_user = f"@{clean_user}"
    session_id = f"harvest_{int(time.time())}_{clean_user}"
    stats = {"streamer": display_user, "niche": niche, "is_live": False, "events_captured": 0}

    if not TIKTOK_LIVE_AVAILABLE or not clean_user:
        return stats

    async def _run():
        client = TikTokLiveClient(unique_id=clean_user)

        @client.on(CommentEvent)
        async def on_comment(event: CommentEvent):
            comm_text = getattr(event, "comment", None) or getattr(event, "content", "")
            if not comm_text:
                return
            user_nick = "Viewer"
            user_id = f"u_{uuid.uuid4().hex[:6]}"
            if getattr(event, "user", None):
                user_nick = getattr(event.user, "nickname", None) or getattr(event.user, "unique_id", "Viewer")
                user_id = str(getattr(event.user, "id", None) or getattr(event.user, "user_id", user_id))

            shared_events.append({
                "event_id": f"hrv_{uuid.uuid4().hex[:12]}",
                "session_id": session_id,
                "streamer": display_user,
                "niche": niche,
                "event_type": "comment",
                "payload_version": "1.0.0",
                "event_time": datetime.now(timezone.utc).isoformat(),
                "ingest_time": datetime.now(timezone.utc).isoformat(),
                "data": {
                    "user_id": user_id,
                    "nickname": user_nick,
                    "comment_text": str(comm_text),
                },
            })
            stats["events_captured"] += 1

        @client.on(GiftEvent)
        async def on_gift(event: GiftEvent):
            if getattr(event, "streaking", False):
                return
            gift_name = getattr(event.gift, "name", "Gift") if getattr(event, "gift", None) else "Gift"
            diamonds = getattr(event.gift, "diamond_count", 1) if getattr(event, "gift", None) else 1
            user_nick = "Sultan"
            user_id = f"u_{uuid.uuid4().hex[:6]}"
            if getattr(event, "user", None):
                user_nick = getattr(event.user, "nickname", None) or "Sultan"
                user_id = str(getattr(event.user, "id", None) or getattr(event.user, "user_id", user_id))

            shared_events.append({
                "event_id": f"hrv_{uuid.uuid4().hex[:12]}",
                "session_id": session_id,
                "streamer": display_user,
                "niche": niche,
                "event_type": "gift",
                "payload_version": "1.0.0",
                "event_time": datetime.now(timezone.utc).isoformat(),
                "ingest_time": datetime.now(timezone.utc).isoformat(),
                "data": {
                    "user_id": user_id,
                    "nickname": user_nick,
                    "gift_name": gift_name,
                    "diamond_count": diamonds,
                    "repeat_count": getattr(event, "repeat_count", 1),
                },
            })
            stats["events_captured"] += 1

        try:
            is_live = await asyncio.wait_for(client.is_live(), timeout=8.0)
            if not is_live:
                print(f"[HARVESTER OFFLINE] {display_user} tidak sedang live.")
                return stats

            stats["is_live"] = True
            print(f"[HARVESTER LIVE] {display_user} ({niche}) - mulai panen {duration_sec} detik...")
            await client.start()
            await asyncio.sleep(duration_sec)
            await client.disconnect()
        except Exception as e:
            print(f"[HARVESTER SKIP] {display_user}: {type(e).__name__} {e}")
            try:
                await client.disconnect()
            except Exception:
                pass
        return stats

    if semaphore:
        async with semaphore:
            return await _run()
    return await _run()


async def run_batch_harvest(
    usernames: list[str] = None,
    niche_filter: str = None,
    duration_sec: int = 20,
) -> dict:
    """
    Menjalankan panen Batch paralel ke banyak room TikTok Live,
    menyimpan Bronze JSONL + Silver Parquet + memperbarui korpus training ML.
    """
    targets = []
    if usernames:
        for u in usernames:
            clean = u.strip()
            if clean:
                niche = niche_filter or detect_store_niche(clean)
                targets.append((clean, niche))
    else:
        for niche_key, accounts in DEFAULT_HARVEST_TARGETS.items():
            if niche_filter and niche_filter != niche_key:
                continue
            for acc in accounts:
                targets.append((acc, niche_key))

    shared_events = []
    sem = asyncio.Semaphore(4)
    tasks = [harvest_single_room(u, n, duration_sec, shared_events, sem) for u, n in targets]
    room_results = await asyncio.gather(*tasks, return_exceptions=False)

    live_rooms = [r["streamer"] for r in room_results if r.get("is_live")]
    batch_id = uuid.uuid4().hex[:10]
    silver_rows_saved = 0
    labeled_added = 0
    bronze_file = None

    if shared_events:
        # 1. Simpan Raw Bronze JSONL + Manifest Checksum
        bronze_file = os.path.join(BRONZE_HARVEST_DIR, f"harvest_{batch_id}.jsonl")
        with open(bronze_file, "w", encoding="utf-8") as f:
            for ev in shared_events:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")

        checksum = calculate_file_md5(bronze_file)
        manifest = {
            "batch_id": batch_id,
            "harvest_time": datetime.now(timezone.utc).isoformat(),
            "live_rooms": live_rooms,
            "total_raw_events": len(shared_events),
            "checksum": checksum,
            "bronze_file": os.path.basename(bronze_file),
        }
        with open(os.path.join(BRONZE_HARVEST_DIR, "metadata_latest.json"), "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)

        # 2. Proses ke Silver Layer Parquet (Deduplikasi + Quality Gate + ML Scoring)
        processor = BronzeToSilverProcessor()
        silver_records = []
        comment_rows_for_ml = []
        for ev in shared_events:
            rec = processor.process_single_event(ev)
            if rec:
                silver_records.append(rec)
                if rec.get("event_type") == "comment":
                    comment_rows_for_ml.append({
                        "comment_text": rec.get("comment_text", ""),
                        "streamer": rec.get("streamer", "@unknown"),
                        "niche": ev.get("niche", "fashion"),
                        "event_time": rec.get("event_time"),
                    })

        if silver_records:
            df_silver = pd.DataFrame(silver_records)
            silver_path = os.path.join(SILVER_OUTPUT_DIR, f"harvest_{batch_id}.parquet")
            df_silver.to_parquet(silver_path, engine="pyarrow", index=False)
            silver_rows_saved = len(silver_records)

        # 3. Tambah ke Batch Labeled CSV untuk training ML
        labeled_added = append_labeled_comments_batch(comment_rows_for_ml, source_tag="tiktok_live_harvest")

    # Selalu sinkronkan arsip Silver ke CSV terlabel & agregasi Gold Mart
    bootstrapped = bootstrap_labeled_csv_from_silver()
    try:
        from pipeline.silver_to_gold import aggregate_silver_to_gold
        aggregate_silver_to_gold()
    except Exception as e:
        print(f"[HARVEST GOLD SYNC WARN] {e}")

    return {
        "status": "SUCCESS",
        "batch_id": batch_id,
        "targets_checked": len(targets),
        "live_rooms_found": live_rooms,
        "total_raw_events": len(shared_events),
        "silver_rows_saved": silver_rows_saved,
        "new_labeled_ml_rows": labeled_added + bootstrapped,
        "bronze_file": bronze_file,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TikPulse Multi-Room TikTok Live Batch Harvester")
    parser.add_argument("--usernames", nargs="*", help="Daftar username TikTok opsional (misal @somethincofficial @aerostreet)")
    parser.add_argument("--niche", type=str, default=None, help="Filter niche opsional (skincare, fashion, shoes, sports, gadgets)")
    parser.add_argument("--duration", type=int, default=20, help="Durasi panen per room dalam detik (default 20)")
    args = parser.parse_args()

    result = asyncio.run(run_batch_harvest(usernames=args.usernames, niche_filter=args.niche, duration_sec=args.duration))
    print("\n[HARVEST SUMMARY]")
    print(json.dumps(result, indent=2, ensure_ascii=False))
