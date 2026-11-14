# TikPulse: Real-Time Live Commerce Intelligence Platform

> **Tugas Besar Pemrograman (TBP): Advanced Data Platform Engineering**  
> **Judul:** Perancangan Data Platform untuk Monitoring Interaksi Real-Time dan Prediksi Minat Beli pada TikTok Live Commerce  
> **Arsitektur:** Medallion Architecture (Bronze -> Silver -> Gold) dengan Redpanda, MinIO, PostgreSQL, MLflow, Prometheus, & Grafana

---

## 1. Ringkasan Eksekutif & Problem Statement

Pada siaran langsung penjualan (*TikTok Live Shopping*), ribuan interaksi penonton (komentar, saweran koin, dan pertanyaan) mengalir sangat deras setiap detiknya. Host live sering kali kewalahan dan kehilangan momentum penjualan karena tidak mengetahui etalase mana yang sedang paling diminati penonton.

**TikPulse** hadir sebagai platform data terintegrasi yang:
1. Menyerap aliran data siaran langsung secara *real-time* (**Data Streaming**) dan menggabungkannya dengan katalog etalase toko (**Data Batch**).
2. Memproses data menggunakan **Medallion Architecture**: membersihkan teks, menyaring spam, dan mendeteksi maksud pembelian (*Purchase Intent*) menggunakan model **Machine Learning**.
3. Menghasilkan **Live Host Cockpit Dashboard** dan **REST API** untuk memandu host mempromosikan produk yang tepat pada detik yang tepat.

---

## 2. Topologi Arsitektur Sistem

```
+-----------------------------------------------------------------------------------+
|                                TIKPULSE ARCHITECTURE                              |
+-----------------------------------------------------------------------------------+
|  [STREAM SOURCE]               [BATCH SOURCE]                                     |
|  TikTok Live Webcast           Katalog Produk Toko (CSV)                          |
|         │                              │                                          |
|         ▼                              ▼                                          |
|  [INGESTION: Python]           [BATCH LOADER: Checksum & Idempotent]              |
|         │                              │                                          |
|         ▼                              ▼                                          |
|  [BROKER: Redpanda]            [BRONZE STORAGE: MinIO / S3]                       |
|  Topic: tiktok.events.raw              │                                          |
|         │                              │                                          |
|         ▼                              ▼                                          |
|  [STREAM CONSUMER & ML ENGINE] ◄───────┘                                          |
|  - Deduplikasi (event_id)                                                         |
|  - Quality Gate (DLQ Routing)                                                     |
|  - Ekstraksi Etalase & ML Intent Classifier (Tracked by MLflow)                   |
|         │                                                                         |
|         ▼                                                                         |
|  [SILVER LAYER: Parquet in MinIO]                                                 |
|         │                                                                         |
|         ▼                                                                         |
|  [GOLD AGGREGATOR] ──► [POSTGRESQL SERVING MARTS]                                 |
|                        - mart_live_summary                                        |
|                        - mart_product_demand                                      |
|                        - mart_top_gifters                                         |
|                        - ml_predictions                                           |
|                                │                                                  |
|                 ┌──────────────┴──────────────┐                                   |
|                 ▼                             ▼                                   |
|         [SERVING: FastAPI]           [VISUALIZATION: Grafana]                     |
|         Endpoint /api/v1/...         Live Host Cockpit                            |
|                                               ▲                                   |
|                                               │                                   |
|  [OBSERVABILITY & ALERTS] ────────────────────┘                                   |
|  Prometheus + 3 Alert Rules (Lag, Job Failure, Health)                            |
+-----------------------------------------------------------------------------------+
```

---

## 3. Persyaratan Sistem & Instalasi Cepat (1-Click Run)

### Kebutuhan Perangkat Lunak:
* Docker & Docker Compose
* Python 3.10+ (untuk menjalankan script worker ingestion)

### Langkah 1: Jalankan Infrastruktur Kontainer
```bash
# Salin konfigurasi environment
cp .env.example .env

# Jalankan seluruh service (Redpanda, MinIO, PostgreSQL, Prometheus, Grafana)
docker compose up -d
```

Verifikasi kesehatan semua service:
```bash
docker compose ps
```

### Langkah 2: Instalasi Dependensi Python
```bash
pip install -r requirements.txt
```

---

## 4. Panduan Eksekusi Pipeline End-to-End

### Langkah 3: Ingest Data Batch Katalog Produk (F-02)
Muat data katalog toko ke Bronze Layer secara *idempotent*:
```bash
python src/ingestion/batch_loader.py
```

### Langkah 4: Jalankan Stream Processor & Quality Gate (F-03, F-05, F-06)
Di terminal pertama, jalankan consumer yang bertugas memproses data Bronze -> Silver -> Gold:
```bash
python src/pipeline/bronze_to_silver.py
```

### Langkah 5: Mulai Menangkap Siaran TikTok Live (F-04)
Di terminal kedua, masukkan username akun TikTok yang sedang siaran langsung (atau gunakan flag `--mock` untuk replayer interaktif):
```bash
# Jika ada akun yang sedang live:
python src/ingestion/tiktok_stream.py --username @namatokobaju

# Atau gunakan mode replayer interaktif (aman untuk presentasi):
python src/ingestion/tiktok_stream.py --mock
```

### Langkah 6: Jalankan Agregator Gold Marts (F-10)
Agregasikan data Silver menjadi Analytical Marts di PostgreSQL:
```bash
python src/pipeline/silver_to_gold.py
```

### Langkah 7: Jalankan Serving REST API (F-10, F-12, F-16)
```bash
uvicorn src.serving.app:app --host 0.0.0.0 --port 8000 --reload
```
Akses dokumentasi interaktif Swagger API di: **`http://localhost:8000/docs`**

---

## 5. Antarmuka Akses Pengguna & Port Inventory

| Layanan | URL Akses | Kredensial Default |
| :--- | :--- | :--- |
| **FastAPI Swagger Docs** | `http://localhost:8000/docs` | Header `X-User-Role: viewer` atau `admin` |
| **Grafana Dashboard** | `http://localhost:3000` | User: `admin` \| Pass: `admin_tikpulse` |
| **MinIO Console** | `http://localhost:9001` | User: `minio_admin` \| Pass: `minio_secure_password_2026` |
| **Prometheus Metrics** | `http://localhost:9090` | Tanpa password |
| **Redpanda Admin** | `http://localhost:9644/public_metrics` | Metrik internal broker |

---

## 6. Pengujian & Quality Assurance (F-18)

Jalankan rangkaian unit test dan ukur code coverage:
```bash
pytest --cov=src tests/ -v
```
*(Target evaluasi dosen: Coverage minimal 40%, mencakup transformasi data, deduplikasi, quality gate, dan kontrol keamanan API).*

---

## 7. Pemetaan Kepatuhan Spesifikasi Tugas (F-01 s.d. F-20)

| Kode Req | Kriteria Wajib | Bukti Implementasi di TikPulse |
| :--- | :--- | :--- |
| **F-01** | Deployment & Isolasi Service | `docker-compose.yml` dengan isolasi network, persistent volume, dan resource limit. |
| **F-02** | Pemrosesan Batch & Penjadwalan | `src/ingestion/batch_loader.py` dengan kalkulasi MD5 checksum dan anti-duplikasi. |
| **F-03** | Transformasi Incremental & Backfill | Transformasi Parquet partisi tanggal dengan kemampuan *replay* dari Bronze. |
| **F-04** | Stream Processing & Metadata | `src/ingestion/tiktok_stream.py` dengan metadata `event_id`, `event_time`, dan `payload_version`. |
| **F-05** | Event-Time, Dedup, & DLQ | `src/pipeline/bronze_to_silver.py` dengan deduplikasi `event_id` dan routing DLQ ke `tiktok.events.dlq`. |
| **F-06** | Medallion Architecture | Bronze (Raw MinIO), Silver (Validated Parquet), Gold (Marts PostgreSQL). |
| **F-07** | Format Analitik Parquet & ACID | Penyimpanan Parquet di MinIO + Transaksi ACID di PostgreSQL. |
| **F-08** | Data Quality Gate | Validasi *not-null* dan kelengkapan kolom dengan pemisahan ke karantina DLQ. |
| **F-10** | Serving SQL & REST API | FastAPI dengan 4 Read endpoints, 1 ML endpoint, dan dokumentasi OpenAPI. |
| **F-11** | Dataset ML, Eksperimen, & Tracking | `src/ml/train_intent_model.py` membandingkan Logistic Regression vs Random Forest via MLflow. |
| **F-12** | Integrasi Inference ML | Online embedded inference dengan pencatatan hasil prediksi ke tabel `ml_predictions`. |
| **F-13** | Dashboard Analitik Grafana | Live Host Cockpit dengan 4 metrik bisnis utama dan visualisasi interaktif. |
| **F-14** | Structured Logs & Telemetri | Prometheus metrics scraper untuk Redpanda, Postgres, dan FastAPI. |
| **F-15** | 3 Alert yang Diuji End-to-End | `config/alert_rules.yml`: Alert Job Fail, Alert Stream High Lag, Alert Service Down. |
| **F-16** | Kontrol Keamanan & Hak Akses | Role separation (`admin` vs `viewer`) dengan pengujian HTTP 401 dan 403. |
| **F-17** | Recovery, Backup, & Restore | Prosedur restore volume MinIO dan offset Redpanda saat failover. |
| **F-18** | Tests & Reproducibility | Rangkaian automated unit test di folder `tests/` dengan Pytest. |
