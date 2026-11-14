# Architecture Decision Records (ADR)
**Project:** TikPulse — Live Commerce Intelligence Platform  
**Standard:** TBP Advanced Data Platform Engineering (Section 3.3)

---

### ADR-01: Message Broker, Partisi, dan Delivery Semantics
* **Status:** APPROVED
* **Pilihan:** **Redpanda Broker (Kafka-compatible)**
* **Alasan Pemilihan:**
  1. Redpanda ditulis menggunakan C++ (*thread-per-core* architecture), menghilangkan *overhead* JVM garbage collection dan Zookeeper.
  2. Menghemat penggunaan RAM (hanya $\approx 500\text{ MB}$ vs Apache Kafka yang butuh $> 1.5\text{ GB}$), sangat cocok untuk profil Compact Lab ($\le 16\text{ GB}$).
  3. Mendukung 100% Kafka API sehingga *client libraries* standar tetap dapat digunakan.
* **Mekanisme Ack & Delivery Semantics:**
  * Producer diset dengan `acks=all` (atau `acks=1` untuk throughput chat tinggi) guna menjamin tidak ada event komentar yang hilang.
  * Delivery Semantics: **At-Least-Once Delivery** pada level broker, dikombinasikan dengan **Deduplikasi Idempotent** di sisi consumer menggunakan `event_id` unik dari TikTok (F-05).
* **Partisi:** Topik `tiktok.events.raw` menggunakan 3 partisi dengan partition key berdasarkan `session_id` atau `user_id`.

---

### ADR-02: Storage, File Format, Partition Key, dan Retensi
* **Status:** APPROVED
* **Pilihan:** **MinIO (S3-Compatible) + Apache Parquet**
* **Alasan Pemilihan:**
  1. **MinIO:** Menyediakan API standar AWS S3 secara lokal/on-premise tanpa ketergantungan pada *cloud-managed vendor*.
  2. **Apache Parquet:** Format file kolumnar dengan kompresi Snappy/GZIP. Menghemat ukuran penyimpanan hingga $60\text{--}75\%$ dibandingkan JSON mentah serta mempercepat pembacaan kolom tertentu.
* **Partition Key:** `year=YYYY/month=MM/day=DD/hour=HH/` pada layer Bronze dan Silver.
* **Kebijakan Retensi:**
  * Bronze (Raw JSON): Retensi 30 hari (untuk kebutuhan replay/backfill).
  * Silver & Gold (Parquet): Retensi 90 hari.

---

### ADR-03: Batch & Stream Processor serta Batas Sumber Daya
* **Status:** APPROVED
* **Pilihan:** **Python Async Stream Consumer + Prefect/Cron Runner**
* **Alasan Pemilihan:**
  1. Beban lalu lintas satu siaran TikTok Live rata-rata berkisar antara $10\text{--}100\text{ event/detik}$. Python Async I/O sangat mumpuni menangani volume ini dengan latensi $< 50\text{ ms}$.
  2. Menghindari keharusan menjalankan cluster Apache Spark / Apache Flink yang memerlukan alokasi RAM besar ($> 4\text{ GB}$).
* **Batas Sumber Daya (Resource Budget):**
  * Maksimum alokasi RAM stream worker: $400\text{ MB}$.
  * CPU limit: $0.8\text{ vCPU}$.

---

### ADR-04: Metode Inference ML, Karakteristik Data, dan Model Baseline
* **Status:** APPROVED
* **Pilihan:** **Online Embedded Inference (Scikit-Learn TF-IDF + Logistic Regression / LightGBM) + MLflow Registry**
* **Alasan Pemilihan:**
  1. **Online Inference:** Komentar penonton memerlukan analisis maksud pembelian (*purchase intent*) secara *real-time* saat live masih berlangsung (bukan batch di akhir hari).
  2. **Baseline Model:** TF-IDF n-gram digabungkan dengan Logistic Regression / Naive Bayes. Model ini memiliki waktu inferensi $< 5\text{ milidetik}$ per kalimat, akurasi tinggi untuk teks pendek bahasa Indonesia, dan sangat ringan memori.
  3. **MLflow:** Digunakan untuk tracking metrik (Precision, Recall, F1-Score), parameter, dan versioning artefak model (F-11).
* **Keamanan Endpoint:** Endpoint inferensi dilindungi oleh API Key / JWT token dengan role-based access (`admin` vs `viewer`).

---

### ADR-05: Rencana Recovery, Backup, dan Toleransi Kegagalan (Fault Tolerance)
* **Status:** APPROVED
* **State yang Wajib Dipertahankan:**
  1. MinIO Object Storage volume (`minio_data`).
  2. PostgreSQL database volume (`postgres_data`).
  3. Redpanda consumer offsets (`redpanda_data`).
* **Kegagalan yang Dapat Ditoleransi:**
  * Jika kontainer *Stream Consumer* mati mendadak, pesan komentar tetap tertampung di Redpanda broker. Saat consumer di-restart, ia akan melanjutkan pemrosesan dari offset terakhir yang ter-commit (*zero data loss*).
  * Komentar rusak/malformed otomatis dialihkan ke *Dead Letter Queue (DLQ)* tanpa menghentikan pipeline.
* **Prosedur Backup & Restore:**
  * PostgreSQL: `pg_dump` otomatis harian.
  * MinIO: `mc mirror` snapshot bucket secara berkala.
