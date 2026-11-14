-- ==============================================================================
-- TikPulse - Serving Layer Analytical SQL Queries (Rubric F-10)
-- Database Target: PostgreSQL / SQLite Marts
-- ==============================================================================

-- 1. Query Ringkasan Metrik Live Stream per Sesi Aktif (F-10.1)
-- Mengambil agregasi real-time: total chat, estimasi saweran rupiah, dan deteksi minat beli
SELECT 
    session_id,
    streamer,
    total_comments,
    total_gift_revenue_idr,
    purchase_intent_count,
    question_count,
    complaint_count,
    ROUND((purchase_intent_count::numeric / NULLIF(total_comments, 0)) * 100, 2) AS buy_conversion_rate_pct,
    last_updated
FROM mart_live_summary
WHERE session_id = 'sess_active'
ORDER BY last_updated DESC
LIMIT 1;

-- 2. Query Leaderboard Permintaan Etalase Produk (F-10.2)
-- Menampilkan 5 etalase yang paling sering di-request penonton dan potensi omzet
SELECT 
    etalase_no,
    product_name,
    price_idr,
    request_count,
    high_intent_count,
    (price_idr * high_intent_count) AS potential_pipeline_gmv_idr,
    last_requested_at
FROM mart_product_demand
WHERE session_id = 'sess_active'
ORDER BY request_count DESC, high_intent_count DESC
LIMIT 5;

-- 3. Query Top Sultan Gifters / Donatur Koin Live (F-10.3)
-- Menemukan penonton paling loyal berdasarkan kontribusi saweran rupiah
SELECT 
    user_id,
    nickname,
    total_gift_idr,
    gift_count,
    ROUND(total_gift_idr::numeric / NULLIF(gift_count, 0), 0) AS avg_gift_per_event_idr,
    last_gift_at
FROM mart_top_gifters
WHERE session_id = 'sess_active'
ORDER BY total_gift_idr DESC
LIMIT 10;

-- 4. Query Deteksi Lonjakan Permintaan Produk (Spike Detection) (F-10.4)
-- Mengidentifikasi produk dengan rasio minat beli di atas 50%
SELECT 
    etalase_no,
    product_name,
    request_count,
    high_intent_count,
    ROUND((high_intent_count::numeric / NULLIF(request_count, 0)) * 100, 1) AS high_intent_ratio_pct
FROM mart_product_demand
WHERE session_id = 'sess_active' AND request_count >= 2
ORDER BY high_intent_ratio_pct DESC, request_count DESC;

-- 5. Query Audit Kualitas Aliran Data (Freshness & Integrity) (F-10.5)
-- Memeriksa interval keterkinian data pada serving layer
SELECT 
    session_id,
    streamer,
    COUNT(*) AS total_registered_etalase,
    SUM(request_count) AS total_requests,
    MAX(last_requested_at) AS latest_request_time
FROM mart_product_demand
GROUP BY session_id, streamer
ORDER BY latest_request_time DESC;
