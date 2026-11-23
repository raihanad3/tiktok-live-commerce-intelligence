"""
Unit Tests for Serving API & Security Role Separation (F-16 & F-18 Requirements)
"""

import pytest
import os
import sys
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from serving.app import app

client = TestClient(app)

def test_api_health():
    """Menguji endpoint health check."""
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "HEALTHY"

def test_security_admin_access_forbidden_for_viewer():
    """Menguji kontrol keamanan F-16: Viewer dilarang mengakses endpoint Admin (HTTP 403)."""
    response = client.post(
        "/api/v1/admin/reset-session",
        headers={"role": "viewer"}
    )
    assert response.status_code == 403
    assert "Akses ditolak" in response.json()["detail"]

def test_ml_inference_input_validation():
    """Menguji validasi payload inferensi: komentar kosong menghasilkan HTTP 422/400 (F-12)."""
    response = client.post(
        "/api/v1/ml/predict",
        json={"comment_text": " "},
        headers={"role": "viewer"}
    )
    # Jika gagal validasi pydantic atau business logic
    assert response.status_code in [400, 422]

def test_get_catalog_endpoint():
    """Menguji endpoint pengambilan katalog toko."""
    response = client.get("/api/v1/catalog?streamer=@aerostreet", headers={"role": "viewer"})
    assert response.status_code == 200
    data = response.json()
    assert "products" in data
    assert len(data["products"]) > 0
    assert any("Aerostreet" in p["product_name"] for p in data["products"])

def test_update_catalog_endpoint():
    """Menguji endpoint pembaruan katalog toko custom."""
    test_products = [
        {"etalase_no": 1, "product_name": "Sepatu Test Edition 1", "price_idr": 150000},
        {"etalase_no": 2, "product_name": "Kaos Test Edition 2", "price_idr": 75000}
    ]
    response = client.post(
        "/api/v1/catalog/update",
        json={"streamer": "@teststore", "products": test_products},
        headers={"role": "viewer"}
    )
    assert response.status_code == 200
    assert response.json()["status"] == "success"

    # Verifikasi langsung lewat GET
    get_res = client.get("/api/v1/catalog?streamer=@teststore", headers={"role": "viewer"})
    assert get_res.status_code == 200
    prods = get_res.json()["products"]
    assert len(prods) == 2
    assert prods[0]["product_name"] == "Sepatu Test Edition 1"

def test_get_available_niches_endpoint():
    """Menguji endpoint pengambilan daftar niche yang tersedia (F-10 / Hybrid Niche)."""
    response = client.get("/api/v1/niches/available", headers={"role": "viewer"})
    assert response.status_code == 200
    data = response.json()
    assert "niches" in data
    assert data["total_niches"] >= 7
    keys = [n["key"] for n in data["niches"]]
    assert "fashion" in keys
    assert "sports" in keys
    assert "skincare" in keys

def test_switch_session_with_niche_override():
    """Menguji beralih toko dengan manual niche override."""
    payload = {
        "username": "@ambiguous_shop_123",
        "niche_override": "kuliner"
    }
    response = client.post("/api/v1/session/switch", json=payload, headers={"role": "viewer"})
    assert response.status_code == 200
    data = response.json()
    assert data["niche"] == "culinary"
    assert data["niche_override_used"] is True

def test_hybrid_niche_detection_hash_consistency():
    """Menguji konsistensi hash-based smart random assignment untuk username ambigu."""
    from common.store_catalog import detect_store_niche
    niche_1 = detect_store_niche("@tokomisterix")
    niche_2 = detect_store_niche("@tokomisterix")
    assert niche_1 == niche_2
    assert niche_1 in ["fashion", "sports", "shoes", "skincare", "hijab", "gadgets", "culinary"]

def test_extract_oec_shopping_products_v1_and_v2():
    """Menguji ekstraksi produk nyata OecLiveShoppingEvent untuk skema v1 (pop_product) dan v2 (CommerceInfo)."""
    from types import SimpleNamespace
    from serving.app import extract_oec_shopping_products

    # Test v1 payload
    v1_event = SimpleNamespace(
        live_product_number=3,
        pop_product=SimpleNamespace(
            title="Serum Vitamin C Brightening 20ml",
            price="Rp 129.000",
            product_id=1729384501,
            image_url="https://example.com/img.jpg"
        ),
        product_snap_shot=None,
        oec_live_shopping_message_v2=None
    )
    prods_v1, pinned_v1 = extract_oec_shopping_products(v1_event)
    assert pinned_v1 == 3
    assert len(prods_v1) == 1
    assert prods_v1[0]["etalase_no"] == 3
    assert prods_v1[0]["product_name"] == "Serum Vitamin C Brightening 20ml"
    assert prods_v1[0]["price_idr"] == 129000
    assert prods_v1[0]["source"] == "TIKTOK_LIVE_PIN"

    # Test v2 payload
    v2_event = SimpleNamespace(
        live_product_number=0,
        pop_product=None,
        product_snap_shot=None,
        oec_live_shopping_message_v2=SimpleNamespace(
            commerce_info=SimpleNamespace(
                pop_card_info=SimpleNamespace(pop_product_id=99002),
                product_info={
                    1: SimpleNamespace(
                        product_id=99001,
                        bag_index=SimpleNamespace(value=1),
                        title=SimpleNamespace(text="Toner Eksfoliasi Mild"),
                        price_info=SimpleNamespace(sale_price_decimal="89000.00")
                    ),
                    2: SimpleNamespace(
                        product_id=99002,
                        bag_index=SimpleNamespace(value=4),
                        title=SimpleNamespace(text="Moisturizer Barrier Gel"),
                        price_info=SimpleNamespace(sale_price_format="Rp 145.000")
                    )
                }
            )
        )
    )
    prods_v2, pinned_v2 = extract_oec_shopping_products(v2_event)
    assert pinned_v2 == 4
    assert [p["etalase_no"] for p in prods_v2] == [1, 4]
    assert prods_v2[0]["price_idr"] == 89000
    assert prods_v2[1]["product_name"] == "Moisturizer Barrier Gel"
    assert prods_v2[1]["price_idr"] == 145000
    assert prods_v2[1]["is_pinned"] is True

def test_real_live_product_demand_no_fake_fallback():
    """Menguji mode REALTIME_TIKTOK_LIVE tidak pernah menampilkan katalog palsu saat belum ada pin/custom."""
    import serving.app as app_mod

    orig_mode = app_mod.stream_mode
    orig_streamer = app_mod.active_streamer
    orig_real = app_mod.is_real_live_connected
    try:
        app_mod.reset_active_session_state()
        app_mod.active_streamer = "@akun_asli_tanpa_katalog_999"
        app_mod.stream_mode = "REALTIME_TIKTOK_LIVE"
        app_mod.is_real_live_connected = True

        resp_empty = client.get("/api/v1/products/demand?limit=5", headers={"role": "viewer"})
        assert resp_empty.status_code == 200
        data_empty = resp_empty.json()
        assert data_empty["catalog_mode"] == "REAL_LIVE_ONLY"
        assert data_empty["leaderboard"] == []

        # Inject produk pin asli dari TikTok Live
        app_mod.current_pinned_etalase = 7
        app_mod.live_captured_products[7] = {
            "etalase_no": 7,
            "product_name": "Produk Pin Live Asli Host",
            "price_idr": 155000,
            "product_id": "12345",
            "source": "TIKTOK_LIVE_PIN",
            "is_pinned": True
        }

        resp_pin = client.get("/api/v1/products/demand?limit=5", headers={"role": "viewer"})
        assert resp_pin.status_code == 200
        lb = resp_pin.json()["leaderboard"]
        assert len(lb) == 1
        assert lb[0]["etalase_no"] == 7
        assert lb[0]["product_name"] == "Produk Pin Live Asli Host"
        assert lb[0]["source"] == "TIKTOK_LIVE_PIN"
        assert lb[0]["is_pinned"] is True
    finally:
        app_mod.reset_active_session_state()
        app_mod.stream_mode = orig_mode
        app_mod.active_streamer = orig_streamer
        app_mod.is_real_live_connected = orig_real

def test_high_volume_ingestion_latency():
    """Menguji pemrosesan 200 event beruntun berjalan cepat (<15 ms/event) dengan akumulasi akurat di memory."""
    import time
    import serving.app as app_mod

    orig_mode = app_mod.stream_mode
    orig_streamer = app_mod.active_streamer
    orig_sess = app_mod.active_session_id
    try:
        app_mod.reset_active_session_state()
        app_mod.active_streamer = "@erigostore"
        app_mod.active_session_id = "sess_latency_test"
        app_mod.stream_mode = "SIMULATION_FALLBACK"

        t_start = time.perf_counter()
        for i in range(200):
            if i % 10 == 0:
                evt = {
                    "event_id": f"lat_gift_{i}",
                    "session_id": "sess_latency_test",
                    "streamer": "@erigostore",
                    "event_type": "gift",
                    "data": {"user_id": "u_sultan", "nickname": "Sultan_Uji", "gift_name": "Rose", "diamond_count": 10, "repeat_count": 1}
                }
            else:
                evt = {
                    "event_id": f"lat_comm_{i}",
                    "session_id": "sess_latency_test",
                    "streamer": "@erigostore",
                    "event_type": "comment",
                    "data": {"user_id": f"u_{i}", "nickname": f"User_{i}", "comment_text": "Kak spill etalase 1 mau checkout sekarang"}
                }
            app_mod.ingest_and_process_event(evt)

        elapsed_ms = (time.perf_counter() - t_start) * 1000
        avg_ms = elapsed_ms / 200
        assert avg_ms < 15.0, f"Latensi terlalu tinggi: {avg_ms:.2f} ms/event"
        assert app_mod.session_summary["total_comments"] == 180
        assert app_mod.session_summary["total_gift_revenue_idr"] == 20 * 10 * 250
        assert app_mod.session_product_demand[1]["request_count"] == 180
        assert app_mod.session_top_gifters["u_sultan"]["gift_count"] == 20
    finally:
        app_mod.reset_active_session_state()
        app_mod.stream_mode = orig_mode
        app_mod.active_streamer = orig_streamer
        app_mod.active_session_id = orig_sess

def test_gift_delta_tracking_rose_combo_and_single_tap():
    """Menguji pelacakan delta gift agar Mawar 1-tap dan combo streak langsung masuk tanpa dobel hitung."""
    from collections import OrderedDict
    from serving.app import compute_gift_delta

    tracker = OrderedDict()

    # 1. Single tap Mawar (repeat_end=0, paket penutup tidak pernah dikirim TikTok) -> langsung +1
    d_single = compute_gift_delta(tracker, "u_1", "5655", "grp_single", repeat_count=1, is_streakable=True, repeat_end=False)
    assert d_single == 1

    # 2. Combo Mawar 3x (1 -> 2 -> 3 dengan repeat_end=0, lalu penutup repeat_end=1 + duplikat)
    deltas_combo = [
        compute_gift_delta(tracker, "u_budi", "5655", "grp_budi_1", 1, True, False),
        compute_gift_delta(tracker, "u_budi", "5655", "grp_budi_1", 2, True, False),
        compute_gift_delta(tracker, "u_budi", "5655", "grp_budi_1", 3, True, False),
        compute_gift_delta(tracker, "u_budi", "5655", "grp_budi_1", 3, True, True),
        compute_gift_delta(tracker, "u_budi", "5655", "grp_budi_1", 3, True, True),
    ]
    assert deltas_combo == [1, 1, 1, 0, 0]
    assert sum(deltas_combo) == 3

    # 3. Combo melompat (1 -> 5)
    assert compute_gift_delta(tracker, "u_ani", "5655", "grp_ani_1", 1, True, False) == 1
    assert compute_gift_delta(tracker, "u_ani", "5655", "grp_ani_1", 5, True, True) == 4

    # 4. Non-streakable gift deduplikasi per group_id
    assert compute_gift_delta(tracker, "u_sultan", "9999", "grp_paus_1", 1, False, True) == 1
    assert compute_gift_delta(tracker, "u_sultan", "9999", "grp_paus_1", 1, False, True) == 0

def test_disconnect_session_endpoint():
    """Menguji endpoint POST /api/v1/session/disconnect (Tombol OFF) memutus sesi ke OFFLINE."""
    import serving.app as app_mod

    orig_mode = app_mod.stream_mode
    orig_sim = app_mod.is_simulation_enabled
    try:
        app_mod.stream_mode = "SIMULATION_FALLBACK"
        app_mod.is_simulation_enabled = True

        resp = client.post("/api/v1/session/disconnect", headers={"role": "viewer"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "DISCONNECTED"
        assert data["stream_mode"] == "OFFLINE"
        assert data["is_real_live"] is False
        assert app_mod.is_simulation_enabled is False
    finally:
        app_mod.stream_mode = orig_mode
        app_mod.is_simulation_enabled = orig_sim

def test_ml_predict_returns_dual_layer_tags():
    """Menguji endpoint /api/v1/ml/predict mengembalikan predicted_intent dan intent_tags."""
    resp = client.post(
        "/api/v1/ml/predict",
        json={"comment_text": "buy product in where?"},
        headers={"role": "viewer"}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["predicted_intent"] == "INTENT_BUY"
    assert "QUESTION" in data["intent_tags"]
    assert len(data["intent_tags"]) <= 2

def test_extract_room_info_products():
    """Menguji ekstraksi produk commerce dari payload room_info TikTok."""
    from serving.app import extract_room_info_products

    mock_room_info = {
        "title": "BIG SALE AEROSTREET!",
        "user_count": 71,
        "has_commerce_goods": True,
        "commerce_info": {
            "pop_product": {
                "product_id": 17300112233,
                "live_product_number": 4,
                "title": "Aerostreet Aerofit Trax Jaket Parasut",
                "price": "Rp 200.013",
                "is_pinned": True
            }
        }
    }
    prods = extract_room_info_products(mock_room_info)
    assert len(prods) == 1
    assert prods[0]["etalase_no"] == 4
    assert prods[0]["product_name"] == "Aerostreet Aerofit Trax Jaket Parasut"
    assert prods[0]["price_idr"] == 200013
    assert prods[0]["source"] == "TIKTOK_LIVE_PIN"


