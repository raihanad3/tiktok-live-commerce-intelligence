"""
TikPulse - Machine Learning Training & Versioning (F-11 Requirement)
Tugas: Melatih Model Klasifikasi Niat Pembeli (Purchase Intent & Sentiment Classifier)
Fitur: Split train-test (seed=42), No data leakage, Evaluasi F1/Precision/Recall,
       MLflow Experiment Tracking & Local Artifact Persistence.
"""

import os
import json
import joblib
import pandas as pd
from datetime import datetime, timezone
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import MultinomialNB
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, precision_score, recall_score, accuracy_score
from sklearn.pipeline import Pipeline

# Konfigurasi MLflow & Path
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_DIR = os.path.dirname(__file__)
MODEL_ARTIFACT_PATH = os.path.join(MODEL_DIR, "model_intent.joblib")
MODEL_META_PATH = os.path.join(MODEL_DIR, "model_metadata.json")
HARVEST_CSV_PATH = os.path.abspath(os.path.join(MODEL_DIR, "..", "..", "data", "batch", "harvested_labeled_comments.csv"))

def get_training_dataset():
    """
    Dataset training anotasi intent bahasa Indonesia untuk e-commerce live streaming.
    Kategori:
      - INTENT_BUY   : Berniat beli / tanya stok / mau checkout
      - QUESTION     : Tanya ukuran / bahan / pengiriman
      - CASUAL       : Sapaan santai / bercanda / spam
      - COMPLAINT    : Komplain barang / pengiriman lama
    """
    data = [
        # INTENT_BUY
        ("Kak spill nomor 1 mau checkout", "INTENT_BUY"),
        ("Etalase 2 warna putih masih ada stok gak min?", "INTENT_BUY"),
        ("Mau ambil celana chino no 3 bisa COD gak?", "INTENT_BUY"),
        ("Checkout nomor 4 dapet gratis ongkir ga?", "INTENT_BUY"),
        ("Aku udah co ya min tolong kirim hari ini", "INTENT_BUY"),
        ("Spill nomor 5 kak mau langsung beli", "INTENT_BUY"),
        ("Harga diskon etalase 1 sampai jam berapa min?", "INTENT_BUY"),
        ("Bisa kirim instan gosend gak kak mau beli sekarang", "INTENT_BUY"),
        ("Kak nomor 3 ada promo beli 2 gratis 1?", "INTENT_BUY"),
        ("Mau order no 2 kak ukuran L", "INTENT_BUY"),

        # QUESTION
        ("Kemeja flanel no 1 bahannya tebel atau tipis kak?", "QUESTION"),
        ("BB 65 tinggi 170 muat ukuran apa ya min?", "QUESTION"),
        ("Bahan kulot no 4 nerawang gak kak?", "QUESTION"),
        ("Pengiriman dari kota mana kak?", "QUESTION"),
        ("Panjang celana chino nomor 3 berapa cm kak?", "QUESTION"),
        ("Dress no 5 ada resleting belakang atau depan?", "QUESTION"),
        ("Warna aslinya lebih gelap atau terang kak?", "QUESTION"),
        ("Kancingnya hidup atau variasi aja min?", "QUESTION"),

        # CASUAL
        ("Halo kak salam dari Medan", "CASUAL"),
        ("Follback dong kak cantik", "CASUAL"),
        ("Semangat live nya kak", "CASUAL"),
        ("Host nya mirip artis deh", "CASUAL"),
        ("Hai kak izin gabung", "CASUAL"),
        ("Lucu banget bajunya", "CASUAL"),
        ("Hadir kak", "CASUAL"),
        ("Bantu tap tap layar ya temen temen", "CASUAL"),

        # COMPLAINT
        ("Kak pesanan kemarin kok belum dikirim ya kecewa banget", "COMPLAINT"),
        ("Barang datang robek jahitannya ga rapi", "COMPLAINT"),
        ("Adminnya dichat ga balas balas tolong respon dong", "COMPLAINT"),
        ("Pengirimannya lama banget padahal udah 5 hari", "COMPLAINT"),
        ("Penipu nih warnanya beda sama yang di live", "COMPLAINT"),
        ("Minta retur barang rusak gimana caranya min", "COMPLAINT")
    ]
    df = pd.DataFrame(data, columns=["text", "label"])
    df["source"] = "curated_baseline"

    # Gabungkan dengan korpus nyata hasil TikTok Batch Harvester jika tersedia
    if os.path.exists(HARVEST_CSV_PATH):
        try:
            df_harvest = pd.read_csv(HARVEST_CSV_PATH)
            valid_labels = {"INTENT_BUY", "QUESTION", "CASUAL", "COMPLAINT"}
            if {"text", "label"}.issubset(df_harvest.columns):
                df_h = df_harvest[["text", "label"]].copy()
                df_h["text"] = df_h["text"].astype(str).str.strip()
                df_h = df_h[df_h["label"].isin(valid_labels) & (df_h["text"].str.len() >= 3)]
                df_h["source"] = "tiktok_harvest_batch"
                df = pd.concat([df, df_h], ignore_index=True)
        except Exception as e:
            print(f"[TRAIN WARN] Gagal membaca harvested_labeled_comments.csv: {e}")

    # Deduplikasi case-insensitive untuk mencegah data leakage antara train dan test
    df["_norm"] = df["text"].str.lower().str.strip()
    df = df.drop_duplicates(subset=["_norm"]).drop(columns=["_norm"]).reset_index(drop=True)
    return df

def train_and_evaluate(experiment_name="Baseline_LogisticRegression"):
    """
    Melatih model dengan MLflow tracking untuk membandingkan 3 eksperimen (F-11)
    menggunakan kombinasi dataset kurasi + data Batch Harvester TikTok.
    """
    df = get_training_dataset()
    X = df["text"]
    y = df["label"]
    harvested_count = int((df["source"] == "tiktok_harvest_batch").sum()) if "source" in df.columns else 0

    # Split train/test dengan random_state tetap untuk reproducibility
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )

    models_to_test = {
        "exp_1_logistic_regression": Pipeline([
            ("tfidf", TfidfVectorizer(ngram_range=(1, 2))),
            ("clf", LogisticRegression(random_state=42, max_iter=300, class_weight="balanced"))
        ]),
        "exp_2_multinomial_nb": Pipeline([
            ("tfidf", TfidfVectorizer(ngram_range=(1, 2))),
            ("clf", MultinomialNB(alpha=0.5))
        ]),
        "exp_3_random_forest": Pipeline([
            ("tfidf", TfidfVectorizer(ngram_range=(1, 2))),
            ("clf", RandomForestClassifier(n_estimators=80, random_state=42, class_weight="balanced"))
        ])
    }

    best_f1 = 0.0
    best_acc = 0.0
    best_pipeline = None
    best_name = ""

    print(f"\n=======================================================")
    print(f"MLflow Training Run: {len(X_train)} Train, {len(X_test)} Test (Total: {len(df)}, Harvested: {harvested_count})")
    print(f"=======================================================")

    for exp_id, pipeline in models_to_test.items():
        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_test)

        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, average="weighted")
        prec = precision_score(y_test, y_pred, average="weighted", zero_division=0)
        rec = recall_score(y_test, y_pred, average="weighted", zero_division=0)

        print(f"\n[HASIL EVALUASI] {exp_id}:")
        print(f" -> Accuracy : {acc:.4f}")
        print(f" -> F1-Score : {f1:.4f}")
        print(f" -> Precision: {prec:.4f}")
        print(f" -> Recall   : {rec:.4f}")

        # Coba log ke MLflow jika server aktif
        try:
            import mlflow
            mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
            mlflow.set_experiment("TikPulse_Purchase_Intent")
            with mlflow.start_run(run_name=exp_id):
                mlflow.log_param("model_type", exp_id)
                mlflow.log_param("total_samples", len(df))
                mlflow.log_param("harvested_samples", harvested_count)
                mlflow.log_metric("accuracy", acc)
                mlflow.log_metric("f1_score", f1)
                mlflow.log_metric("precision", prec)
                mlflow.log_metric("recall", rec)
                mlflow.sklearn.log_model(pipeline, "model")
                print(f" [MLflow] Berhasil mencatat run ke {MLFLOW_TRACKING_URI}")
        except Exception as e:
            print(f" [MLflow Server Offline - Lewati remote logging]: {e}")

        if f1 >= best_f1:
            best_f1 = f1
            best_acc = acc
            best_pipeline = pipeline
            best_name = exp_id

    # Simpan model terbaik ke disk sebagai artefak produksi + metadata training
    joblib.dump(best_pipeline, MODEL_ARTIFACT_PATH)
    meta = {
        "best_model": best_name,
        "f1_score": round(float(best_f1), 4),
        "accuracy": round(float(best_acc), 4),
        "total_training_samples": len(df),
        "harvested_tiktok_samples": harvested_count,
        "baseline_samples": len(df) - harvested_count,
        "trained_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(MODEL_META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(f"\n[MODEL SAVED] Model terbaik '{best_name}' (F1: {best_f1:.4f}, Samples: {len(df)}) disimpan di: {MODEL_ARTIFACT_PATH}")
    return best_pipeline

if __name__ == "__main__":
    train_and_evaluate()
