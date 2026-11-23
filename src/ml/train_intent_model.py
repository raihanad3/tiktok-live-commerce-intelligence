"""
TikPulse - Machine Learning Training & Versioning (F-11 Requirement)
Arsitektur ML-A+:
  - Indonesian TikTok Slang & Typo Text Normalizer (common.text_normalizer)
  - FeatureUnion: Word N-Gram TF-IDF (1,2) + Character Subword N-Gram TF-IDF (char_wb 3..5)
  - Benchmark 3 Classifier Berbasis Jurnal 2024-2026:
      1. Word+Char TF-IDF + LogisticRegression (C=2.5, class_weight='balanced')
      2. Word+Char TF-IDF + Calibrated LinearSVC
      3. Calibrated Soft-Voting Ensemble (LinearSVC + LogisticRegression + ComplementNB)
"""

import os
import sys
import joblib
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.naive_bayes import ComplementNB
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import VotingClassifier
from sklearn.metrics import f1_score, precision_score, recall_score, accuracy_score
from sklearn.pipeline import Pipeline, FeatureUnion

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.text_normalizer import normalize_tiktok_text

# Konfigurasi MLflow (timeout singkat agar tidak hang bila server MLflow offline)
os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "1")
os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "0")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_DIR = os.path.dirname(__file__)
MODEL_ARTIFACT_PATH = os.path.join(MODEL_DIR, "model_intent.joblib")

def get_training_dataset():
    """
    Dataset training seimbang (320 sampel: 80 per kelas) mencakup chat sangat pendek,
    kalimat panjang, slang/singkatan TikTok Live Indonesia, dan semi-English.
    """
    intent_buy = [
        "Kak spill nomor 1 mau checkout",
        "Etalase 2 warna putih masih ada stok gak min?",
        "Mau ambil celana chino no 3 bisa COD gak?",
        "Checkout nomor 4 dapet gratis ongkir ga?",
        "Aku udah co ya min tolong kirim hari ini",
        "Spill nomor 5 kak mau langsung beli",
        "Harga diskon etalase 1 sampai jam berapa min?",
        "Bisa kirim instan gosend gak kak mau beli sekarang",
        "Kak nomor 3 ada promo beli 2 gratis 1?",
        "Mau order no 2 kak ukuran L",
        "Fix ambil etalase 7 warna hitam ya kak",
        "Udah co 2 pcs min tolong diproses cepet",
        "Keep no 4 dong kak mau checkout sekarang",
        "Gas co etalase 6 mumpung flash sale",
        "Kak link beli etalase 8 di keranjang kuning mana?",
        "Mau beli 3 botol serum no 2 dapet bonus ga?",
        "Sdh co warna mocca ya ka tolong amanah",
        "Bisa bayar di tempat COD buat order no 5?",
        "Pengen beli sepatu no 1 tapi takut kehabisan",
        "Order sekarang dikirim hari ini gak kak?",
        "Spill etalase 9 dong min siap co nih",
        "Checkout skrg masih dapet voucher live?",
        "Mau pesan 2 lusin buat stok jualan lagi",
        "Saya ambil yang dipegang host warna navy",
        "Cara checkout keranjang kuning gimana kak mau beli",
        "I want to buy this item right now",
        "Where is the yellow basket link to order?",
        "Already checked out number 3 please ship today",
        "Buy product in where?",
        "Can I order two pcs with COD?",
        "Mau co etalase 10 tapi stok tinggal 2 buruan",
        "Titip pesan no 1 ukuran XL langsung bayar",
        "Co no 1",
        "Checkout kak",
        "Mau beli jaket nya dong bang",
        "Saya udah checkout warna olive ya bang tolong kirim",
        "Beli 2 dapat diskon tambahan ga mau co nih",
        "Link keranjang kuning nya mana mau order",
        "Otw checkout etalase 12 warna cream",
        "Udah bayar ya min pesanan no 3 ditunggu resinya",
        "Mau order jaket parasut nya 2 pcs",
        "Fix order hoodie hitam size XL sekarang",
        "Tolong spill etalase 2 mau langsung co payment",
        "Ada voucher live gak buat checkout jam ini?",
        "Aku ambil satu ya kak warna sage green",
        "Pesan celana kargo no 5 dua biji ya min",
        "Ready co sekarang kak kirim pakai kargo bisa?",
        "Mau beli buat kado besok bisa sameday?",
        "Udah masuk orderan saya belum kak barusan co",
        "Kak checkout nya lewat keranjang oranye atau kuning?",
        "Mau check out sekarang takut kehabisan warna hitam",
        "Order nomor 6 size M satu ya bang",
        "Gas beli mumpung harga live turun",
        "Saya mau pesan bundle hemat etalase 4",
        "Bisa COD ke Makassar gak mau beli sepatu",
        "Check out no 7 ya ka",
        "Pesan 1 lusin dapet harga grosir kan mau co",
        "Udah transfer ya min tolong dipacking rapi",
        "Order now how to pay?",
        "I will buy size M please pin it",
        "Take 2 pcs for number 3",
        "Add to cart and checkout done!",
        "Mau beli kaos polos 5 pcs campur warna",
        "Co sepatu aerostreet no 1 ukuran 42 ya min",
        "Langsung co nih mumpung diskon live",
        "Siap checkout etalase 15 kalau stok aman",
        "Aku udah beli 3 item sekaligus kak",
        "Mau order gamis warna dusty pink no 2",
        "Beli sekarang besok udah dikirim belum kak?",
        "Keep warna biru navy ukuran L mau bayar",
        "Spill no 11 cepetan kak jari udah siap co",
        "Tolong pin etalase jaket mau aku checkout",
        "Mau borong skincare nya mumpung promo live",
        "Checkout pake shopee pay atau dana bisa gak mau beli",
        "Barusan co dua jaket ya bang tolong cek",
        "Fix beli yang lagi dipakai model sekarang",
        "Orderan atas nama Rian udah masuk kan min?",
        "Mau beli tapi bingung pilih warna bantu spill mau co",
        "Checkout 1 ya min warna mocca",
        "Ready to buy please show product 4"
    ]

    question = [
        "Kemeja flanel no 1 bahannya tebel atau tipis kak?",
        "BB 65 tinggi 170 muat ukuran apa ya min?",
        "Bahan kulot no 4 nerawang gak kak?",
        "Pengiriman dari kota mana kak?",
        "Panjang celana chino nomor 3 berapa cm kak?",
        "Dress no 5 ada resleting belakang atau depan?",
        "Warna aslinya lebih gelap atau terang kak?",
        "Kancingnya hidup atau variasi aja min?",
        "Lp 86 bb 66 tb 156 celana yg di pegang size apa?",
        "Ld 110 bb 72 muat pake ukuran L atau XL kak?",
        "Lingkar paha celana ini berapa cm ya min?",
        "Tinggi badan 155 panjang bajunya sampai lutut ga?",
        "Bahannya melar atau kaku kak?",
        "Sepatu no 2 insole nya berapa cm untuk size 42?",
        "Sunscreen ini aman buat bumil dan busui gak kak?",
        "Cocok untuk kulit berminyak dan berjerawat ga min?",
        "Lipmatte nya transferproof dan tahan berapa jam?",
        "Itu warna cream atau broken white sih kak?",
        "Ada kantong kanan kiri gak di rok nya?",
        "Dipakai siang hari gerah atau adem bahannya?",
        "Isinya berapa gram atau berapa ml ya kak?",
        "Expired date produknya masih lama gak min?",
        "Original resmi bpom kan kak produknya?",
        "Bedanya varian A sama varian B apa ya kak?",
        "Kalau dicuci luntur atau nyusut gak kainnya?",
        "Yang dipegang host itu etalase nomor berapa?",
        "Ukuran paling besar sampai BB berapa kg kak?",
        "What size fits 65kg weight and 160cm height?",
        "Is this fabric stretchy or sheer?",
        "Where do you ship from and how long does it take?",
        "Does it come with warranty or box?",
        "What shade is the host wearing right now?",
        "jaket bng",
        "spill jaket bang",
        "sepatu kak",
        "hoodie hitam min",
        "celana kargo no berapa bang?",
        "udah masuk ga ka",
        "masuk ga ka",
        "muat ga ka buat bb 75?",
        "ready gak size 41?",
        "ada warna hijau botol gak bang?",
        "jaket parasut anti air gak bang?",
        "bisa buat cowok cewek gak bang?",
        "tinggi 168 berat 60 pakai M atau L?",
        "size L lingkar dada berapa cm ka?",
        "no 4 masih ada warna putih gak?",
        "kalau hujan tembus air gak jaketnya?",
        "bahan katun combed berapa s kak?",
        "berat jaketnya berapa gram min?",
        "harga normalnya berapa sih kak?",
        "promo nya sampai jam berapa ya ka?",
        "bisa tukar size kalau kekecilan gak?",
        "itu etalase nomor berapa yg dipegang?",
        "kaos kaki dapat berapa pasang ka?",
        "ada saku dalam nya gak jaket ini?",
        "resleting nya YKK atau biasa bang?",
        "sol sepatunya dijahit atau lem aja?",
        "lip balm nya ada stain warnanya gak?",
        "cushion ini oksidasi gak di kulit sawo matang?",
        "raket ini tarikan maksimal berapa lbs?",
        "charger nya support fast charging iPhone gak?",
        "sambalnya tahan berapa hari di suhu ruang?",
        "size chart nya lihat dimana kak?",
        "how much is the jacket?",
        "is it waterproof?",
        "ready stock ga?",
        "free shipping to Surabaya?",
        "what material is this?",
        "ada ukuran jumbo XXL gak kak?",
        "kainnya gampang kusut ga min?",
        "bisa dicuci di mesin cuci gak?",
        "warnanya sama persis sama di foto?",
        "itu sepatu running atau casual bang?",
        "panjang lengan jaketnya berapa cm?",
        "etalase 1 sama 3 bedanya apa ka?",
        "muat buat kaki lebar gak sepatunya?",
        "ada resleting samping gak celananya?",
        "kapan restock ukuran M nya kak?",
        "yg warna army etalase brp bng?"
    ]

    casual = [
        "Halo kak salam dari Medan",
        "Follback dong kak cantik",
        "Semangat live nya kak",
        "Host nya mirip artis deh",
        "Hai kak izin gabung",
        "Lucu banget bajunya",
        "Hadir kak",
        "Bantu tap tap layar ya temen temen",
        "Assalamualaikum kak selamat malam",
        "Salam kenal dari Surabaya kak",
        "Rame banget live nya hari ini",
        "Suaranya jelas kok kak lanjut",
        "Host nya ramah banget suka deh",
        "Cakep banget warnanya kelihatan mewah",
        "Nyimak dulu ya kak sambil ngopi",
        "Izin pantau terus kak semangat",
        "Tap tap layar biar makin rame",
        "Halo min baru masuk live nih",
        "Wih keren banget produk lokal",
        "Sukses terus jualannya ya kak",
        "Mantap host nya jago jelasin",
        "Gabung lagi kak malam ini",
        "Salfok sama background nya bagus",
        "Dari Jakarta hadir meramaikan",
        "Haha lucu banget komentarnya",
        "Oke siap makasih infonya kak",
        "Makasih udah dibacain komennya",
        "Hello from Malaysia nice stream",
        "Watching from Singapore keep it up",
        "Good evening everyone",
        "Nice host very friendly",
        "Wow looks amazing today",
        "dapat diskon langsung wkwkw dari 200 san ke 180 han",
        "wkwk dapat harga murah banget aku tadi",
        "mantap banget dapet potongan gede wkwkw",
        "wah murah banget jadi 180 ribuan doang",
        "keren bener diskonnya nyampe 70 persen",
        "alhamdulillah barangku kemarin udah sampai bagus banget",
        "puas banget belanja disini kemarin cepat nyampe",
        "aku udah punya warna hitamnya nyaman banget dipakai",
        "recommended banget sih toko ini bahannya adem",
        "gokil host nya lucu abis",
        "selamat malam semua yang lagi nonton",
        "bantu share live nya ya guys",
        "salam dari Bandung kak",
        "salam dari Semarang hadir",
        "ikut nimbrung bentar ya bang",
        "mantap bang lanjut terus",
        "cakep bener jaketnya dipake host",
        "keren bang stylenya",
        "bagus banget modelnya suka",
        "hehe makasih ya kak udah dijawab",
        "sip oke paham kak",
        "gas terus bang jualannya",
        "semoga laris manis malam ini",
        "ramai banget nih penontonnya",
        "suara aman gambar jernih bang",
        "ciee dapet harga flash sale",
        "beruntung banget aku dapet voucher tadi wkwk",
        "harganya ramah di kantong mahasiswa",
        "seneng banget paketku kemarin dapet bonus stiker",
        "kualitas aerostreet memang tidak diragukan lagi",
        "lokal pride mantap betul",
        "iya bener banget kak setuju",
        "mantul bang",
        "oke kak thank you infonya",
        "sip makasih banyak min",
        "sama sama kak",
        "lagi santai sambil nonton live",
        "first time watching this live",
        "love the color so much",
        "great stream today!",
        "haha so funny",
        "nice jacket bro",
        "good quality local brand",
        "aku tim nyimak aja dulu ya",
        "nabung dulu ntar gajian baru beli wkwk",
        "abis gajian langsung kesini lagi deh",
        "terbaik memang toko ini",
        "halo bang sehat selalu"
    ]

    complaint = [
        "Kak pesanan kemarin kok belum dikirim ya kecewa banget",
        "Barang datang robek jahitannya ga rapi",
        "Adminnya dichat ga balas balas tolong respon dong",
        "Pengirimannya lama banget padahal udah 5 hari",
        "Penipu nih warnanya beda sama yang di live",
        "Minta retur barang rusak gimana caranya min",
        "Paket saya nyampe tapi kurang 1 barang min",
        "Kenapa yang dikirim salah ukuran padahal pesan XL",
        "Baju datang ada noda kotor dan bau apek",
        "Resi belum update dari kemarin tolong cek",
        "Botolnya bocor tumpah semua pas dibuka",
        "Kaca bedaknya pecah padahal udah minta bubble wrap",
        "Bahannya tipis banget beda sama yang dijanjikan",
        "Saya mau ajukan pengembalian dana refund min",
        "Kecewa pesan hitam yang datang malah abu",
        "Komplain ke CS dari pagi tidak ada jawaban",
        "Kualitasnya cacat resleting macet tidak bisa dipakai",
        "Udah seminggu paket nyangkut di ekspedisi terus",
        "Jangan beli di sini barangnya rusak pas sampai",
        "Salah kirim warna nih min padahal buat acara besok",
        "Packing nya asal asalan penyok semua kardusnya",
        "Produk tidak sesuai deskripsi sangat mengecewakan",
        "Refund saya kapan cair ya kak udah 3 hari",
        "Barang palsu bukan original ini mah",
        "Malah dikirim barang bekas ada bekas pakainya",
        "Nyesel beli jahitan bagian ketiak bolong",
        "Tolong tanggung jawab barangnya patah di jalan",
        "My package arrived damaged and broken",
        "Wrong size delivered very disappointed",
        "Shipping is way too slow still not sent",
        "Customer service never replies to my message",
        "Want a refund item is defective",
        "Barang datang cacat sobek bagian belakang",
        "Kok dikirimnya beda model sama etalase kak kecewa",
        "Sudah 10 hari paket tidak bergerak di gudang",
        "Sepatu yang dikirim lem nya lepas semua parah",
        "Resleting jaket macet pas baru dibuka dari plastik",
        "Kotak sepatu hancur penyok parah pas diterima",
        "Kenapa jumlah barang kurang 2 pcs sih min",
        "Chat di TikTok shop tidak pernah dibalas admin",
        "Mau ajukan komplain pengembalian barang rusak",
        "Kecewa berat warna pudar dan luntur pas dicuci pertama",
        "Kirimnya lambat banget udah seminggu belum dikemas",
        "Barang reject kenapa masih dikirim ke pembeli",
        "Saya dirugikan pesan 2 yang datang cuma 1",
        "Parah banget jahitan lepas dan benang berantakan",
        "Tutup botol pecah isi serum tumpah kemana mana",
        "Layar TWS mati sebelah baru dipake sehari rusak",
        "Kabel charger gak ngecas sama sekali barang rusak",
        "Salah kirim produk pesan kemeja yang datang kaos",
        "Kualitas murahan tidak sesuai ekspektasi kecewa",
        "Pengajuan retur saya ditolak terus padahal ada video unboxing",
        "Minta ganti rugi barang pecah di pengiriman",
        "Bau apek banget bajunya kayak stok lama berjamur",
        "Nomor resi fiktif gak bisa dilacak udah 4 hari",
        "Bohong katanya ready ternyata preorder lama banget",
        "Ukuran XL tapi kecil banget kayak ukuran S salah tag",
        "Admin gak niat jualan ditanya komplain malah diem",
        "Paket nyasar dan tidak ada tanggung jawab penjual",
        "Rugi beli di sini barang tidak lengkap",
        "Produk kadaluarsa expired bulan lalu kok dikirim",
        "Sol sepatu kiri kanan beda ukuran parah banget",
        "Tas baru dipakai sekali tali nya langsung putus",
        "Gak amanah pesan warna navy dikasih marun tanpa konfirmasi",
        "Komplain paket bocor bau menyengat",
        "Kainnya bolong kecil di tengah dada cacat produksi",
        "Kenapa statusnya masih dikemas terus dari minggu lalu",
        "Jangan percaya foto produknya beda jauh aslinya jelek",
        "Tolong proses refund saya barangnya cacat berat",
        "Lama amat proses pengirimannya keburu acaranya lewat",
        "Order canceled without reason very annoying",
        "Missing item inside my parcel please refund",
        "Bad quality stitching ripped on arrival",
        "Seller not responding to my complaint at all",
        "Received wrong color and wrong size",
        "Kecewa banget pengiriman molor parah",
        "Paket penyok isi rusak pecah semua",
        "Tidak sesuai pesanan minta pengembalian dana",
        "Ada noda tinta di kerah baju padahal baru buka segel",
        "Barang bekas retur dikirim ulang ke saya kotor banget"
    ]

    data = (
        [(t, "INTENT_BUY") for t in intent_buy]
        + [(t, "QUESTION") for t in question]
        + [(t, "CASUAL") for t in casual]
        + [(t, "COMPLAINT") for t in complaint]
    )
    df = pd.DataFrame(data, columns=["text", "label"])
    df["norm_text"] = df["text"].apply(normalize_tiktok_text)
    return df

def build_feature_union() -> FeatureUnion:
    """Menggabungkan Word N-Gram (konteks kata) + Character Subword N-Gram (tahan slang & typo)."""
    return FeatureUnion([
        ("word_tfidf", TfidfVectorizer(ngram_range=(1, 2), sublinear_tf=True)),
        ("char_tfidf", TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), sublinear_tf=True, min_df=2)),
    ])

def train_and_evaluate(experiment_name="ML_A_Plus_WordChar_Ensemble"):
    """Melatih dan mengevaluasi 3 model ML-A+ berbasis jurnal 2024-2026."""
    df = get_training_dataset()
    X = df["norm_text"]
    y = df["label"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, random_state=42, stratify=y
    )

    models_to_test = {
        "exp_1_word_char_logreg": Pipeline([
            ("features", build_feature_union()),
            ("clf", LogisticRegression(C=2.5, class_weight="balanced", max_iter=500, random_state=42)),
        ]),
        "exp_2_word_char_calibrated_svm": Pipeline([
            ("features", build_feature_union()),
            ("clf", CalibratedClassifierCV(
                LinearSVC(C=1.0, class_weight="balanced", random_state=42),
                cv=3
            )),
        ]),
        "exp_3_calibrated_soft_voting_ensemble": Pipeline([
            ("features", build_feature_union()),
            ("clf", VotingClassifier(
                estimators=[
                    ("svm", CalibratedClassifierCV(
                        LinearSVC(C=1.0, class_weight="balanced", random_state=42),
                        cv=3
                    )),
                    ("logreg", LogisticRegression(C=2.5, class_weight="balanced", max_iter=500, random_state=42)),
                    ("cnb", ComplementNB(alpha=0.5)),
                ],
                voting="soft",
                weights=[2, 2, 1],
            )),
        ]),
    }

    best_f1 = 0.0
    best_pipeline = None
    best_name = ""

    print("\n=======================================================")
    print(f"ML-A+ Training Run: {len(X_train)} Train, {len(X_test)} Test (Total {len(df)})")
    print("=======================================================")

    for exp_id, pipeline in models_to_test.items():
        pipeline.fit(X_train, y_train)
        y_pred = pipeline.predict(X_test)

        acc = accuracy_score(y_test, y_pred)
        f1_w = f1_score(y_test, y_pred, average="weighted")
        f1_m = f1_score(y_test, y_pred, average="macro")
        prec = precision_score(y_test, y_pred, average="weighted", zero_division=0)
        rec = recall_score(y_test, y_pred, average="weighted", zero_division=0)

        print(f"\n[HASIL EVALUASI] {exp_id}:")
        print(f" -> Accuracy    : {acc:.4f}")
        print(f" -> F1-Weighted : {f1_w:.4f}")
        print(f" -> F1-Macro    : {f1_m:.4f}")
        print(f" -> Precision   : {prec:.4f}")
        print(f" -> Recall      : {rec:.4f}")

        try:
            import mlflow
            mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
            mlflow.set_experiment("TikPulse_Purchase_Intent")
            with mlflow.start_run(run_name=exp_id):
                mlflow.log_param("model_type", exp_id)
                mlflow.log_metric("accuracy", acc)
                mlflow.log_metric("f1_score", f1_w)
                mlflow.log_metric("f1_macro", f1_m)
                mlflow.log_metric("precision", prec)
                mlflow.log_metric("recall", rec)
                mlflow.sklearn.log_model(pipeline, "model")
                print(f" [MLflow] Berhasil mencatat run ke {MLFLOW_TRACKING_URI}")
        except Exception as e:
            print(f" [MLflow Server Offline - Lewati remote logging]: {e}")

        if f1_w >= best_f1:
            best_f1 = f1_w
            best_pipeline = pipeline
            best_name = exp_id

    joblib.dump(best_pipeline, MODEL_ARTIFACT_PATH)
    print(f"\n[MODEL SAVED] Model terbaik '{best_name}' (F1-Weighted: {best_f1:.4f}) disimpan di: {MODEL_ARTIFACT_PATH}")
    return best_pipeline

if __name__ == "__main__":
    train_and_evaluate()
