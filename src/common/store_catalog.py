"""
TikPulse - Store Catalog & Niche Detection Engine
Menyediakan katalog dinamis dan komentar live stream yang 100% selaras dengan nama toko.
"""

import re
import pandas as pd

STORE_NICHES = {
    "sports": {
        "keywords": ["sport", "raket", "badminton", "jersey", "bola", "gym", "fit", "run", "atlet", "ray", "raysportindo"],
        "category": "Olahraga & Badminton",
        "products": [
            (1, "SPT-RKT-001", "Raket Badminton Carbon Pro Series 4U", 450000, 30, "raket,badminton,bulutangkis,carbon"),
            (2, "SPT-SPU-002", "Sepatu Bulutangkis Grip Non-Marking", 389000, 25, "sepatu,badminton,grip,non marking"),
            (3, "SPT-STC-003", "Shuttlecock Tournament Grade 1 Slop Isi 12", 115000, 80, "shuttlecock,kok,slop,bulutangkis"),
            (4, "SPT-JRS-004", "Jersey Badminton Dryfit Breathable", 85000, 100, "jersey,baju badminton,dryfit,olahraga"),
            (5, "SPT-GRP-005", "Grip Raket Karet Anti Licin Isi 3 Pcs", 25000, 150, "grip,karet,pegangan,raket"),
            (6, "SPT-TAS-006", "Tas Raket Badminton Thermo 2 Kompartemen", 210000, 20, "tas raket,thermo,badminton bag"),
            (7, "SPT-SNR-007", "Senar Raket Nanogy 98 Original", 95000, 60, "senar,string,nanogy,tarikan"),
            (8, "SPT-KNE-008", "Knee Pad Support Pelindung Lutut Sport", 45000, 50, "knee pad,pelindung lutut,deker"),
            (9, "SPT-KKS-009", "Kaos Kaki Tebal Badminton Anti Slip", 30000, 90, "kaos kaki,anti slip,tebal,sport"),
            (10, "SPT-CLN-010", "Celana Pendek Sport Olahraga Running Dryfit", 65000, 70, "celana pendek,sport,running,kolor")
        ],
        "comments": [
            "Kak tolong spill raket badminton no 1 dong tarikan senar berapa lbs mau checkout!",
            "Sepatu bulutangkis no 2 ready size 42 warna putih hitam gak min?",
            "Shuttlecock no 3 dapet 1 slop isi 12 kan kak mau beli 2 slop sekarang",
            "Jersey badminton no 4 bahannya adem dan nyerap keringat gak kak?",
            "Spill grip raket no 5 kak mau co bareng raket no 1",
            "Bisa COD raket no 1 ke Surabaya min?",
            "Udah checkout raket no 1 ya min tolong di-packing kayu yang aman!",
            "Tas raket no 6 muat berapa raket kak tolong spill ruang dalamnya",
            "Senar no 7 bisa langsung dipasangin di raket gak min?",
            "Sepatu no 2 sol bawahnya empuk gak kak buat lompat smash?",
            "Keren banget barangnya min salam dari anak bulutangkis!",
            "Udah checkout jersey no 4 ukuran L ya tolong diproses min",
            "Kaos kaki no 9 bahannya tebel gak min?",
            "Kok no 3 lajunya stabil gak kak buat latihan tanding?",
            "Packing raket no 1 aman sampai Papua gak min mau beli sekarang"
        ]
    },
    "shoes": {
        "keywords": ["sepatu", "shoe", "sneaker", "foot", "aerostreet", "ventela", "compass"],
        "category": "Footwear & Sepatu",
        "products": [
            (1, "SH-SNK-001", "Sepatu Sneakers Retro Vulcanized White Gum", 149000, 50, "sneakers,sepatu,vulcanized,putih,retro"),
            (2, "SH-RUN-002", "Sepatu Running Ultra Light Mesh Breathable", 169000, 40, "running,lari,mesh,ringan,olahraga"),
            (3, "SH-SLP-003", "Sepatu Slip On Kasual Kanvas Classic", 119000, 60, "slip on,kasual,kanvas,tanpa tali"),
            (4, "SH-SND-004", "Sandal Slide Foam Empuk Anti Licin", 69000, 100, "sandal,slide,eva foam,empuk"),
            (5, "SH-INS-005", "Insole Memory Foam Antibakteri Ortopedi", 35000, 120, "insole,alas sepatu,memory foam"),
            (6, "SH-CLN-006", "Shoe Cleaner Foam Pembersih Sepatu Instan", 49000, 80, "cleaner,pembersih sepatu,foam"),
            (7, "SH-LCE-007", "Tali Sepatu Katun Pipih Premium 120cm", 15000, 200, "tali sepatu,shoelaces,katun"),
            (8, "SH-SOX-008", "Kaos Kaki Ankle Socks Katun 3 Pasang", 25000, 150, "kaos kaki,ankle,semata kaki"),
            (9, "SH-HGH-009", "Sepatu Sneakers High Vintage White Black", 179000, 30, "high,tinggi,vintage,sneaker"),
            (10, "SH-WTR-010", "Waterproof Spray Pelindung Sepatu Nano", 55000, 75, "waterproof spray,anti air,nano")
        ],
        "comments": [
            "Kak tolong spill sneakers retro nomor 1 dong mau checkout!",
            "Sepatu no 1 warna putih gum ready size 42 gak min?",
            "Sepatu running no 2 bahannya enteng gak kak mau beli buat jogging",
            "Slip on no 3 muat untuk kaki lebar gak min mau co",
            "Sandal slide no 4 bahan eva foam nya empuk gak kak?",
            "Spill etalase 1 min mau lihat jahitan sol sampingnya",
            "Pembersih sepatu no 6 aman buat bahan suede gak min?",
            "Udah checkout sneakers no 1 ya min tolong segera dikirim!",
            "Tali sepatu no 7 panjang berapa cm kak?",
            "Bisa COD sepatu no 2 ke Bandung min?"
        ]
    },
    "skincare": {
        "keywords": ["skin", "beauty", "glow", "cosmetic", "makeup", "serum", "somethinc", "skintific", "originote", "scarlett", "wardah"],
        "category": "Skincare & Beauty",
        "products": [
            (1, "SK-SRM-001", "Serum Niacinamide 10% Brightening Glow", 115000, 90, "serum,niacinamide,brightening,mencerahkan"),
            (2, "SK-SUN-002", "Sunscreen Gel SPF 50+ PA++++ No Whitecast", 89000, 120, "sunscreen,tabir surya,spf50,gel"),
            (3, "SK-MST-003", "Moisturizer 5X Ceramide Barrier Repair", 129000, 75, "moisturizer,ceramide,skin barrier,pelembab"),
            (4, "SK-CLN-004", "Gentle Facial Wash Cleanser Low pH", 65000, 110, "facial wash,cleanser,sabun cuci muka"),
            (5, "SK-CSH-005", "Cushion Foundation Flawless Matte SPF 35", 145000, 50, "cushion,foundation,matte,bedak"),
            (6, "SK-LIP-006", "Velvet Matte Lip Tint Longlasting Transferproof", 49000, 150, "lip tint,liptint,velvet,matte,bibir"),
            (7, "SK-TNR-007", "Exfoliating AHA BHA PHA Peeling Toner", 98000, 60, "toner,exfoliating,aha bha,peeling"),
            (8, "SK-RET-008", "Retinol Rejuvenating Night Cream 20g", 135000, 40, "retinol,night cream,anti aging"),
            (9, "SK-MSK-009", "Sheet Mask Hydrating Hyaluronic Isi 5 Pcs", 40000, 100, "sheet mask,masker wajah,hydrating"),
            (10, "SK-EYE-010", "Eye Cream Peptide Dark Circle Remover", 75000, 70, "eye cream,krim mata,mata panda")
        ],
        "comments": [
            "Kak tolong spill serum brightening no 1 dong mau checkout!",
            "Sunscreen no 2 ninggalin whitecast gak min di kulit sawo matang?",
            "Moisturizer no 3 aman buat kulit berjerawat dan fungal acne?",
            "Udah co cushion no 5 shade sand ya min tolong cepat dikirim!",
            "Lip tint no 6 transferproof gak kak tahan berapa jam?",
            "Spill etalase 1 min mau lihat tekstur cairannya",
            "Paket etalase 1 dan 2 ada diskon bundling gak min?",
            "Facial wash no 4 busanya lembut gak kak bikin ketarik ga?",
            "Toner no 7 bisa dipake tiap hari gak min?",
            "Udah checkout serum no 1 dan sunscreen no 2 ya min!"
        ]
    },
    "hijab": {
        "keywords": ["hijab", "gamis", "butik", "dress", "muslim", "khimar", "abaya", "rabbani", "zoya", "wanita", "modest"],
        "category": "Fashion Muslimah",
        "products": [
            (1, "HB-PSH-001", "Pashmina Silk Premium Shiny Soft Flowy", 59000, 120, "pashmina,silk,hijab,kerudung,shiny"),
            (2, "HB-VOL-002", "Hijab Segiempat Voal Watersplash Anti Air", 45000, 150, "voal,segiempat,kerudung,watersplash"),
            (3, "HB-ABY-003", "Gamis Abaya Silk Turkey Elegan Busui Friendly", 225000, 35, "gamis,abaya,silk,turkey,dress muslim"),
            (4, "HB-KLT-004", "Celana Kulot Linen Highwaist Loose Fit", 65000, 80, "kulot,linen,celana wanita,highwaist"),
            (5, "HB-DRS-005", "Midi Dress Floral Vintage Korean Style", 110000, 45, "dress,midi dress,floral,vintage,korean"),
            (6, "HB-MKN-006", "Mukena Parasut Traveling Mini Pouch Ringan", 89000, 70, "mukena,traveling,parasut,mini pouch"),
            (7, "HB-CPT-007", "Inner Ciput Bandana Rajut Anti Pusing 4 Pcs", 18000, 200, "ciput,inner hijab,bandana rajut"),
            (8, "HB-TNK-008", "Tunik Rayon Twill Premium Adem Busui", 95000, 60, "tunik,rayon twill,atasan wanita"),
            (9, "HB-PLK-009", "Rok Plisket Premium Tebal Payung Flare", 55000, 90, "rok plisket,rok payung,bawahan"),
            (10, "HB-BRS-010", "Bros Dagu Mutiara Hijab Pin Tuspin Mewah", 15000, 250, "bros,tuspin,pin hijab,mutiara")
        ],
        "comments": [
            "Kak spill pashmina silk no 1 warna champagne dong mau checkout!",
            "Voal no 2 beneran anti air gak min kalo kena cipratan?",
            "Gamis abaya no 3 muat sampai BB 75 kg gak kak?",
            "Kulot no 4 warna cream nerawang gak min bahannya?",
            "Spill dress vintage no 5 kak mau lihat motif bunganya",
            "Mukena traveling no 6 ukurannya pas dilipat sekecil apa min?",
            "Udah checkout gamis abaya no 3 ya tolong dikirim hari ini!",
            "Pashmina no 1 gampang dibentuk gak kak licin ga?",
            "Bisa COD ke Bandung gak kak?",
            "Suka banget sama motif dress no 5 kak semangaat!"
        ]
    },
    "gadget": {
        "keywords": ["gadget", "phone", "hp", "case", "elektronik", "ibox", "anker", "apple", "xiaomi", "samsung", "tech"],
        "category": "Gadget & Aksesoris",
        "products": [
            (1, "GD-CAS-001", "Casing Magnetic Shockproof Hybrid Case", 45000, 150, "casing,case hp,magnetic,shockproof"),
            (2, "GD-KBL-002", "Kabel Fast Charging 65W Braided Type-C", 39000, 200, "kabel data,fast charging,type c,65w"),
            (3, "GD-PWR-003", "Powerbank 20000mAh Dual Port PD 22.5W", 189000, 50, "powerbank,20000mah,fast charging,pd"),
            (4, "GD-TWS-004", "TWS Bluetooth 5.3 Earphone Bass Noise Reduction", 129000, 80, "tws,earphone,bluetooth,headset"),
            (5, "GD-TMP-005", "Tempered Glass Full Cover Anti Spy Privacy", 25000, 250, "tempered glass,anti gores,anti spy"),
            (6, "GD-HLD-006", "Stand Holder HP Meja Lipat Aluminium", 35000, 100, "holder hp,stand meja,aluminium"),
            (7, "GD-CHG-007", "Kepala Charger GaN 33W Fast Charging Dual Port", 79000, 90, "charger,kepala charger,gan,33w"),
            (8, "GD-WAT-008", "Smartwatch Fitness Tracker IP68 Waterproof", 249000, 30, "smartwatch,jam tangan pintar,fitness"),
            (9, "GD-RNG-009", "Ring Light Tripod 26cm Live TikTok Streaming", 85000, 45, "ring light,tripod,live streaming"),
            (10, "GD-OTG-010", "Adapter OTG USB 3.0 to Type-C Metal Compact", 19000, 300, "otg,adapter,type c,usb")
        ],
        "comments": [
            "Kak tolong spill casing shockproof no 1 untuk tipe hp ku dong!",
            "Kabel data no 2 support fast charging 65W gak min mau checkout!",
            "Powerbank no 3 boleh dibawa masuk pesawat terbang gak kak?",
            "TWS no 4 suaranya ngebass gak min delay ga buat main game?",
            "Spill tempered glass no 5 kak gampang pasang sendiri gak?",
            "Udah co powerbank no 3 ya tolong packing bubble wrap tebel!",
            "Smartwatch no 8 notifikasi WA bisa masuk gak kak?",
            "Bisa COD ke Semarang min?",
            "Charger no 7 cepet panas gak min pas ngecas?",
            "Spill holder hp no 6 min kokoh gak buat tablet?"
        ]
    },
    "kuliner": {
        "keywords": ["makan", "kuliner", "kopi", "coffee", "snack", "pedas", "sambal", "kue", "food", "cake", "boci", "seblak", "kenangan", "cookies"],
        "category": "Kuliner & Makanan",
        "products": [
            (1, "KL-SBL-001", "Sambal Bawang Cumi Mercon 150g Pedas Nampol", 32000, 100, "sambal,cumi,mercon,pedas,bawang"),
            (2, "KL-KPI-002", "Kopi Arabika Single Origin Gayo 250g Roasting Fresh", 65000, 75, "kopi,coffee,arabika,gayo,biji kopi"),
            (3, "KL-BCI-003", "Baso Aci Tulang Rangu Kuah Pedas Jeruk Limau", 18000, 150, "baso aci,boci,tulang rangu,instan"),
            (4, "KL-KRP-004", "Keripik Kaca Daun Jeruk Extra Pedas 250g", 15000, 200, "keripik kaca,pedas,daun jeruk,snack"),
            (5, "KL-BRN-005", "Brownies Fudgy Panggang Choco Melted Shiny", 45000, 60, "brownies,fudgy,coklat,cake,kue"),
            (6, "KL-SBL-006", "Seblak Instan Komplit Topping Ceker Cuanki", 22000, 120, "seblak,instan,cuanki,ceker,pedas"),
            (7, "KL-DDG-007", "Dendeng Sapi Balado Renyah Kering 100g", 58000, 40, "dendeng sapi,balado,lauk,renyah"),
            (8, "KL-MDU-008", "Madu Murni Hutan Asli Raw Honey 500ml", 85000, 50, "madu,honey,madu murni,hutan"),
            (9, "KL-CKS-009", "Cookies Red Velvet Cream Cheese Soft Baked", 49000, 70, "cookies,red velvet,cream cheese"),
            (10, "KL-MTC-010", "Minuman Bubuk Matcha Latte Premium 10 Sachet", 35000, 90, "matcha,latte,minuman bubuk,greentea")
        ],
        "comments": [
            "Kak spill sambal cumi mercon no 1 dong pedes banget gak mau checkout!",
            "Kopi Gayo no 2 bisa minta digilingin halus gak min?",
            "Baso aci no 3 tahan berapa hari di suhu ruang kak?",
            "Keripik kaca no 4 bumbu daun jeruknya gurih gak min mau beli 3",
            "Brownies fudgy no 5 dapet topping apa aja kak?",
            "Udah checkout sambal cumi no 1 ya min tolong kirim hari ini!",
            "Bisa COD ke Jogja gak kak?",
            "Seblak no 6 kuahnya kental pedas gurih kan min?",
            "Madu no 8 asli 100% tanpa campuran kan kak?",
            "Spill etalase 1 min mau lihat potongan cuminya"
        ]
    },
    "fashion": {
        "keywords": ["distro", "kaos", "baju", "erigo", "streetwear", "clothing", "apparel", "hoodie", "jaket", "kemeja", "celana"],
        "category": "Fashion & Distro",
        "products": [
            (1, "FS-FLN-001", "Kemeja Flanel Oversize Pria Kotak Hitam", 89000, 45, "flanel,kemeja,kotak,hitam,oversize"),
            (2, "FS-PLN-002", "Kemeja Putih Polos Slimfit Katun Oxford", 75000, 60, "putih,polos,formal,kerja,oxford"),
            (3, "FS-CHN-003", "Celana Chino Panjang Slimfit Pria Abu Gelap", 99000, 30, "chino,celana panjang,abu,pria"),
            (4, "FS-KOS-004", "Kaos Polos Heavyweight 24s Cotton Combed", 49000, 120, "kaos,tshirt,polos,cotton combed"),
            (5, "FS-CRG-005", "Celana Cargo Panjang Relaxed Fit Army Green", 115000, 40, "cargo,celana cargo,army,panjang"),
            (6, "FS-JKT-006", "Jaket Denim Jeans Bio Bleach Classic", 135000, 25, "jaket,denim,jeans,outer,bio bleach"),
            (7, "FS-HOD-007", "Hoodie Fleece Boxy Oversize 330gsm Hitam", 159000, 35, "hoodie,fleece,boxy,oversize,sweater"),
            (8, "FS-TAS-008", "Tas Selempang Kanvas Waterproof Unisex", 55000, 50, "tas,selempang,kanvas,slempang,bag"),
            (9, "FS-BOX-009", "Boxer Celana Kolor Santai Cotton Motif", 25000, 100, "boxer,kolor,celana santai,katun"),
            (10, "FS-TOP-010", "Topi Baseball Bordir New York Distro", 29000, 80, "topi,baseball,cap,distro,ny")
        ],
        "comments": [
            "Kak tolong spill kemeja flanel no 1 dong mau checkout!",
            "Kemeja flanel no 1 ukuran XL lingkar dada berapa min?",
            "Kaos no 4 bahannya 100% cotton combed 24s kan kak?",
            "Celana cargo no 5 muat untuk BB 70 kg gak min?",
            "Jaket denim no 6 bahannya tebal dan kaku gak kak?",
            "Spill hoodie no 7 min mau lihat bagian tudung kepalanya",
            "Udah checkout kemeja no 1 dan celana chino no 3 ya min!",
            "Bisa COD ke Bandung gak kak?",
            "Topi no 10 pengait belakangnya besi atau plastik?",
            "Semangat live nya kak bajunya keren-keren banget!"
        ]
    }
}

def detect_store_niche(streamer: str) -> str:
    """Mendeteksi niche toko secara cerdas dari username."""
    st = streamer.lower().replace("@", "").replace("_", "").replace(".", "")
    for niche_key, data in STORE_NICHES.items():
        for kw in data["keywords"]:
            if kw in st or kw in streamer.lower():
                return niche_key
    return "universal"

def clean_store_title(streamer: str) -> str:
    s = streamer.replace("@", "").replace("_", " ").replace(".", " ").strip()
    words = [w.capitalize() for w in s.split() if w]
    return " ".join(words) if words else "Toko Pilihan"

def get_store_catalog(streamer: str) -> pd.DataFrame:
    """Mengembalikan katalog 10 produk yang 100% sesuai dengan toko tersebut."""
    niche = detect_store_niche(streamer)
    
    if niche in STORE_NICHES:
        rows = STORE_NICHES[niche]["products"]
        data = []
        for r in rows:
            data.append({
                "etalase_no": r[0],
                "sku": r[1],
                "product_name": r[2],
                "category": STORE_NICHES[niche]["category"],
                "price_idr": r[3],
                "stock": r[4],
                "keywords": r[5]
            })
        return pd.DataFrame(data)
    else:
        # Universal Dynamic Catalog
        title = clean_store_title(streamer)
        items = [
            (1, f"{title} - Produk Terlaris No. 1 Best Seller", 89000, 50, "terlaris,bestseller,no1"),
            (2, f"{title} - Produk Unggulan Rekomendasi Host", 125000, 35, "unggulan,rekomendasi,spesial"),
            (3, f"{title} - Paket Hemat Spesial Live Shopping", 65000, 60, "paket hemat,promo,diskon"),
            (4, f"{title} - Edisi Terbaru New Arrival 2026", 99000, 40, "baru,new arrival,terbaru"),
            (5, f"{title} - Varian Favorit Pilihan Pelanggan", 49000, 80, "favorit,murah,terjangkau"),
            (6, f"{title} - Premium Exclusive Quality Edition", 175000, 20, "premium,exclusive,mewah"),
            (7, f"{title} - Aksesoris & Perlengkapan Tambahan", 35000, 100, "aksesoris,pelengkap"),
            (8, f"{title} - Edisi Terbatas Limited Special Drop", 145000, 25, "limited,terbatas,spesial"),
            (9, f"{title} - Paket Bundling Komplit Hemat", 110000, 45, "bundling,komplit,paket"),
            (10, f"{title} - Voucher Belanja & Merchandise Resmi", 25000, 150, "voucher,merchandise,resmi")
        ]
        data = []
        for it in items:
            data.append({
                "etalase_no": it[0],
                "sku": f"PRD-UNV-{it[0]:03d}",
                "product_name": it[1],
                "category": f"Katalog {title}",
                "price_idr": it[2],
                "stock": it[3],
                "keywords": it[4]
            })
        return pd.DataFrame(data)

def get_store_comments(streamer: str) -> list[str]:
    """Mengembalikan daftar komentar yang 100% selaras dengan niche toko."""
    niche = detect_store_niche(streamer)
    if niche in STORE_NICHES:
        return STORE_NICHES[niche]["comments"]
    else:
        title = clean_store_title(streamer)
        return [
            f"Kak tolong spill etalase nomor 1 dari {title} dong mau checkout sekarang!",
            f"Etalase 2 ready stok banyak gak min mau order 2 pcs?",
            f"Bisa COD ke luar kota gak kak {streamer}?",
            f"Udah checkout etalase nomor 1 ya min tolong segera diproses!",
            f"Spill etalase 3 dong min mau lihat detail paketnya",
            f"Pengiriman dari kota mana ya kak {title}?",
            f"Produk no 1 kualitasnya original dan terjamin kan min?",
            f"Ada garansi uang kembali gak kak kalo barang tidak sesuai?",
            f"Spill etalase nomor 4 min mau lihat tampak belakangnya",
            f"Semangat live nya kak {streamer} lancar terus usahanya!",
            f"Bisa kirim sameday / instan hari ini gak kak?",
            f"Udah co etalase nomor 2 ya min jangan lupa bonusnya ya!"
        ]
