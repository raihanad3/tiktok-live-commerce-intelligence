"""
TikPulse - Store Catalog & Niche Detection Engine
Menyediakan katalog dinamis, katalog resmi brand (Aerostreet, Somethinc, Raysportindo, Erigo),
serta persistensi penyesuaian katalog per toko oleh pengguna.
"""

import os
import re
import json
import pandas as pd

CUSTOM_CATALOGS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "catalogs"))
os.makedirs(CUSTOM_CATALOGS_DIR, exist_ok=True)

BRAND_CATALOGS = {
    "aerostreet": {
        "category": "Aerostreet Official Footwear",
        "products": [
            (1, "AERO-HPS-001", "Aerostreet Hoops Low White Black Sneakers", 149000, 50, "hoops,sneakers,sepatu,hitam,putih"),
            (2, "AERO-MSS-002", "Aerostreet Massive Low All White Gum Sole", 139000, 45, "massive,all white,gum,kanvas"),
            (3, "AERO-OSK-003", "Aerostreet Osaka Low Retro Vintage Series", 129000, 60, "osaka,vintage,retro,sepatu"),
            (4, "AERO-TKY-004", "Aerostreet 2D Tokyo White Grey Limited", 149000, 40, "2d,tokyo,white grey,sneaker"),
            (5, "AERO-SLD-005", "Aerostreet Sandal Slide Cloud Foam Black", 59000, 120, "sandal,slide,cloud foam,empuk"),
            (6, "AERO-TSH-006", "Aerostreet T-Shirt Graphic Kaos Distro Katun 24s", 69000, 100, "kaos,tshirt,baju,distro,katun"),
            (7, "AERO-RUN-007", "Aerostreet Running Falcon Breathable Mesh", 169000, 35, "running,falcon,lari,olahraga"),
            (8, "AERO-SLP-008", "Aerostreet Slip On Comfy Canvas Black White", 119000, 55, "slip on,slipon,kanvas,praktis"),
            (9, "AERO-SOX-009", "Aerostreet Kaos Kaki Ankle 3 Pasang Premium", 25000, 150, "kaos kaki,socks,ankle"),
            (10, "AERO-CLN-010", "Aerostreet Cleaner Kit Foam Pembersih Sepatu", 45000, 80, "cleaner,pembersih,kit,foam")
        ]
    },
    "somethinc": {
        "category": "Somethinc Beauty & Skincare",
        "products": [
            (1, "SMT-CALM-001", "Somethinc Calm Down! Skinpair Barrier Moisturizer", 119000, 80, "calm down,moisturizer,pelembab,skin barrier"),
            (2, "SMT-CRMC-002", "Somethinc Ceramic Skin Saviour Moisturizer Gel", 139000, 75, "ceramic,skin saviour,moisturizer,gel"),
            (3, "SMT-SABI-003", "Somethinc 5% Niacinamide + Sabi Beet Brightening Serum", 115000, 90, "sabi beet,niacinamide,brightening,mencerahkan"),
            (4, "SMT-SUN-004", "Somethinc Holyshield! UV Watery Sunscreen Gel SPF 50+", 99000, 110, "sunscreen,holyshield,tabir surya,spf 50"),
            (5, "SMT-CPY-005", "Somethinc Copy Paste Breathable Mesh Cushion SPF 35", 169000, 60, "copy paste,cushion,mesh,foundation"),
            (6, "SMT-MAT-006", "Somethinc Checkmatte Transferproof Lipstick Matte", 89000, 130, "checkmatte,lipstick,lip tint,matte"),
            (7, "SMT-DIAM-007", "Somethinc Diamond Phyto Stem Cell Serum 20ml", 149000, 45, "diamond,stem cell,anti aging"),
            (8, "SMT-EYE-008", "Somethinc Game Changer Tripeptide Eye Concentrate", 79000, 70, "game changer,eye cream,mata panda"),
            (9, "SMT-CLN-009", "Somethinc Low pH Gentle & Mild Facial Cleanser", 75000, 95, "facial wash,cleanser,sabun cuci muka"),
            (10, "SMT-AHA-010", "Somethinc AHA BHA PHA Peeling Solution 20ml", 98000, 65, "peeling solution,peeling,aha bha,eksfoliasi,toner"),
            (11, "SMT-RES-011", "Somethinc Level 1% Encapsulated Retinol Serum", 145000, 50, "retinol,encapsulated,anti aging"),
            (12, "SMT-TON-012", "Somethinc Glow Maker Daily Clarifying Treatment Toner", 109000, 60, "glow maker,toner,clarifying"),
            (13, "SMT-POW-013", "Somethinc Under Control HD Blur Loose Powder", 105000, 70, "loose powder,bedak tabur,blur"),
            (14, "SMT-MIST-014", "Somethinc SRSLY Balancer Make Up Setting Spray", 79000, 85, "setting spray,makeup spray,balancer"),
            (15, "SMT-LIP-015", "Somethinc Ceraplump Tinted Lip Glow Balm", 89000, 100, "ceraplump,lip glow,lip balm,bibir,tinted"),
            (16, "SMT-BROW-016", "Somethinc Brow Wiz Precision Eyebrow Pencil", 55000, 120, "brow wiz,pensil alis,eyebrow"),
            (17, "SMT-ACNE-017", "Somethinc Acne Shot SOS Spot Gel Jerawat", 75000, 80, "acne shot,obat jerawat,spot gel"),
            (18, "SMT-OIL-018", "Somethinc Bakuchiol Skinpair Oil Serum 20ml", 99000, 65, "bakuchiol,oil serum,bumil"),
            (19, "SMT-BALM-019", "Somethinc Omega Butter Deep Cleansing Balm", 139000, 55, "cleansing balm,omega butter,makeup remover"),
            (20, "SMT-BODY-020", "Somethinc Skin Goals Brightening Body Crème", 85000, 90, "body creme,hand body,skin goals"),
            (25, "SMT-CVR-025", "Somethinc Coverblurr Breathable Cushion Refill", 129000, 50, "coverblurr,cushion refill,refil,cushion")
        ]
    },
    "ray": {
        "category": "Raysportindo Badminton Specialist",
        "products": [
            (1, "RAY-ARC-001", "Yonex Arcsaber 11 Pro Carbon 4U Badminton Racket", 520000, 25, "arcsaber,yonex,raket,badminton,carbon"),
            (2, "RAY-HLB-002", "Lining Halbertec 8000 Professional Series Racket", 480000, 20, "lining,halbertec,raket,smash"),
            (3, "RAY-SHL-003", "Shuttlecock JP Gold Tournament Grade 1 Slop Isi 12", 125000, 90, "shuttlecock,jp gold,kok,bulutangkis"),
            (4, "RAY-SPU-004", "Sepatu Badminton Yonex Power Cushion Non-Marking", 450000, 30, "sepatu,yonex,power cushion,grip"),
            (5, "RAY-JRS-005", "Jersey Badminton Victor Dryfit Breathable Tournament", 95000, 85, "jersey,baju badminton,victor,dryfit"),
            (6, "RAY-TAS-006", "Tas Raket Badminton Thermo Double Compartment", 230000, 25, "tas raket,thermo,badminton bag"),
            (7, "RAY-SNR-007", "Senar Badminton Yonex BG 66 Ultimax Original", 105000, 70, "senar,bg66,ultimax,tarikan"),
            (8, "RAY-GRP-008", "Grip Karet Raket Yonex AC102EX Tacky Feel Isi 3", 35000, 160, "grip,karet,yonex,ac102"),
            (9, "RAY-KNE-009", "Knee Pad Support Deker Lutut Olahraga Badminton", 55000, 60, "knee pad,deker,lutut,pelindung"),
            (10, "RAY-SOX-010", "Kaos Kaki Badminton Tebal Cushioned Anti Slip", 35000, 110, "kaos kaki,cushion,anti slip")
        ]
    },
    "erigo": {
        "category": "Erigo Official Streetwear",
        "products": [
            (1, "ERG-TSH-001", "Erigo T-Shirt Basic Cotton Combed Oversized", 75000, 120, "kaos,tshirt,basic,erigo,katun"),
            (2, "ERG-COA-002", "Erigo Coach Jacket Windproof Iconic Graphic", 169000, 45, "coach jacket,jaket coach,erigo,windproof"),
            (3, "ERG-CHN-003", "Erigo Chino Pants Slim Fit Stretch Cotton", 145000, 50, "chino,celana panjang,chino pants"),
            (4, "ERG-HOD-004", "Erigo Pullover Hoodie Fleece Heavyweight", 185000, 40, "hoodie,fleece,sweater,jaket"),
            (5, "ERG-CRG-005", "Erigo Cargo Pants Tactical 6 Pockets", 165000, 35, "cargo,celana cargo,tactical"),
            (6, "ERG-SHT-006", "Erigo Short Pants Chino Casual Santai", 89000, 70, "celana pendek,short pants,santai"),
            (7, "ERG-FLN-007", "Erigo Kemeja Flanel Tartan Long Sleeve", 135000, 55, "flanel,kemeja,tartan,kotak kotak"),
            (8, "ERG-HAT-008", "Erigo Baseball Cap Embroidery Logo", 55000, 80, "topi,baseball cap,snapback"),
            (9, "ERG-BAG-009", "Erigo Waist Bag Crossbody Cordura Water Resistant", 95000, 60, "waist bag,tas selempang,cordura"),
            (10, "ERG-SKJ-010", "Erigo Sukajan Reversible Embroidered Jacket", 275000, 20, "sukajan,jaket sukajan,reversible")
        ]
    }
}

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
            "Spill grip raket no 5 kak mau co bareng raket no 1"
        ]
    },
    "shoes": {
        "keywords": ["sepatu", "shoe", "sneaker", "foot", "aerostreet", "ventela", "compass"],
        "category": "Footwear & Sepatu",
        "products": [
            (1, "SH-SNK-001", "Aerostreet Hoops Low White Black Sneakers", 149000, 50, "hoops,sneakers,sepatu,hitam,putih"),
            (2, "SH-RUN-002", "Aerostreet Massive Low All White Gum Sole", 139000, 40, "massive,all white,gum,kanvas"),
            (3, "SH-SLP-003", "Aerostreet Osaka Low Retro Vintage Series", 129000, 60, "osaka,vintage,retro,sepatu"),
            (4, "SH-SND-004", "Aerostreet Sandal Slide Cloud Foam Black", 59000, 100, "sandal,slide,eva foam,empuk"),
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
            "Sandal slide no 4 bahan eva foam nya empuk gak kak?"
        ]
    },
    "skincare": {
        "keywords": ["skin", "beauty", "glow", "cosmetic", "makeup", "serum", "somethinc", "skintific", "originote", "scarlett", "wardah"],
        "category": "Skincare & Beauty",
        "products": [
            (1, "SK-SRM-001", "Somethinc Calm Down! Skinpair Barrier Moisturizer", 119000, 90, "calm down,pelembab,skin barrier"),
            (2, "SK-SUN-002", "Somethinc Ceramic Skin Saviour Moisturizer Gel", 139000, 120, "ceramic,skin saviour,gel"),
            (3, "SK-MST-003", "Somethinc 5% Niacinamide + Sabi Beet Serum", 115000, 75, "serum,niacinamide,sabi beet"),
            (4, "SK-CLN-004", "Somethinc Holyshield! UV Watery Sunscreen Gel SPF 50+", 99000, 110, "sunscreen,holyshield,tabir surya"),
            (5, "SK-CSH-005", "Somethinc Copy Paste Breathable Mesh Cushion SPF 35", 169000, 50, "copy paste,cushion,foundation"),
            (6, "SK-LIP-006", "Somethinc Checkmatte Transferproof Lipstick Matte", 89000, 150, "checkmatte,lipstick,matte"),
            (7, "SK-TNR-007", "Exfoliating AHA BHA PHA Peeling Toner", 98000, 60, "toner,exfoliating,aha bha,peeling"),
            (8, "SK-RET-008", "Retinol Rejuvenating Night Cream 20g", 135000, 40, "retinol,night cream,anti aging"),
            (9, "SK-MSK-009", "Sheet Mask Hydrating Hyaluronic Isi 5 Pcs", 40000, 100, "sheet mask,masker wajah,hydrating"),
            (10, "SK-EYE-010", "Eye Cream Peptide Dark Circle Remover", 75000, 70, "eye cream,krim mata,mata panda"),
            (25, "SK-CVR-025", "Somethinc Coverblurr Breathable Cushion Refill", 129000, 50, "coverblurr,cushion refill,refil")
        ],
        "comments": [
            "Kak tolong spill serum brightening no 1 dong mau checkout!",
            "Sunscreen no 2 ninggalin whitecast gak min di kulit sawo matang?",
            "Moisturizer no 3 aman buat kulit berjerawat dan fungal acne?",
            "Udah co cushion no 5 shade sand ya min tolong cepat dikirim!",
            "Lip tint no 6 transferproof gak kak tahan berapa jam?"
        ]
    },
    "hijab": {
        "keywords": ["hijab", "gamis", "butik", "dress", "muslim", "khimar", "abaya", "rabbani", "zoya", "wanita", "modest"],
        "category": "Fashion Muslimah & Hijab",
        "products": [
            (1, "HJB-PAS-001", "Pashmina Silk Premium Shiny Flowy 180x75", 49000, 150, "pashmina,silk,hijab,flowy,mengkilap"),
            (2, "HJB-GMS-002", "Gamis Abaya Crinkle Airflow Busui Friendly", 135000, 45, "gamis,abaya,crinkle,busui,dress"),
            (3, "HJB-VOL-003", "Jilbab Segiempat Voal Paris Premium Laser Cut", 35000, 200, "voal,paris,segiempat,jilbab,kerudung"),
            (4, "HJB-BRG-004", "Bergo Instan Maryam Jersey Non Pet Menutup Dada", 29000, 180, "bergo,maryam,instan,jersey"),
            (5, "HJB-CPT-005", "Ciput Ninja Anti Pusing Rayon Premium", 15000, 300, "ciput,ninja,inner,dalaman jilbab"),
            (6, "HJB-TNK-006", "Tunik Polos Katun Rayon Adem Nyaman", 89000, 60, "tunik,atasan wanita,rayon"),
            (7, "HJB-KHM-007", "Khimar Syari Ceruty Babydoll 2 Layer", 75000, 70, "khimar,syari,ceruty,babydoll"),
            (8, "HJB-BSR-008", "Bros Dagu Mutiara Hijab Elegan Isi 6", 12000, 250, "bros,mutiara,pin jilbab"),
            (9, "HJB-HNS-009", "Handsock Manset Tangan Cincin Rajut", 18000, 150, "manset,handsock,jempol,cincin"),
            (10, "HJB-MUK-010", "Mukena Parasut Korea Mini Travel Friendly", 95000, 80, "mukena,travel,mini,parasut")
        ],
        "comments": [
            "Pashmina silk no 1 gampang dibentuk gak kak licin ga?",
            "Gamis no 2 muat untuk LD 110 gak min mau checkout!",
            "Segiempat voal no 3 tegak di dahi gak kak?",
            "Spill bergo maryam nomor 4 dong kak",
            "Udah co pashmina no 1 warna mocca ya min!"
        ]
    },
    "gadgets": {
        "keywords": ["gadget", "phone", "case", "charger", "tws", "earphone", "audio", "kabel", "powerbank", "tech"],
        "category": "Gadget & Aksesoris Elektronik",
        "products": [
            (1, "GDG-CHG-001", "Fast Charger 65W GaN Dual Port Type C", 139000, 80, "charger,fast charging,gan,type c"),
            (2, "GDG-TWS-002", "TWS Bluetooth 5.3 ANC Low Latency Bass", 159000, 60, "tws,earphone,bluetooth,anc,headset"),
            (3, "GDG-KBL-003", "Kabel Data Type C to C 100W Braided 1M", 35000, 200, "kabel data,type c,100w,braided"),
            (4, "GDG-CSE-004", "Case Magnetic Wireless MagSafe Shockproof", 45000, 150, "case,casing,magsafe,shockproof"),
            (5, "GDG-PWR-005", "Powerbank 20.000 mAh 22.5W Fast Charging LED", 199000, 40, "powerbank,20000mah,fast charging"),
            (6, "GDG-TG-006", "Tempered Glass Privacy Anti Spy Full Cover", 25000, 250, "tempered glass,anti spy,privacy"),
            (7, "GDG-HLD-007", "Holder HP Meja Lipat Aluminium Portable", 39000, 100, "holder,stand hp,dudukan meja"),
            (8, "GDG-AUD-008", "Headset Gaming Stereo Bass with Mic 3.5mm", 79000, 70, "headset,gaming,earphone kabel"),
            (9, "GDG-STR-009", "Strap Gantungan HP Lanyard Braided Lucu", 15000, 300, "strap,lanyard,gantungan hp"),
            (10, "GDG-OTG-010", "Adapter USB OTG USB 3.0 to Type C Metal", 19000, 180, "otg,adapter,usb,type c")
        ],
        "comments": [
            "Charger no 1 bisa buat iPhone dan laptop gak min?",
            "TWS no 2 bass nya nendang gak kak buat main game ada delay ga?",
            "Powerbank no 5 aman dibawa masuk kabin pesawat gak min?",
            "Spill case magnetic no 4 buat tipe hp apa aja?",
            "Udah co kabel data no 3 warna hitam ya min!"
        ]
    },
    "culinary": {
        "keywords": ["kopi", "coffee", "snack", "makanan", "kuliner", "sambal", "keripik", "minuman", "frozen", "roti", "food"],
        "category": "Makanan & Minuman Kuliner",
        "products": [
            (1, "CUL-KPI-001", "Biji Kopi Arabika Gayo Specialty 250g", 75000, 100, "kopi,arabika,gayo,beans,biji kopi"),
            (2, "CUL-SMB-002", "Sambal Cumi Ciamik Pedas Nampol 150g", 35000, 180, "sambal,cumi,pedas,lauk,makanan"),
            (3, "CUL-KRP-003", "Keripik Kaca Pedas Daun Jeruk Gurih 200g", 18000, 250, "keripik,kaca,cemilan,snack,pedas"),
            (4, "CUL-FRZ-004", "Dimsum Ayam Udang Premium Frozen Isi 20", 55000, 80, "dimsum,frozen food,ayam,udang"),
            (5, "CUL-DRK-005", "Bubuk Minuman Matcha Latte Premium 500g", 69000, 90, "matcha,bubuk minuman,latte,greentea"),
            (6, "CUL-ROT-006", "Roti Toast Mentega Coklat Crunchy", 25000, 120, "roti,toast,coklat,mentega"),
            (7, "CUL-BAS-007", "Basreng Pedas Renyah Daun Jeruk 250g", 22000, 220, "basreng,bakso goreng,pedas"),
            (8, "CUL-MDN-008", "Madu Murni Randu Alami 500g Original", 89000, 60, "madu,murni,randu,sehat"),
            (9, "CUL-BMB-009", "Bumbu Nasi Goreng Spesial Instan Isi 5", 15000, 200, "bumbu,nasi goreng,instan"),
            (10, "CUL-CKL-010", "Coklat Truffle Melted Artisan Box 12 Pcs", 49000, 100, "coklat,truffle,melted,manis")
        ],
        "comments": [
            "Sambal cumi no 2 tahan berapa lama kak di suhu ruang?",
            "Biji kopi no 1 bisa digiling sekalian gak kak level gilingan V60?",
            "Dimsum no 4 halal dan free saus pedas manis gak min?",
            "Basreng no 7 bumbunya pedas nampol gak kak?",
            "Udah co kopi no 1 dan basreng no 7 ya min tolong packing aman!"
        ]
    },
    "fashion": {
        "keywords": ["baju", "distro", "fashion", "kaos", "tshirt", "flanel", "celana", "erigo", "hoodie", "jacket"],
        "category": "Distro & Streetwear Fashion",
        "products": [
            (1, "FSH-KAS-001", "Kemeja Flanel Katun Tartan Long Sleeve", 129000, 50, "kemeja,flanel,tartan,pria"),
            (2, "FSH-TKT-002", "Kaos Polos Cotton Combed 24s Heavyweight", 65000, 150, "kaos polos,cotton combed,tshirt"),
            (3, "FSH-CHN-003", "Celana Chino Panjang Pria Slimfit Stretch", 139000, 45, "chino,celana panjang,slimfit"),
            (4, "FSH-JKT-004", "Coach Jacket Windproof Graphic Streetwear", 179000, 35, "coach jacket,jaket,windproof"),
            (5, "FSH-CRG-005", "Celana Cargo Pendek Pria 6 Kantong", 99000, 60, "celana pendek,cargo,tactical"),
            (6, "FSH-HOD-006", "Hoodie Pullover Fleece Boxy Fit 330gsm", 189000, 40, "hoodie,sweater,fleece,oversize"),
            (7, "FSH-DNM-007", "Jaket Denim Jeans Washed Classic Indigo", 199000, 25, "jaket jeans,denim,indigo"),
            (8, "FSH-VES-008", "Vest Rompi Utility Tactical Street", 115000, 45, "rompi,vest,tactical,utility"),
            (9, "FSH-WST-009", "Tas Selempang Waistbag Waterproof Cordura", 59000, 80, "waistbag,tas selempang,cordura"),
            (10, "FSH-HAT-010", "Topi Baseball Polos Vintage Washed", 39000, 100, "topi,baseball cap,vintage")
        ],
        "comments": [
            "Kemeja flanel no 1 ukuran XL lingkar dada berapa min?",
            "Kaos no 2 bahannya 100% cotton combed 24s kan kak?",
            "Celana chino no 3 muat untuk BB 70 kg gak min?",
            "Spill hoodie no 6 min mau lihat bagian tudung kepalanya",
            "Udah checkout kemeja no 1 dan celana chino no 3 ya min!"
        ]
    }
}

NICHE_ALIASES = {
    "kuliner": "culinary",
    "gadget": "gadgets",
    "sport": "sports",
    "shoe": "shoes",
    "baju": "fashion",
    "beauty": "skincare"
}

def normalize_niche_key(key: str) -> str:
    if not key:
        return ""
    k = str(key).lower().strip()
    return NICHE_ALIASES.get(k, k)

def detect_store_niche(streamer: str, force_assign: bool = True) -> str:
    """
    Mendeteksi niche toko secara cerdas dari username.
    
    Args:
        streamer: Username TikTok (e.g. @tokobajumurah)
        force_assign: Jika True, akan random assign niche secara konsisten (hash-based) bila tidak ada keyword match
    """
    st = streamer.lower().replace("@", "").replace("_", "").replace(".", "")

    # Priority 1: Brand khusus yang sudah terdaftar
    for brand_key in BRAND_CATALOGS.keys():
        if brand_key in st or brand_key in streamer.lower():
            if brand_key == "somethinc":
                return "skincare"
            elif brand_key == "aerostreet":
                return "shoes"
            elif brand_key == "ray":
                return "sports"
            elif brand_key == "erigo":
                return "fashion"

    # Priority 2: Keyword matching dari username
    for niche_key, data in STORE_NICHES.items():
        for kw in data["keywords"]:
            if kw in st or kw in streamer.lower():
                return niche_key

    # Priority 3: Smart consistent hash-based assignment (Hybrid Niche Detection)
    if force_assign:
        import hashlib
        hash_obj = hashlib.md5(streamer.encode("utf-8"))
        hash_int = int(hash_obj.hexdigest(), 16)
        niche_keys = list(STORE_NICHES.keys())
        return niche_keys[hash_int % len(niche_keys)]

    return "universal"

def clean_store_title(streamer: str) -> str:
    s = streamer.replace("@", "").replace("_", " ").replace(".", " ").strip()
    words = [w.capitalize() for w in s.split() if w]
    return " ".join(words) if words else "Toko Pilihan"

def save_custom_store_catalog(streamer: str, products_list: list) -> bool:
    """Menyimpan katalog kustom toko ke penyimpanan JSON permanen."""
    clean = streamer.lower().replace("@", "").strip()
    filepath = os.path.join(CUSTOM_CATALOGS_DIR, f"{clean}_catalog.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(products_list, f, indent=2, ensure_ascii=False)
    return True

def get_store_catalog(streamer: str, niche_override: str = None) -> pd.DataFrame:
    """Mengembalikan katalog produk toko (Kustom Toko > Manual Niche Override > Brand Resmi > Niche Catalog > Universal)."""
    clean = streamer.lower().replace("@", "").strip()
    
    # 1. Cek apakah ada katalog kustom yang disimpan pengguna untuk toko ini
    custom_filepath = os.path.join(CUSTOM_CATALOGS_DIR, f"{clean}_catalog.json")
    if os.path.exists(custom_filepath):
        try:
            with open(custom_filepath, "r", encoding="utf-8") as f:
                items = json.load(f)
            data = []
            for it in items:
                data.append({
                    "etalase_no": int(it["etalase_no"]),
                    "sku": str(it.get("sku", f"PRD-{clean[:4].upper()}-{int(it['etalase_no']):03d}")),
                    "product_name": str(it["product_name"]),
                    "category": str(it.get("category", f"Katalog {clean_store_title(streamer)}")),
                    "price_idr": int(it["price_idr"]),
                    "stock": int(it.get("stock", 50)),
                    "keywords": str(it.get("keywords", str(it["product_name"]).lower()))
                })
            return pd.DataFrame(data)
        except Exception as e:
            print(f"[CUSTOM CATALOG ERROR] {e}")

    # 2. Manual Niche Override jika dipilih pengguna
    norm_override = normalize_niche_key(niche_override)
    if norm_override and norm_override in STORE_NICHES:
        rows = STORE_NICHES[norm_override]["products"]
        data = []
        for r in rows:
            data.append({
                "etalase_no": r[0],
                "sku": r[1],
                "product_name": r[2],
                "category": STORE_NICHES[norm_override]["category"],
                "price_idr": r[3],
                "stock": r[4],
                "keywords": r[5]
            })
        return pd.DataFrame(data)

    # 3. Cek apakah streamer cocok dengan brand spesifik resmi
    for brand_key, brand_data in BRAND_CATALOGS.items():
        if brand_key in clean:
            data = []
            for r in brand_data["products"]:
                data.append({
                    "etalase_no": r[0],
                    "sku": r[1],
                    "product_name": r[2],
                    "category": brand_data["category"],
                    "price_idr": r[3],
                    "stock": r[4],
                    "keywords": r[5]
                })
            return pd.DataFrame(data)

    # 4. Niche Fallback (Auto-detect / Smart hash)
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

    # 5. Universal Fallback
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

def get_store_comments(streamer: str, niche_override: str = None) -> list[str]:
    """Mengembalikan daftar komentar simulasi yang sesuai jika dibutuhkan."""
    norm_override = normalize_niche_key(niche_override)
    niche = norm_override if (norm_override and norm_override in STORE_NICHES) else detect_store_niche(streamer)
    if niche in STORE_NICHES:
        return STORE_NICHES[niche]["comments"]
    else:
        title = clean_store_title(streamer)
        return [
            f"Kak tolong spill etalase nomor 1 dari {title} dong mau checkout sekarang!",
            f"Etalase 2 ready stok banyak gak min mau order 2 pcs?",
            f"Bisa COD ke luar kota gak kak {streamer}?",
            f"Udah checkout etalase nomor 1 ya min tolong segera diproses!"
        ]
