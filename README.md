# SentimentETL

## Deskripsi
**SentimentETL** adalah pipeline ETL (Extract, Transform, Load) untuk analisis sentimen komentar di platform media sosial seperti Reddit dan YouTube. Proyek ini dirancang untuk mengumpulkan, memproses, dan menyimpan data, sehingga memudahkan analisis sentimen terhadap opini publik.

## Fitur Utama
- **Data Extraction**: Mengambil data komentar dari Reddit dan YouTube menggunakan API.
- **Data Transformation**: Memproses data untuk menganalisis sentimen, struktur komentar, dan analisis sentimen setiap komentar.
- **Data Loading**: Menyimpan data terstruktur ke dalam data warehouse.

## Teknologi yang Digunakan
- **Bahasa Pemrograman**: Python
- **Framework ETL**: Apache Airflow
- **Containerization**: Docker
- **Data Warehouse**: Clickhouse
- **API dan Library Extract**:  
  - [AsyncPRAW (Python Reddit API Wrapper)](https://asyncpraw.readthedocs.io/en/latest/)  
  - [YouTube API](https://developers.google.com/youtube/v3)

## Cara Menggunakan
1. **Kloning Repositori**
   ```bash
   git clone https://github.com/Edo-Bagus/SentimentETL.git
   cd SentimentETL
   ```
2. Setup Environment
   - Pastikan Docker sudah terinstal
   - Jalan kan perintah berikut
   - Pastikan file .env lengkap untuk semua credential yang diperlukan
   ```bash
   docker compose up
   ```
4. Menjalankan Pipeline
   - Akses Apache Airflow melalui http://localhost:8080.
   - Aktifkan dan jalankan DAG yang diinginkan.

## Strukture Direktori
```graphqsl
SentimentETL/
│
├── dags/                   # Definisi DAG untuk Airflow
├── etl/                    # Script untuk ekstraksi dan transformasi data
├── data_warehouse_schema.txt  # Skema untuk data warehouse
├── Dockerfile              # Konfigurasi Docker
├── docker-compose.yml      # Konfigurasi Docker Compose
├── requirements.txt        # Daftar dependensi Python
├── README.md               # Dokumentasi proyek
└── .env                    # Variabel environment untuk credential

```

## Demo dan Analisis

<a href="https://invincible-carp-c2a.notion.site/Analisis-Sentimen-Media-Sosial-terhadap-Produk-iPhone-Studi-Kasus-pada-Platform-Reddit-dan-YouTube-1444177a0d22802fbc0cdc0c082612da?pvs=4">
    Analisis Sentimen Media Sosial terhadap Produk iPhone: Studi Kasus pada Platform Reddit dan YouTube
</a>
<br/>

<a href="https://colab.research.google.com/drive/1qwa2Y4e7nH0uvHhqClOI3a4yl01AYwl9#scrollTo=8O2AodlDW76T">
    Analisis Sentimen Media Sosial terhadap Produk iPhone (Google Colab)
</a>
<br/>
<a href="https://youtu.be/spcbPMmO_vg">
    Video Presentasi
</a>

