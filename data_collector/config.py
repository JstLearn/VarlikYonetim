"""
Veri toplama servisi konfigürasyon dosyası
"""

import os
from dotenv import load_dotenv
import pathlib
import logging

# .env dosyasının yolunu belirle ve yükle
env_path = pathlib.Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Veritabanı bağlantı ayarları
DB_CONFIG = {
    'driver': os.getenv('DB_DRIVER', '{ODBC Driver 17 for SQL Server}'),
    'server': os.getenv('DB_SERVER', 'localhost'),
    'database': os.getenv('DB_DATABASE', 'VARLIK_YONETIM'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'trusted_connection': os.getenv('DB_TRUSTED_CONNECTION', 'no')
}

# Konfigürasyon kontrolü
for key, value in DB_CONFIG.items():
    if not value:
        logging.error(f"Eksik veritabanı konfigürasyonu: {key}")

# Veri toplama ayarları
COLLECTION_CONFIG = {
    "start_date": "2025-1-1 00:00:00",  # UTC+0
    "timeframe": "1d",  # Günlük mumlar
    "retry_delay": 1,  # Hata durumunda bekleme süresi (saniye)
    "rate_limit_delay": 0.1  # API rate limit için bekleme süresi (saniye)
}

# Binance API ayarları
BINANCE_CONFIG = {
    'api_key': os.getenv('BINANCE_API_KEY', ''),
    'api_secret': os.getenv('BINANCE_API_SECRET', ''),
    'testnet': False,
    'timeout': 30
}

# Yahoo Finance ayarları
YAHOO_CONFIG = {
    'timeout': 30,
    'max_retries': 3,
    'retry_delay': 5
}

# Loglama ayarları
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': 'data_collector.log',
    'max_size': 10485760,  # 10MB
    'backup_count': 5
}

# Veri çekme sıklığı (saniye)
UPDATE_INTERVAL = 1  # Realtime güncelleme için 1 saniye 
