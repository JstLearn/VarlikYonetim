"""
Veri çekme uygulaması için yapılandırma ayarları
"""

import os
from dotenv import load_dotenv
import pathlib

# .env dosyasının yolunu belirle ve yükle
env_path = pathlib.Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Veritabanı bağlantı ayarları
DB_CONFIG = {
    "server": os.getenv('DB_SERVER'),
    "database": os.getenv('DB_DATABASE'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "driver": "SQL Server Native Client 11.0"
}

# Para birimleri listesi - Tüm para birimlerinin USD karşılığı
CURRENCY_PAIRS = [
    'EUR/USD',  # Euro
    'GBP/USD',  # İngiliz Sterlini
    'TRY/USD',  # Türk Lirası
    'JPY/USD',  # Japon Yeni
    'CHF/USD',  # İsviçre Frangı
    'CAD/USD',  # Kanada Doları
    'AUD/USD',  # Avustralya Doları
    'NZD/USD',  # Yeni Zelanda Doları
    'CNY/USD',  # Çin Yuanı
    'INR/USD'   # Hindistan Rupisi
]

# Veri çekme sıklığı (saniye)
UPDATE_INTERVAL = 3600  # Her saat başı

# Veri toplama ayarları
COLLECTION_CONFIG = {
    "start_date": "2025-01-01 00:00:00",  # UTC+0
    "timeframe": "1d",  # Günlük mumlar
    "retry_delay": 60,  # Hata durumunda bekleme süresi (saniye)
    "rate_limit_delay": 1  # API rate limit için bekleme süresi (saniye)
} 