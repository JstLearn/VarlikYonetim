"""
Veri çekme uygulaması için yapılandırma ayarları
"""

import os
from dotenv import load_dotenv # type: ignore
import pathlib

# .env dosyasının yolunu belirle
env_path = pathlib.Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Veritabanı bağlantı bilgileri
DB_CONFIG = {
    'driver': 'ODBC Driver 17 for SQL Server',
    'server': os.getenv('DB_SERVER'),
    'database': os.getenv('DB_DATABASE'),
    'uid': os.getenv('DB_USER'),
    'pwd': os.getenv('DB_PASSWORD')
}

# Veri kaynakları için semboller ve bilgileri
CURRENCY_PAIRS = {
    'EUR/USD': {'symbol': 'EURUSD=X', 'type': 'FOREX', 'country': 'EU'},
    'GBP/USD': {'symbol': 'GBPUSD=X', 'type': 'FOREX', 'country': 'UK'},
    'USD/TRY': {'symbol': 'USDTRY=X', 'type': 'FOREX', 'country': 'TR'},
    'EUR/TRY': {'symbol': 'EURTRY=X', 'type': 'FOREX', 'country': 'TR'},
    'GBP/TRY': {'symbol': 'GBPTRY=X', 'type': 'FOREX', 'country': 'TR'},
    'BTC/USD': {'symbol': 'BTC-USD', 'type': 'KRIPTO', 'country': 'US'},
    'ETH/USD': {'symbol': 'ETH-USD', 'type': 'KRIPTO', 'country': 'US'},
    'XRP/USD': {'symbol': 'XRP-USD', 'type': 'KRIPTO', 'country': 'US'}
}

# Veri çekme sıklığı (saniye)
UPDATE_INTERVAL = 3600  # Her saat başı 