"""
Farklı veri kaynaklarından veri toplama işlemleri
"""

import yfinance as yf # type: ignore
from datetime import datetime, timezone
from config import CURRENCY_PAIRS

def collect_currency_data(pair_info):
    """Döviz ve kripto para verilerini toplar"""
    try:
        ticker = yf.Ticker(pair_info['symbol'])
        hist = ticker.history(period='1d', interval=pair_info['interval'])
        
        if not hist.empty:
            # Yahoo Finance'den gelen son fiyat
            last_price = float(hist['Close'].iloc[-1])
            
            # USD karşılığını hesapla
            if pair_info['symbol'].endswith('USD') or pair_info['symbol'] == 'EURUSD=X' or pair_info['symbol'] == 'GBPUSD=X':
                usd_value = last_price
            else:
                # TRY pariteler için USD karşılığını hesapla
                usd_try = yf.Ticker('USDTRY=X').history(period='1d')['Close'].iloc[-1]
                usd_value = last_price / usd_try
            
            # UTC+0 tarihini al
            utc_now = datetime.now(timezone.utc)
            
            data = (
                pair_info['parite'],
                pair_info['interval'],
                pair_info['tip'],
                pair_info['ulke'],
                last_price,  # Orijinal fiyat
                usd_value,   # USD karşılığı
                utc_now     # UTC+0 tarih
            )
            return data
        return None
    except Exception as e:
        print(f"Veri alınırken hata oluştu ({pair_info['parite']}): {str(e)}")
        return None

def collect_all_data():
    """Tüm veri kaynaklarından veri toplar"""
    all_data = []
    
    for pair_name, pair_info in CURRENCY_PAIRS.items():
        # pair_info sözlüğüne parite adını ve interval bilgisini ekle
        pair_info['parite'] = pair_name
        pair_info['interval'] = '1h'  # Saatlik veri
        pair_info['tip'] = pair_info['type']  # type -> tip
        pair_info['ulke'] = pair_info['country']  # country -> ulke
        
        data = collect_currency_data(pair_info)
        if data:
            all_data.append(data)
    
    return all_data 