"""
Farklı veri kaynaklarından veri toplama işlemleri
"""

import yfinance as yf
from datetime import datetime, timezone, timedelta
import pandas as pd
import time
import logging
from database import Database
from config import CURRENCY_PAIRS, COLLECTION_CONFIG

class DataCollector:
    def __init__(self):
        self.db = Database()
        self.baslangic_tarihi = datetime(2025, 1, 1, 0, 0, 0)  # UTC+0
        self.para_birimleri = CURRENCY_PAIRS
        
    def get_candles(self, para_birimi, baslangic, bitis=None):
        try:
            # Para birimi formatını yfinance formatına çevir
            yf_symbol = para_birimi.replace('/', '') + '=X'
            logging.info(f"yfinance'den veri çekiliyor: {yf_symbol}")
            
            # Ticker oluştur
            ticker = yf.Ticker(yf_symbol)
            
            # Veriyi çek (1 dakikalık)
            df = ticker.history(
                start=baslangic,
                end=bitis if bitis else datetime.utcnow(),
                interval='1m'
            )
            
            if df.empty:
                logging.warning(f"Veri bulunamadı: {para_birimi}")
                return pd.DataFrame()
            
            # DataFrame'i düzenle
            df = df.reset_index()
            df = df.rename(columns={
                'Datetime': 'timestamp',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })
            
            # Hacim verisi yoksa 0 olarak ayarla
            if 'volume' not in df.columns:
                df['volume'] = 0
                
            # Timestamp'i UTC'ye çevir
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
            
            logging.info(f"{len(df)} adet veri çekildi: {para_birimi}")
            return df
            
        except Exception as e:
            logging.error(f"Veri çekme hatası ({para_birimi}): {str(e)}")
            return pd.DataFrame()

    def collect_missing_data(self, para_birimi):
        # Son kaydedilen mumu kontrol et
        son_mum = self.db.get_last_candle(para_birimi)
        baslangic = self.baslangic_tarihi
        
        if son_mum:
            baslangic = son_mum + timedelta(minutes=1)
        
        simdi = datetime.utcnow()
        
        while baslangic < simdi:
            bitis = min(baslangic + timedelta(days=7), simdi)  # yfinance için 7 günlük veri limiti
            
            logging.info(f"Veri çekiliyor: {para_birimi} - {baslangic} -> {bitis}")
            
            veriler = self.get_candles(para_birimi, baslangic, bitis)
            if not veriler.empty:
                self.db.save_candles(para_birimi, veriler)
                self.db.log_operation(
                    para_birimi, 
                    baslangic, 
                    bitis, 
                    "Başarılı", 
                    f"{len(veriler)} adet veri kaydedildi"
                )
            
            baslangic = bitis + timedelta(minutes=1)
            time.sleep(1)  # Rate limit için bekle

    def run_continuous(self):
        while True:
            for para_birimi in self.para_birimleri:
                try:
                    self.collect_missing_data(para_birimi)
                except Exception as e:
                    logging.error(f"Hata oluştu ({para_birimi}): {str(e)}")
                    self.db.log_operation(
                        para_birimi,
                        datetime.utcnow(),
                        datetime.utcnow(),
                        "Hata",
                        str(e)
                    )
            
            # 1 dakika bekle
            time.sleep(60) 

def collect_currency_data(parite):
    """
    yfinance'den kur verilerini çeker
    
    Args:
        parite (str): Para birimi çifti (örn: EUR/USD)
    """
    try:
        # yfinance formatına çevir (EUR/USD -> EURUSD=X)
        symbol = f"{parite.replace('/', '')}=X"
        logging.info(f"yfinance'den veri çekiliyor: {symbol}")
        
        # yfinance'den veriyi çek
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="1d")  # Son 1 günlük veri
        
        if not df.empty:
            # Kolon isimlerini düzenle
            df = df.rename(columns={
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })
            
            # Hacim verisi yoksa 0 olarak ayarla
            if 'volume' not in df.columns:
                df['volume'] = 0
                
            logging.info(f"{len(df)} adet veri çekildi: {parite}")
            return df
        else:
            logging.warning(f"Veri bulunamadı: {parite}")
            return None
            
    except Exception as e:
        logging.error(f"Veri çekme hatası ({parite}): {str(e)}")
        return None

def collect_all_data(db):
    """
    Tüm para birimleri için verileri toplar ve veritabanına kaydeder
    
    Args:
        db (Database): Veritabanı bağlantı nesnesi
    """
    for parite in CURRENCY_PAIRS:
        try:
            # Son veriyi kontrol et
            last_date = db.get_last_candle(parite)
            if last_date:
                start_date = last_date
            else:
                start_date = datetime.strptime(COLLECTION_CONFIG["start_date"], "%Y-%m-%d %H:%M:%S")
            
            end_date = datetime.now()
            
            logging.info(f"Veri çekiliyor: {parite} - {start_date} -> {end_date}")
            
            # Verileri çek
            df = collect_currency_data(parite)
            if df is not None:
                # Veritabanına kaydet
                db.save_candles(parite, df)
                
        except Exception as e:
            logging.error(f"Hata oluştu ({parite}): {str(e)}")
            continue 