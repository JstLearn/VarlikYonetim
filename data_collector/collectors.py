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
        # Config'den başlangıç tarihini al
        self.baslangic_tarihi = datetime.strptime(COLLECTION_CONFIG['start_date'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        self.para_birimleri = CURRENCY_PAIRS
        
    def get_candles(self, para_birimi, baslangic, bitis=None):
        try:
            # Para birimi formatını yfinance formatına çevir
            yf_symbol = self._get_yfinance_symbol(para_birimi)
            
            # Ticker oluştur
            ticker = yf.Ticker(yf_symbol)
            
            # Veriyi çek
            df = ticker.history(
                start=baslangic,
                end=bitis if bitis else datetime.now(timezone.utc),
                interval='1d'
            )
            
            if df.empty:
                return pd.DataFrame()
            
            # DataFrame'i düzenle
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
                
            return df
            
        except Exception as e:
            logging.error(f"Veri çekme hatası ({para_birimi}): {str(e)}")
            return pd.DataFrame()

    def _get_yfinance_symbol(self, para_birimi):
        """
        Para birimi çiftini yfinance sembolüne çevirir
        
        Args:
            para_birimi (str): Para birimi çifti (örn: EUR/USD)
            
        Returns:
            str: yfinance sembolü
        """
        base, quote = para_birimi.split('/')
        
        # USD bazlı çiftler için özel format
        if quote == 'USD':
            return f'USD{base}=X'  # USDJPY=X, USDCNY=X, USDINR=X, USDTRY=X gibi
        
        return f'{base}{quote}=X'

    def collect_missing_data(self, para_birimi):
        """
        Eksik verileri tespit edip tamamlar
        
        Args:
            para_birimi (str): Para birimi çifti (örn: EUR/USD)
        """
        conn = None
        try:
            conn = self.db.connect()
            if not conn:
                return
                
            cursor = conn.cursor()
            
            # Veritabanındaki ilk ve son tarihleri al
            cursor.execute("""
                SELECT MIN(CAST(tarih as date)) as ilk_tarih,
                       MAX(CAST(tarih as date)) as son_tarih
                FROM kurlar
                WHERE parite = ?
            """, (para_birimi,))
            
            row = cursor.fetchone()
            if not row:
                # Hiç veri yoksa başlangıç tarihinden itibaren al
                veriler = self.get_candles(para_birimi, self.baslangic_tarihi)
                if not veriler.empty:
                    self.db.save_candles(para_birimi, veriler)
                    logging.info(f"INSERT: {para_birimi} - {len(veriler)} kayıt (İlk yükleme)")
                return
                
            db_ilk_tarih = row[0]
            db_son_tarih = row[1]
            
            if not db_ilk_tarih or not db_son_tarih:
                return
            
            # Başlangıç tarihi ile ilk kayıt arasında eksik var mı kontrol et
            db_ilk_datetime = datetime.combine(db_ilk_tarih, datetime.min.time()).replace(tzinfo=timezone.utc)
            if db_ilk_datetime > self.baslangic_tarihi:
                veriler = self.get_candles(
                    para_birimi,
                    self.baslangic_tarihi,
                    db_ilk_datetime
                )
                if not veriler.empty:
                    self.db.save_candles(para_birimi, veriler)
                    logging.info(f"INSERT: {para_birimi} - {len(veriler)} kayıt (Başlangıç dönemi)")
            
            # Son kayıttan sonraki eksikleri kontrol et
            if db_son_tarih:
                simdi = datetime.now(timezone.utc)
                db_son_datetime = datetime.combine(db_son_tarih, datetime.min.time()).replace(tzinfo=timezone.utc)
                if db_son_datetime.date() < simdi.date():
                    veriler = self.get_candles(
                        para_birimi,
                        db_son_datetime + timedelta(days=1)
                    )
                    if not veriler.empty:
                        self.db.save_candles(para_birimi, veriler)
                        logging.info(f"INSERT: {para_birimi} - {len(veriler)} kayıt (Son güncelleme)")
            
        except Exception as e:
            logging.error(f"Hata: {str(e)}")
        finally:
            if conn:
                conn.close()

    def run_continuous(self):
        """Sürekli çalışan veri toplama döngüsü"""
        while True:
            try:
                for para_birimi in self.para_birimleri:
                    self.collect_missing_data(para_birimi)
                    time.sleep(0.1)
                    
                logging.info("Döngü başarıyla tamamlandı")
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"Hata: {str(e)}")
                time.sleep(1)

def collect_currency_data(parite, start_date, end_date):
    """
    yfinance'den kur verilerini çeker
    
    Args:
        parite (str): Para birimi çifti (örn: EUR/USD)
        start_date (datetime): Başlangıç tarihi
        end_date (datetime): Bitiş tarihi
    """
    try:
        # yfinance formatına çevir
        base, quote = parite.split('/')
        
        # USD bazlı çiftler için özel format
        if quote == 'USD':
            symbol = f'USD{base}=X'  # USDJPY=X, USDCNY=X, USDINR=X, USDTRY=X gibi
        else:
            symbol = f'{base}{quote}=X'
            
        # yfinance'den veriyi çek
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start_date, end=end_date, interval='1d')
        
        if not df.empty:
            # Kolon isimlerini düzenle
            df = df.rename(columns={
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })
            
            # USD bazlı çiftlerde değerleri ters çevir
            if quote == 'USD':
                for col in ['open', 'high', 'low', 'close']:
                    df[col] = 1 / df[col]
            
            # Hacim verisi yoksa 0 olarak ayarla
            if 'volume' not in df.columns:
                df['volume'] = 0
                
            return df
        else:
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
    # Config'den başlangıç tarihini al
    default_start = datetime.strptime(COLLECTION_CONFIG['start_date'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    end_date = datetime.now(timezone.utc)
    
    for parite in CURRENCY_PAIRS:
        try:
            # Son veriyi kontrol et
            last_date = db.get_last_candle(parite)
            total_records = 0
            
            if last_date:
                # Son veriden bir gün sonrasından başla
                start_date = (last_date + timedelta(days=1)).replace(tzinfo=timezone.utc)
                if start_date >= end_date:
                    continue
            else:
                start_date = default_start
            
            # Verileri haftalık periyotlarla çek (yfinance limiti için)
            current_start = start_date
            while current_start < end_date:
                current_end = min(current_start + timedelta(days=7), end_date)
                
                # Verileri çek
                df = collect_currency_data(parite, current_start, current_end)
                if df is not None and not df.empty:
                    # Veritabanına kaydet
                    db.save_candles(parite, df)
                    total_records += len(df)
                
                # Bir sonraki hafta
                current_start = current_end + timedelta(days=1)
                
                # Rate limit için bekle
                time.sleep(1)
            
            # İşlem sonunda özet log
            if total_records > 0:
                logging.info(f"{parite} için {total_records} yeni kayıt eklendi ({start_date.date()} - {end_date.date()})")

                
        except Exception as e:
            logging.error(f"Hata: {str(e)}")
            continue 