"""
Farklı veri kaynaklarından veri toplama işlemleri
"""

import yfinance as yf
from datetime import datetime, timezone, timedelta
import pandas as pd
import time
from database import Database
from config import CURRENCY_PAIRS, COLLECTION_CONFIG

# Yfinance TimedeltaIndex uyarısını düzelt
import pandas as _pd
def _monkey_patch_timedeltaindex(dst_error_hours):
    return _pd.to_timedelta(dst_error_hours, unit='h')
    
if hasattr(yf.utils, '_pd'):
    yf.utils._pd.TimedeltaIndex = _monkey_patch_timedeltaindex

class DataCollector:
    def __init__(self):
        # Global uyarı ayarları
        import warnings
        import pandas as pd
        warnings.filterwarnings('ignore')
        pd.options.mode.chained_assignment = None
        
        self.db = Database()
        # Config'den başlangıç tarihini al
        self.baslangic_tarihi = datetime.strptime(COLLECTION_CONFIG['start_date'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        self.para_birimleri = CURRENCY_PAIRS
        
    def get_candles(self, para_birimi, baslangic, bitis=None):
        max_retries = 3
        retry_delay = 2  # saniye
        
        for attempt in range(max_retries):
            try:
                # Tüm pandas uyarılarını engelle
                import warnings
                import pandas as pd
                pd.options.mode.chained_assignment = None
                warnings.simplefilter(action='ignore', category=FutureWarning)
                warnings.simplefilter(action='ignore', category=UserWarning)
                
                # Para birimi formatını yfinance formatına çevir
                yf_symbol = self._get_yfinance_symbol(para_birimi)
                print(f"Veri çekiliyor: {para_birimi} ({yf_symbol}) - Deneme {attempt + 1}/{max_retries}")
                
                # Ticker oluştur ve veriyi çek
                ticker = yf.Ticker(yf_symbol)
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    df = ticker.history(
                        start=baslangic,
                        end=bitis if bitis else datetime.now(timezone.utc),
                        interval='1d'
                    )
                
                # Boş DataFrame kontrolü
                if df is None or df.empty or len(df.index) == 0:
                    if attempt < max_retries - 1:
                        print(f"Veri bulunamadı, yeniden deneniyor ({para_birimi}, {baslangic} - {bitis})")
                        time.sleep(retry_delay)
                        continue
                    print(f"Veri bulunamadı ({para_birimi}, {baslangic} - {bitis})")
                    return pd.DataFrame()
                
                try:
                    # DataFrame'i düzenle
                    df = df.rename(columns={
                        'Open': 'open',
                        'High': 'high',
                        'Low': 'low',
                        'Close': 'close',
                        'Volume': 'volume'
                    })
                    
                    # Gerekli kolonların varlığını kontrol et
                    required_columns = ['open', 'high', 'low', 'close']
                    if not all(col in df.columns for col in required_columns):
                        print(f"Eksik kolonlar var: {para_birimi}")
                        return pd.DataFrame()
                    
                    # Hacim verisi yoksa 0 olarak ayarla
                    if 'volume' not in df.columns:
                        df['volume'] = 0
                    
                    # NaN değer kontrolü
                    if df[required_columns].isnull().any().any():
                        print(f"NaN değerler var: {para_birimi}")
                        return pd.DataFrame()
                    
                    # USD bazlı çiftlerde değerleri ters çevir
                    base, quote = para_birimi.split('/')
                    if quote == 'USD':
                        for col in required_columns:
                            if df[col].astype(float).min() > 0:  # Sıfıra bölme kontrolü
                                df[col] = 1 / df[col].astype(float)
                            else:
                                print(f"Geçersiz değerler var: {para_birimi}")
                                return pd.DataFrame()
                    
                    return df
                    
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"DataFrame düzenleme hatası, yeniden deneniyor ({para_birimi}): {str(e)}")
                        time.sleep(retry_delay)
                        continue
                    print(f"DataFrame düzenleme hatası ({para_birimi}): {str(e)}")
                    return pd.DataFrame()
                
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Veri çekme hatası, yeniden deneniyor ({para_birimi}): {str(e)}")
                    time.sleep(retry_delay)
                    continue
                print(f"Veri çekme hatası ({para_birimi}): {str(e)}")
                return pd.DataFrame()
        
        return pd.DataFrame()

    def _get_yfinance_symbol(self, para_birimi):
        """
        Para birimi çiftini yfinance sembolüne çevirir
        
        Args:
            para_birimi (str): Para birimi çifti (örn: EUR/USD)
            
        Returns:
            str: yfinance sembolü
        """
        try:
            base, quote = para_birimi.split('/')
            
            # Özel sembol eşleştirmeleri
            special_pairs = {
                'TRY/USD': 'USDTRY=X',
                'JPY/USD': 'USDJPY=X',
                'EUR/USD': 'EURUSD=X',
                'GBP/USD': 'GBPUSD=X',
                'CHF/USD': 'USDCHF=X',
                'CAD/USD': 'USDCAD=X',
                'AUD/USD': 'AUDUSD=X',
                'NZD/USD': 'NZDUSD=X',
                'INR/USD': 'USDINR=X'
            }
            
            if para_birimi in special_pairs:
                return special_pairs[para_birimi]
            
            # USD bazlı çiftler için özel format
            if quote == 'USD':
                return f'USD{base}=X'
            else:
                return f'{base}{quote}=X'
                
        except Exception as e:
            print(f"Sembol dönüştürme hatası ({para_birimi}): {str(e)}")
            return para_birimi.replace('/', '') + '=X'

    def collect_missing_data(self, para_birimi):
        """
        Eksik verileri tespit edip tamamlar
        
        Args:
            para_birimi (str): Para birimi çifti (örn: EUR/USD)
        """
        try:
            if not self.db._connection:
                self.db.connect()
                
            if not self.db._connection:
                print("Veritabanı bağlantısı kurulamadı")
                return
                
            # Veritabanındaki ilk ve son tarihleri al
            cursor = self.db._connection.cursor()
            
            cursor.execute("""
                SELECT MIN(CAST(tarih as date)) as ilk_tarih,
                       MAX(CAST(tarih as date)) as son_tarih
                FROM kurlar
                WHERE parite = ?
            """, (para_birimi,))
            
            row = cursor.fetchone()
            if row is None:
                # Hiç veri yoksa başlangıç tarihinden itibaren al
                veriler = self.get_candles(para_birimi, self.baslangic_tarihi)
                if not veriler.empty:
                    self.db.save_candles(para_birimi, veriler)
                    print(f"INSERT: {para_birimi} - {len(veriler)} kayıt (İlk yükleme)")
                return
            
            db_ilk_tarih, db_son_tarih = row
            
            if db_ilk_tarih is None or db_son_tarih is None:
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
                    print(f"INSERT: {para_birimi} - {len(veriler)} kayıt (Başlangıç dönemi)")
            
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
                        print(f"INSERT: {para_birimi} - {len(veriler)} kayıt (Son güncelleme)")
            
        except Exception as e:
            print(f"Veri toplama hatası ({para_birimi}): {str(e)}")

    def run_continuous(self):
        """Sürekli çalışan veri toplama döngüsü"""
        while True:
            try:
                for para_birimi in self.para_birimleri:
                    self.collect_missing_data(para_birimi)
                    time.sleep(0.1)
                    
                print("Döngü başarıyla tamamlandı")
                time.sleep(1)
                
            except Exception as e:
                print(f"Hata: {str(e)}")
                time.sleep(1)

def collect_currency_data(parite, start_date, end_date):
    """
    yfinance'den kur verilerini çeker
    
    Args:
        parite (str): Para birimi çifti (örn: EUR/USD)
        start_date (datetime): Başlangıç tarihi
        end_date (datetime): Bitiş tarihi
        
    Returns:
        pandas.DataFrame: Kur verileri
    """
    try:
        collector = DataCollector()
        return collector.get_candles(parite, start_date, end_date)
    except Exception as e:
        print(f"Veri toplama hatası: {str(e)}")
        return pd.DataFrame()

def collect_all_data(db):
    """
    Tüm para birimleri için veri toplar
    
    Args:
        db (Database): Veritabanı bağlantısı
    """
    try:
        collector = DataCollector()
        collector.run_continuous()
    except Exception as e:
        print(f"Veri toplama hatası: {str(e)}") 