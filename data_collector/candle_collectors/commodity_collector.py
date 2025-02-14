"""
Emtia veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
import warnings
import logging
import time
from utils.database import Database
from utils.config import COLLECTION_CONFIG, YAHOO_CONFIG

# Yahoo Finance sembol eşleştirmeleri
YAHOO_SYMBOLS = {
    # Ana Emtia Futures
    'GOLD/USD': 'GC=F',           # Altın Futures
    'SILVER/USD': 'SI=F',         # Gümüş Futures
    'COPPER/USD': 'HG=F',         # Bakır Futures
    'PALLADIUM/USD': 'PA=F',      # Palladyum Futures
    'PLATINUM/USD': 'PL=F',       # Platin Futures
    'NATURAL_GAS/USD': 'NG=F',    # Doğal Gaz Futures
    'BRENT_OIL/USD': 'BZ=F',      # Brent Petrol Futures
    'CRUDE_OIL/USD': 'CL=F',      # Ham Petrol Futures
    'GASOLINE_RBOB/USD': 'RB=F',  # Benzin RBOB Futures
    'HEATING_OIL/USD': 'HO=F',    # Isıtma Yağı Futures
    'US_COFFEE_C/USD': 'KC=F',    # Kahve Futures
    'US_COCOA/USD': 'CC=F',       # Kakao Futures
    'US_SUGAR/USD': 'SB=F',       # Şeker Futures
    'US_COTTON/USD': 'CT=F',      # Pamuk Futures
    'US_CORN/USD': 'ZC=F',        # Mısır Futures
    'US_WHEAT/USD': 'ZW=F',       # Buğday Futures
    'US_SOYBEAN/USD': 'ZS=F',     # Soya Fasulyesi Futures
    'LIVE_CATTLE/USD': 'LE=F',    # Canlı Sığır Futures
    'LEAN_HOGS/USD': 'HE=F',      # Domuz Futures
    'ROUGH_RICE/USD': 'ZR=F',     # Pirinç Futures
    'LUMBER/USD': 'LBS=F',        # Kereste Futures
    
    # Metaller
    'ALUMINUM/USD': 'ALI=F',      # Alüminyum Futures
    'ZINC/USD': 'ZNC=F',          # Çinko Futures
    'LEAD/USD': 'LED=F',          # Kurşun Futures
    'NICKEL/USD': 'NIC=F',        # Nikel Futures
    'TIN/USD': 'TIN=F',           # Kalay Futures
    
    # ETF Alternatifleri
    'XETRA-GOLD/EUR': 'GLD',      # SPDR Gold Shares
    'LONDON_GOLD/GBP': 'IGLN.L',  # iShares Physical Gold
    
    # Alternatif Semboller
    'CRUDE_OIL_WTI/USD': 'CL=F',  # WTI Ham Petrol
    'US_SOYBEANS/USD': 'ZS=F',    # Soya Fasulyesi
}

class CommodityCollector:
    def __init__(self):
        """Sınıf başlatıcı"""
        self.db = Database()
        self.baslangic_tarihi = datetime.strptime(
            COLLECTION_CONFIG['start_date'], 
            '%Y-%m-%d %H:%M:%S'
        ).replace(tzinfo=timezone.utc)
        self.timeout = YAHOO_CONFIG['timeout']
        self.max_retries = YAHOO_CONFIG['max_retries']
        self.retry_delay = 2  # 2 saniye bekleme süresi
        self._collect_log_mesaj = []  # Log mesajlarını saklamak için
        
        # Logging ayarları
        logging.basicConfig(level=logging.WARNING)
        self.logger = logging.getLogger('yfinance')
        self.logger.setLevel(logging.ERROR)
        
    def get_yahoo_symbol(self, symbol):
        """Yahoo Finance sembolünü döndürür"""
        return YAHOO_SYMBOLS.get(symbol, symbol)
        
    def log(self, message):
        """Zaman damgalı log mesajı yazdırır"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")
        
    def get_active_pairs(self):
        """Aktif emtia paritelerini getirir"""
        try:
            query = """
                SELECT parite, borsa, veriler_guncel, ulke 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                WHERE tip = 'COMMODITY' 
                AND aktif = 1 
                AND (veri_var = 1 OR veri_var IS NULL)
            """
            
            results = self.db.fetch_all(query)
            if not results:
                return []
                
            pairs = []
            for row in results:
                pairs.append({
                    'symbol': row[0],
                    'exchange': row[1],
                    'ulke': row[3]
                })
                
            return pairs
            
        except Exception as e:
            self.log(f"Hata: Emtia pariteleri alınamadı - {str(e)}")
            return []
            
    def collect_data(self, symbol, start_date, end_date=None):
        """Emtia verilerini yfinance'den toplar"""
        try:
            # Yahoo Finance sembolünü al
            yahoo_symbol = self.get_yahoo_symbol(symbol)
            self._collect_log_mesaj = []  # Log mesajlarını sıfırla
            
            if yahoo_symbol != symbol:
                self._collect_log_mesaj.append(f"Sembol: {yahoo_symbol}")
            
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = (end_date or datetime.now(timezone.utc)).strftime('%Y-%m-%d')
            
            # Uyarıları bastır
            warnings.filterwarnings('ignore')
            self.logger.disabled = True
            
            df = pd.DataFrame()  # Boş DataFrame oluştur
            
            for attempt in range(self.max_retries):
                try:
                    # Her denemeden önce kısa bir bekleme
                    if attempt > 0:
                        bekleme_suresi = self.retry_delay * (2 if attempt == 1 else 4)  # 2 veya 4 saniye
                        time.sleep(bekleme_suresi)
                        self._collect_log_mesaj.append(f"Deneme {attempt + 1}")
                    
                    result = yf.download(
                        tickers=yahoo_symbol,
                        start=start_str,
                        end=end_str,
                        interval='1d',
                        progress=False,
                        auto_adjust=True,
                        prepost=False,
                        threads=False,
                        timeout=self.timeout * (attempt + 1)  # Her denemede timeout'u artır
                    )
                    
                    if result is None or result.empty:
                        if attempt < self.max_retries - 1:
                            continue
                        self._collect_log_mesaj.append("Veri alınamadı")
                        self._update_data_status(symbol, False)
                        return df
                        
                    df = result
                    break
                    
                except Exception as e:
                    if attempt < self.max_retries - 1:
                        continue
                    self._collect_log_mesaj.append(f"Hata: {str(e)}")
                    self._update_data_status(symbol, False)
                    return df
                finally:
                    # Her denemeden sonra kısa bir bekleme
                    time.sleep(0.2)  # 200ms bekleme
            
            # Logging'i geri aç
            self.logger.disabled = False
            
            if df.empty:
                self._collect_log_mesaj.append("Veri alınamadı")
                self._update_data_status(symbol, False)
                return df
            
            df = df.rename(columns={
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })
            
            required_columns = ['open', 'high', 'low', 'close']
            if not all(col in df.columns for col in required_columns):
                self._collect_log_mesaj.append("Gerekli kolonlar eksik")
                self._update_data_status(symbol, False)
                return pd.DataFrame()
            
            if 'volume' not in df.columns:
                df['volume'] = 0
            
            if df[required_columns].isnull().any().any():
                self._collect_log_mesaj.append("Eksik değerler var")
                self._update_data_status(symbol, False)
                return pd.DataFrame()
            
            self._update_data_status(symbol, True)
            return df
            
        except Exception as e:
            self.log(f"{symbol} -> Hata: {str(e)}")
            self._update_data_status(symbol, False)
            return pd.DataFrame()
            
    def _update_data_status(self, symbol, has_data):
        """Emtia için veri durumunu günceller"""
        try:
            query = """
                SELECT veri_var 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                WHERE parite = ?
            """
            
            row = self.db.fetch_one(query, (symbol,))
            if row:
                mevcut_durum = row[0]
                yeni_durum = 1 if has_data else 0
                
                if mevcut_durum != yeni_durum:
                    update_query = """
                        UPDATE p
                        SET p.veri_var = ?
                        FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                        WHERE p.parite = ?
                    """
                    
                    if self.db.execute_non_query(update_query, (yeni_durum, symbol)):
                        return yeni_durum
            else:
                self.log(f"{symbol} -> Veritabanında bulunamadı")
            
        except Exception as e:
            self.log(f"{symbol} -> Güncelleme hatası: {str(e)}")
            
    def get_dolar_karsiligi(self, symbol, fiyat, ulke):
        """Emtianın dolar karşılığını hesaplar"""
        return fiyat

    def save_candles(self, symbol, df, ulke):
        """Mum verilerini veritabanına kaydeder"""
        if df.empty:
            return False
            
        try:
            kayit_sayisi = 0
            
            for tarih, row in df.iterrows():
                try:
                    fiyat = float(row['close'])
                    dolar_karsiligi = self.get_dolar_karsiligi(symbol, fiyat, ulke)
                    
                    if dolar_karsiligi is None:
                        self._collect_log_mesaj.append("Dolar karşılığı hesaplanamadı")
                        continue
                        
                    insert_query = """
                        IF NOT EXISTS (
                            SELECT 1 FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                            WHERE parite = ? AND [interval] = ? AND tarih = ?
                        )
                        INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar] (
                            parite, [interval], tarih, fiyat, dolar_karsiligi, borsa, tip, ulke
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    
                    params = (
                        symbol, '1d', tarih,
                        symbol, '1d', tarih, fiyat, dolar_karsiligi, 'COMMODITY', 'COMMODITY', ulke
                    )
                    
                    if self.db.execute_non_query(insert_query, params):
                        kayit_sayisi += 1
                    
                except Exception as e:
                    self._collect_log_mesaj.append(f"Kayıt hatası ({tarih}): {str(e)}")
                    continue
                    
            if kayit_sayisi > 0:
                self._collect_log_mesaj.append(f"{kayit_sayisi} yeni kayıt")
                
            if self._collect_log_mesaj:
                self.log(f"{symbol} -> " + " | ".join(self._collect_log_mesaj))
                
            return True
            
        except Exception as e:
            self.log(f"{symbol} -> Veri kaydetme hatası: {str(e)}")
            return False
            
    def run(self):
        """Tüm emtia verilerini toplar"""
        self.log("="*50)
        
        pairs = self.get_active_pairs()
        if not pairs:
            self.log("İşlenecek emtia verisi yok")
            return
            
        self.log(f"Toplam {len(pairs)} emtia işlenecek")
        
        for pair in pairs:
            symbol = pair['symbol']
            ulke = pair['ulke']
            log_mesaj = []
            
            try:
                query = """
                    SELECT MAX(tarih) as son_tarih
                    FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                    WHERE parite = ?
                """
                
                row = self.db.fetch_one(query, (symbol,))
                son_tarih = row[0] if row and row[0] else None
                
                if son_tarih is None:
                    veriler = self.collect_data(symbol, self.baslangic_tarihi)
                    if not veriler.empty:
                        self.save_candles(symbol, veriler, ulke)
                else:
                    simdi = datetime.now(timezone.utc)
                    son_guncelleme = datetime.combine(son_tarih.date(), datetime.min.time()).replace(tzinfo=timezone.utc)
                    
                    if son_guncelleme.date() < simdi.date():
                        veriler = self.collect_data(
                            symbol,
                            son_guncelleme + timedelta(days=1),
                            simdi
                        )
                        if not veriler.empty:
                            self.save_candles(symbol, veriler, ulke)
                
            except Exception as e:
                self.log(f"{symbol} -> İşlem hatası: {str(e)}")
                continue

if __name__ == "__main__":
    collector = CommodityCollector()
    collector.run() 