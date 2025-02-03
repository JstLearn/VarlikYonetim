"""
Forex veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
from database import Database
from config import COLLECTION_CONFIG

class ForexCollector:
    def __init__(self):
        self.db = Database()
        self.baslangic_tarihi = datetime.strptime(COLLECTION_CONFIG['start_date'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        
    def log(self, message):
        """Zaman damgalı log mesajı yazdırır"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")
        
    def get_active_pairs(self):
        """Aktif Forex paritelerini getirir"""
        try:
            conn = self.db.connect()
            if not conn:
                return []
                
            cursor = conn.cursor()
            cursor.execute("""
                SELECT parite, borsa, veriler_guncel, ulke 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] 
                WHERE borsa = 'FOREX' AND tip = 'SPOT' AND aktif = 1
            """)
            
            pairs = []
            for row in cursor.fetchall():
                pairs.append({
                    'symbol': row[0],
                    'exchange': row[1],
                    'ulke': row[3]
                })
            
            if pairs:
                self.log(f"Toplam {len(pairs)} Forex çifti işlenecek")
                
            return pairs
            
        except Exception as e:
            self.log(f"Hata: Forex pariteleri alınamadı - {str(e)}")
            return []
            
    def collect_data(self, symbol, start_date, end_date=None):
        """Forex verilerini toplar"""
        yf_error = None
        # Önce yfinance'den dene
        try:
            # Yahoo Finance formatına çevir (EUR/USD -> EURUSD=X)
            yf_symbol = symbol.replace('/', '') + "=X"
            
            # Tüm uyarıları bastır
            import warnings, sys, io
            warnings.filterwarnings('ignore')
            
            # yfinance hata mesajlarını yakala
            stderr = sys.stderr
            sys.stderr = io.StringIO()
            
            # Veriyi çek
            df = yf.download(
                tickers=yf_symbol,
                start=start_date,
                end=end_date or datetime.now(timezone.utc),
                interval='1d',
                progress=False
            )
            
            # Hata mesajını al
            error_output = sys.stderr.getvalue()
            sys.stderr = stderr
            
            if "1 Failed download" in error_output:
                yf_error = error_output.split(']:')[1].strip()
                df = None
            
            if df is not None and not df.empty:
                self._update_data_status(symbol, True)
                return df
                
        except Exception as e:
            if not yf_error:
                yf_error = str(e)
            
        # yfinance'den alınamadıysa investing.com'dan dene
        try:
            import investpy
            
            # Para birimlerini ayır ve büyük harfe çevir
            symbol_upper = symbol.upper()
            base, quote = symbol_upper.split('/')
            
            # Tarih formatını ayarla
            start_str = start_date.strftime('%d/%m/%Y')
            end_str = (end_date or datetime.now()).strftime('%d/%m/%Y')
            
            # Investing.com'dan veri çek
            df = investpy.get_currency_cross_historical_data(
                currency_cross=f'{base}/{quote}',
                from_date=start_str,
                to_date=end_str
            )
            
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Tarihi index yap
                df.index = pd.to_datetime(df.index)
                
                self._update_data_status(symbol, True)
                return df
                
        except Exception as e:
            inv_error = str(e)
            if "currency_cross" in inv_error.lower():
                inv_error = inv_error.replace(symbol.lower(), symbol_upper)
            self.log(f"yf: {yf_symbol}   inv:{symbol_upper} denendi -> Veri alınamadı (yfinance: {yf_error}, investing: {inv_error})")
            
        # Her iki kaynaktan da veri alınamadıysa
        self._update_data_status(symbol, False)
        return pd.DataFrame()
            
    def _update_data_status(self, symbol, has_data):
        """Parite için veri durumunu günceller"""
        try:
            conn = self.db.connect()
            if not conn:
                return
                
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE [VARLIK_YONETIM].[dbo].[pariteler]
                SET veri_var = ?
                WHERE parite = ?
            """, (1 if has_data else 0, symbol))
            
            conn.commit()
            
        except Exception as e:
            self.log(f"Hata: Veri durumu güncellenemedi ({symbol}) - {str(e)}")
            if conn:
                conn.rollback()
                
    def get_dolar_karsiligi(self, symbol, fiyat):
        """Paritenin dolar karşılığını hesaplar"""
        # Parite çiftini ayır (örn: EUR/USD -> ['EUR', 'USD'])
        base, quote = symbol.split('/')
        
        if quote == 'USD':  # Direkt USD karşılığı
            return fiyat
            
        try:
            conn = self.db.connect()
            if not conn:
                return None
                
            cursor = conn.cursor()
            
            # Önce QUOTE/USD formatında ara
            cursor.execute("""
                SELECT TOP 1 fiyat
                FROM [VARLIK_YONETIM].[dbo].[kurlar]
                WHERE parite = ? AND borsa = 'FOREX'
                ORDER BY tarih DESC
            """, (f"{quote}/USD",))
            
            row = cursor.fetchone()
            if row:
                quote_usd = float(row[0])
                return fiyat * quote_usd
                
            # Bulunamazsa USD/QUOTE formatında ara ve tersini al
            cursor.execute("""
                SELECT TOP 1 fiyat
                FROM [VARLIK_YONETIM].[dbo].[kurlar]
                WHERE parite = ? AND borsa = 'FOREX'
                ORDER BY tarih DESC
            """, (f"USD/{quote}",))
            
            row = cursor.fetchone()
            if row:
                quote_usd = float(row[0])
                return fiyat * (1 / quote_usd)
                
        except Exception as e:
            self.log(f"Dolar karşılığı hesaplama hatası ({symbol}): {str(e)}")
            
        return None

    def save_candles(self, symbol, df, ulke):
        """Mum verilerini veritabanına kaydeder"""
        if df.empty:
            return False
            
        try:
            conn = self.db.connect()
            if not conn:
                return False
                
            cursor = conn.cursor()
            kayit_sayisi = 0
            
            for tarih, row in df.iterrows():
                try:
                    fiyat = float(row['Close'])
                    dolar_karsiligi = self.get_dolar_karsiligi(symbol, fiyat)
                    
                    if dolar_karsiligi is None:
                        self.log(f"Dolar karşılığı hesaplanamadı: {symbol}")
                        continue
                        
                    cursor.execute("""
                        IF NOT EXISTS (
                            SELECT 1 FROM [VARLIK_YONETIM].[dbo].[kurlar] 
                            WHERE parite = ? AND [interval] = ? AND tarih = ?
                        )
                        INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar] (
                            parite, [interval], tarih, fiyat, dolar_karsiligi, borsa, tip, ulke
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, 
                    (symbol, '1d', tarih, 
                     symbol, '1d', tarih, fiyat, dolar_karsiligi, 'FOREX', 'SPOT', ulke))
                    
                    kayit_sayisi += 1
                    
                except Exception as e:
                    self.log(f"Kayıt hatası ({symbol}, {tarih}): {str(e)}")
                    continue
                    
            conn.commit()
            
            if kayit_sayisi > 0:
                self.log(f"{symbol} için {kayit_sayisi} yeni kayıt eklendi")
                
            return True
            
        except Exception as e:
            self.log(f"Veri kaydetme hatası ({symbol}): {str(e)}")
            return False
            
    def run(self):
        """Tüm Forex verilerini toplar"""
        self.log("="*50)
        self.log("FOREX VERİLERİ TOPLANIYOR")
        self.log("="*50)
        
        pairs = self.get_active_pairs()
        if not pairs:
            self.log("İşlenecek Forex verisi yok")
            return
            
        for pair in pairs:
            symbol = pair['symbol']
            ulke = pair['ulke']
            
            try:
                # Son kayıt tarihini kontrol et
                conn = self.db.connect()
                if not conn:
                    continue
                    
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT MAX(tarih) as son_tarih
                    FROM [VARLIK_YONETIM].[dbo].[kurlar]
                    WHERE parite = ?
                """, (symbol,))
                
                row = cursor.fetchone()
                son_tarih = row[0] if row and row[0] else None
                
                if son_tarih is None:
                    # Hiç veri yoksa başlangıç tarihinden itibaren al
                    veriler = self.collect_data(symbol, self.baslangic_tarihi)
                    if not veriler.empty:
                        self.save_candles(symbol, veriler, ulke)
                else:
                    # Son tarihten sonraki verileri al
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
                    else:
                        continue
                
            except Exception as e:
                self.log(f"İşlem hatası ({symbol}): {str(e)}")
                continue 