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
                WHERE borsa = 'FOREX' AND tip = 'SPOT' 
                AND aktif = 1 
                AND (veri_var = 1 OR veri_var IS NULL)
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
        
        # Bugünün tarihini al
        simdi = datetime.now()
        
        # Başlangıç tarihi bugünden büyükse, bugünden 1 gün önceyi kullan
        if start_date.date() >= simdi.date():
            start_date = simdi - timedelta(days=1)
        
        # Bitiş tarihi yoksa bugünü kullan
        if end_date is None:
            end_date = simdi
        # Bitiş tarihi başlangıç tarihinden küçükse bugünü kullan
        elif end_date.date() <= start_date.date():
            end_date = simdi
            
        # Önce yfinance'den dene
        try:
            # Yahoo Finance formatına çevir (EUR/USD -> EURUSD=X)
            yf_symbol = symbol.replace('/', '') + "=X"
            
            # Tarihleri string formatına çevir
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')
            
            # Tüm uyarıları bastır
            import warnings, sys, io
            warnings.filterwarnings('ignore')
            
            # yfinance hata mesajlarını yakala
            stderr = sys.stderr
            sys.stderr = io.StringIO()
            
            # Veriyi çek
            df = yf.download(
                tickers=yf_symbol,
                start=start_str,
                end=end_str,
                interval='1d',
                progress=False,
                auto_adjust=True,  # Otomatik düzeltme yap
                prepost=False,  # Pre/post market verilerini alma
                threads=False  # Tek thread kullan
            )
            
            # Hata mesajını al
            error_output = sys.stderr.getvalue()
            sys.stderr = stderr
            
            if "1 Failed download" in error_output:
                yf_error = error_output.split(']:')[1].strip() if ']:' in error_output else error_output
                df = None
            
            if df is not None and not df.empty:
                return df, True
                
        except Exception as e:
            if not yf_error:
                yf_error = str(e)
            
        # yfinance'den alınamadıysa investing.com'dan dene
        try:
            import investpy
            
            # Para birimlerini ayır ve büyük harfe çevir
            symbol_upper = symbol.upper()
            base, quote = symbol_upper.split('/')
            
            # Investing.com için tarihleri ayarla (dd/mm/yyyy formatında)
            # Başlangıç tarihi bugünden küçük olmalı
            if start_date.date() >= simdi.date():
                start_date = simdi - timedelta(days=1)
            
            # Bitiş tarihi başlangıç tarihinden büyük olmalı
            if end_date.date() <= start_date.date():
                end_date = start_date + timedelta(days=1)
                if end_date.date() > simdi.date():
                    end_date = simdi
            
            # Tarihleri string formatına çevir
            start_str = start_date.strftime('%d/%m/%Y')
            end_str = end_date.strftime('%d/%m/%Y')
            
            # Investing.com'dan veri çek
            df = investpy.get_currency_cross_historical_data(
                currency_cross=f'{base}/{quote}',
                from_date=start_str,
                to_date=end_str
            )
            
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Tarihi index yap
                df.index = pd.to_datetime(df.index)
                return df, True
                
        except Exception as e:
            inv_error = str(e)
            if "currency_cross" in inv_error.lower():
                inv_error = inv_error.replace(symbol.lower(), symbol_upper)
            self.log(f"yf: {yf_symbol}   inv:{symbol} denendi -> Veri alınamadı")
            self.log(f"yfinance hata mesajı: {yf_error}")
            self.log(f"investing hata mesajı: {inv_error}")
            
        # Her iki kaynaktan da veri alınamadıysa
        return pd.DataFrame(), False
            
    def _update_data_status(self, symbol, has_data):
        """Parite için veri durumunu günceller"""
        conn = None
        try:
            conn = self.db.connect()
            if not conn:
                self.log(f"{symbol} için veritabanı bağlantısı kurulamadı (veri_var güncellemesi)")
                return
                
            cursor = conn.cursor()
            
            # Her durumda güncelle
            yeni_durum = 1 if has_data else 0
            cursor.execute("""
                UPDATE [VARLIK_YONETIM].[dbo].[pariteler]
                SET veri_var = ?
                WHERE parite = ?
            """, (yeni_durum, symbol))
            
            # Her zaman commit yap
            conn.commit()
            self.log(f"{symbol} için veri_var = {yeni_durum} olarak güncellendi")
            
        except Exception as e:
            self.log(f"Veri durumu güncellenemedi ({symbol}) - Hata: {str(e)}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass
                
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
            
        self.log(f"Toplam {len(pairs)} Forex çifti işlenecek")
        
        for pair in pairs:
            symbol = pair['symbol']
            ulke = pair['ulke']
            conn = None
            
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
                
                # Bugünün tarihini al
                simdi = datetime.now()
                
                if son_tarih is None:
                    # Hiç veri yoksa başlangıç tarihinden itibaren al
                    baslangic = self.baslangic_tarihi
                    if baslangic.date() > simdi.date():
                        baslangic = simdi
                    veriler, has_data = self.collect_data(symbol, baslangic, simdi)
                    if has_data:
                        self.save_candles(symbol, veriler, ulke)
                else:
                    # Son tarihten sonraki verileri al
                    son_guncelleme = datetime.combine(son_tarih.date(), datetime.min.time())
                    
                    if son_guncelleme.date() < simdi.date():
                        baslangic = son_guncelleme + timedelta(days=1)
                        if baslangic.date() > simdi.date():
                            baslangic = simdi
                        veriler, has_data = self.collect_data(symbol, baslangic, simdi)
                        if has_data:
                            self.save_candles(symbol, veriler, ulke)
                    else:
                        continue
                
                # Her parite işlendikten sonra commit yap
                conn.commit()
                
            except Exception as e:
                self.log(f"İşlem hatası ({symbol}): {str(e)}")
                if conn:
                    try:
                        conn.rollback()
                    except:
                        pass
                continue
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass 