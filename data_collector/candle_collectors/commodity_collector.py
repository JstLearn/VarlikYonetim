"""
Emtia veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
from utils.database import Database
from utils.config import COLLECTION_CONFIG

class CommodityCollector:
    def __init__(self):
        self.db = Database()
        self.baslangic_tarihi = datetime.strptime(COLLECTION_CONFIG['start_date'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        
    def log(self, message):
        """Zaman damgalı log mesajı yazdırır"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")
        
    def get_active_pairs(self):
        """Aktif emtia paritelerini getirir"""
        try:
            conn = self.db.connect()
            if not conn:
                return []
                
            cursor = conn.cursor()
            cursor.execute("""
                SELECT parite, borsa, veriler_guncel, ulke 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] 
                WHERE tip = 'COMMODITY' 
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
                self.log(f"Parite: {row[0]}, Borsa: {row[1]}, Güncel: {'Evet' if row[2] else 'Hayır'}")
                
            return pairs
            
        except Exception as e:
            self.log(f"Hata: Emtia pariteleri alınamadı - {str(e)}")
            return []
            
    def collect_data(self, symbol, start_date, end_date=None):
        """Emtia verilerini yfinance'den toplar"""
        try:
            # Tarihleri string formatına çevir
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = (end_date or datetime.now(timezone.utc)).strftime('%Y-%m-%d')
            
            # Tüm uyarıları bastır
            import warnings, sys, io
            warnings.filterwarnings('ignore')
            
            # yfinance hata mesajlarını yakala
            stderr = sys.stderr
            sys.stderr = io.StringIO()
            
            # yfinance'den veri çek
            df = yf.download(
                tickers=symbol,
                start=start_str,
                end=end_str,
                interval='1d',
                progress=False,
                auto_adjust=True,
                prepost=False,
                threads=False
            )
            
            # Hata mesajını al
            error_output = sys.stderr.getvalue()
            sys.stderr = stderr
            
            if "1 Failed download" in error_output:
                error_msg = error_output.split(']:')[1].strip() if ']:' in error_output else error_output
                self.log(f"yf: {symbol} denendi -> Veri alınamadı")
                self.log(f"yfinance hata mesajı: {error_msg}")
                self._update_data_status(symbol, False)
                return pd.DataFrame()
            
            if df is None or df.empty:
                self.log(f"yf: {symbol} denendi -> Veri alınamadı")
                self.log("yfinance hata mesajı: Veri bulunamadı")
                self._update_data_status(symbol, False)
                return pd.DataFrame()
            
            # DataFrame'i düzenle
            df = df.rename(columns={
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })
            
            # Gerekli kolonları kontrol et
            required_columns = ['open', 'high', 'low', 'close']
            if not all(col in df.columns for col in required_columns):
                self.log(f"yf: {symbol} denendi -> Veri alınamadı")
                self.log("yfinance hata mesajı: Gerekli kolonlar eksik")
                self._update_data_status(symbol, False)
                return pd.DataFrame()
            
            if 'volume' not in df.columns:
                df['volume'] = 0
            
            if df[required_columns].isnull().any().any():
                self.log(f"yf: {symbol} denendi -> Veri alınamadı")
                self.log("yfinance hata mesajı: Eksik değerler var")
                self._update_data_status(symbol, False)
                return pd.DataFrame()
            
            # Veri durumunu güncelle
            self._update_data_status(symbol, True)
            
            return df
            
        except Exception as e:
            self.log(f"yf: {symbol} denendi -> Veri alınamadı")
            self.log(f"yfinance hata mesajı: {str(e)}")
            self._update_data_status(symbol, False)
            return pd.DataFrame()
            
    def _update_data_status(self, symbol, has_data):
        """Emtia için veri durumunu günceller"""
        conn = None
        try:
            conn = self.db.connect()
            if not conn:
                self.log(f"{symbol} için veritabanı bağlantısı kurulamadı (veri_var güncellemesi)")
                return
                
            cursor = conn.cursor()
            
            # Önce mevcut durumu kontrol et
            cursor.execute("""
                SELECT veri_var 
                FROM [VARLIK_YONETIM].[dbo].[pariteler]
                WHERE parite = ?
            """, (symbol,))
            
            row = cursor.fetchone()
            if row:
                mevcut_durum = row[0]
                yeni_durum = 1 if has_data else 0
                
                if mevcut_durum != yeni_durum:
                    # Sadece değişiklik varsa güncelle
                    cursor.execute("""
                        UPDATE [VARLIK_YONETIM].[dbo].[pariteler]
                        SET veri_var = ?
                        WHERE parite = ?
                    """, (yeni_durum, symbol))
                    
                    # Hemen commit yap
                    conn.commit()
                    self.log(f"{symbol} için veri_var = {yeni_durum} olarak güncellendi (önceki değer: {mevcut_durum})")
            else:
                self.log(f"{symbol} emtiası veritabanında bulunamadı")
            
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
            
    def get_dolar_karsiligi(self, symbol, fiyat, ulke):
        """Emtianın dolar karşılığını hesaplar"""
        # Çoğu emtia USD bazında işlem görür
        # Bazı özel durumlar için kontrol eklenebilir
        return fiyat

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
                    fiyat = float(row['close'])
                    dolar_karsiligi = self.get_dolar_karsiligi(symbol, fiyat, ulke)
                    
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
                     symbol, '1d', tarih, fiyat, dolar_karsiligi, 'COMMODITY', 'COMMODITY', ulke))
                    
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