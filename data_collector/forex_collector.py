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
                print(f"Parite: {row[0]}, Borsa: {row[1]}, Güncel: {'Evet' if row[2] else 'Hayır'}")
                
            return pairs
            
        except Exception as e:
            print(f"Hata: Forex pariteleri alınamadı - {str(e)}")
            return []
            
    def collect_data(self, symbol, start_date, end_date=None):
        """Forex verilerini yfinance'den toplar"""
        try:
            # Sembol formatını düzelt (EUR/USD -> EURUSD=X)
            formatted_symbol = f"{symbol.replace('/', '')}=X"
            
            # yfinance'den veri çek
            ticker = yf.Ticker(formatted_symbol)
            df = ticker.history(
                start=start_date,
                end=end_date or datetime.now(timezone.utc),
                interval='1d'
            )
            
            if df.empty:
                print(f"{symbol} -> yfinance'de veri bulunamadı")
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
                return pd.DataFrame()
            
            if 'volume' not in df.columns:
                df['volume'] = 0
            
            if df[required_columns].isnull().any().any():
                return pd.DataFrame()
            
            # Veri durumunu güncelle
            self._update_data_status(symbol, True)
            
            return df
            
        except Exception as e:
            print(f"{symbol} -> yfinance hatası: {str(e)}")
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
            print(f"Hata: Veri durumu güncellenemedi ({symbol}) - {str(e)}")
            if conn:
                conn.rollback()
                
    def get_dolar_karsiligi(self, symbol, fiyat):
        """Paritenin dolar karşılığını hesaplar"""
        # Parite çiftini ayır (örn: EUR/USD -> ['EUR', 'USD'])
        base, quote = symbol.split('/')
        
        if quote == 'USD':
            return fiyat
            
        try:
            # Quote'un dolar kurunu bul
            conn = self.db.connect()
            if not conn:
                return None
                
            cursor = conn.cursor()
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
        except:
            pass
            
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
                    fiyat = float(row['close'])
                    dolar_karsiligi = self.get_dolar_karsiligi(symbol, fiyat)
                    
                    if dolar_karsiligi is None:
                        print(f"Dolar karşılığı hesaplanamadı: {symbol}")
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
                    print(f"Kayıt hatası ({symbol}, {tarih}): {str(e)}")
                    continue
                    
            conn.commit()
            
            if kayit_sayisi > 0:
                print(f"{symbol} için {kayit_sayisi} yeni kayıt eklendi")
                
            return True
            
        except Exception as e:
            print(f"Veri kaydetme hatası ({symbol}): {str(e)}")
            return False
            
    def run(self):
        """Tüm Forex verilerini toplar"""
        print("\n" + "="*50)
        print("FOREX VERİLERİ TOPLANIYOR")
        print("="*50)
        
        pairs = self.get_active_pairs()
        if not pairs:
            print("İşlenecek Forex verisi yok")
            return
            
        print(f"Toplam {len(pairs)} Forex çifti işlenecek")
        
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
                        print(f"{symbol} -> Güncel")
                
            except Exception as e:
                print(f"İşlem hatası ({symbol}): {str(e)}")
                continue 