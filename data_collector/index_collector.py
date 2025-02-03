"""
Endeks veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
from database import Database
from config import COLLECTION_CONFIG

class IndexCollector:
    def __init__(self):
        self.db = Database()
        self.baslangic_tarihi = datetime.strptime(COLLECTION_CONFIG['start_date'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        
    def log(self, message):
        """Zaman damgalı log mesajı yazdırır"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")
        
    def get_active_pairs(self):
        """Aktif endeks paritelerini getirir"""
        try:
            conn = self.db.connect()
            if not conn:
                return []
                
            cursor = conn.cursor()
            cursor.execute("""
                SELECT parite, borsa, veriler_guncel, ulke 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] 
                WHERE tip = 'INDEX' AND aktif = 1
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
            self.log(f"Hata: Endeks pariteleri alınamadı - {str(e)}")
            return []
            
    def collect_data(self, symbol, start_date, end_date=None):
        """Endeks verilerini yfinance'den toplar"""
        try:
            # yfinance'den veri çek
            ticker = yf.Ticker(symbol)
            df = ticker.history(
                start=start_date,
                end=end_date or datetime.now(timezone.utc),
                interval='1d'
            )
            
            if df.empty:
                yf_error = "Veri bulunamadı"
                self.log(f"yf: {symbol} denendi -> Veri alınamadı\nyfinance hata mesajı: {yf_error}")
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
                yf_error = "Gerekli kolonlar eksik"
                self.log(f"yf: {symbol} denendi -> Veri alınamadı\nyfinance hata mesajı: {yf_error}")
                self._update_data_status(symbol, False)
                return pd.DataFrame()
            
            if 'volume' not in df.columns:
                df['volume'] = 0
            
            if df[required_columns].isnull().any().any():
                yf_error = "Eksik değerler var"
                self.log(f"yf: {symbol} denendi -> Veri alınamadı\nyfinance hata mesajı: {yf_error}")
                self._update_data_status(symbol, False)
                return pd.DataFrame()
            
            # Veri durumunu güncelle
            self._update_data_status(symbol, True)
            
            return df
            
        except Exception as e:
            yf_error = str(e)
            self.log(f"yf: {symbol} denendi -> Veri alınamadı\nyfinance hata mesajı: {yf_error}")
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
                
    def get_dolar_karsiligi(self, symbol, fiyat, ulke):
        """Endeksin dolar karşılığını hesaplar"""
        if ulke == 'USA':  # Amerikan endeksleri zaten dolar bazında
            return fiyat
            
        try:
            # Ülke para biriminin dolar kurunu bul
            conn = self.db.connect()
            if not conn:
                return None
                
            cursor = conn.cursor()
            
            # Ülke para birimi kodunu belirle
            currency_map = {
                'Turkey': 'TRY',
                'Japan': 'JPY',
                'UK': 'GBP',
                'Europe': 'EUR',
                # Diğer ülkeler eklenebilir
            }
            
            currency = currency_map.get(ulke)
            if not currency:
                return None
                
            cursor.execute("""
                SELECT TOP 1 fiyat
                FROM [VARLIK_YONETIM].[dbo].[kurlar]
                WHERE parite = ? AND borsa = 'FOREX'
                ORDER BY tarih DESC
            """, (f"{currency}/USD",))
            
            row = cursor.fetchone()
            if row:
                currency_usd = float(row[0])
                return fiyat / currency_usd  # Endeks değerini dolara çevir
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
                     symbol, '1d', tarih, fiyat, dolar_karsiligi, 'INDEX', 'INDEX', ulke))
                    
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
        """Tüm endeks verilerini toplar"""
        self.log("="*50)
        self.log("ENDEKS VERİLERİ TOPLANIYOR")
        self.log("="*50)
        
        pairs = self.get_active_pairs()
        if not pairs:
            self.log("İşlenecek endeks verisi yok")
            return
            
        self.log(f"Toplam {len(pairs)} endeks işlenecek")
        
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