"""
Binance Futures veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
from binance.client import Client
from utils.database import Database
from utils.config import COLLECTION_CONFIG

class BinanceFuturesCollector:
    def __init__(self):
        self.db = Database()
        self.client = Client(None, None)  # API key olmadan da çalışır
        self.baslangic_tarihi = datetime.strptime(COLLECTION_CONFIG['start_date'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        
    def log(self, message):
        """Zaman damgalı log mesajı yazdırır"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")
        
    def get_active_pairs(self):
        """Aktif Binance Futures paritelerini getirir"""
        try:
            conn = self.db.connect()
            if not conn:
                return []
                
            cursor = conn.cursor()
            cursor.execute("""
                SELECT parite, borsa, veriler_guncel 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                WHERE borsa = 'BINANCE' AND tip = 'FUTURES' AND aktif = 1
                AND (veri_var = 1 OR veri_var IS NULL)
            """)
            
            pairs = []
            for row in cursor.fetchall():
                pairs.append({
                    'symbol': row[0],
                    'exchange': row[1]
                })
            
            if pairs:
                self.log(f"Toplam {len(pairs)} Futures çifti işlenecek")
                
            return pairs
            
        except Exception as e:
            self.log(f"Hata: Futures pariteleri alınamadı - {str(e)}")
            return []
            
    def collect_data(self, symbol, start_date, end_date=None):
        """Binance Futures verilerini toplar"""
        try:
            # Sembol formatını düzelt (BTC/USDT -> BTCUSDT)
            formatted_symbol = symbol.replace('/', '')
            
            # Zaman damgalarını hazırla
            start_ts = int(start_date.timestamp() * 1000)
            end_ts = int((end_date or datetime.now(timezone.utc)).timestamp() * 1000)
            
            # Futures API'sini kullan
            self.client.futures_ping()  # Test bağlantısı
            klines = self.client.futures_historical_klines(
                formatted_symbol,
                Client.KLINE_INTERVAL_1DAY,
                start_ts,
                end_ts
            )
            
            if not klines:
                binance_error = "Veri bulunamadı"
                self.log(f"binance: {formatted_symbol} denendi -> Veri alınamadı\nbinance hata mesajı: {binance_error}")
                self._update_data_status(symbol, False)
                return pd.DataFrame()
            
            # DataFrame'e çevir
            df = pd.DataFrame(klines, columns=[
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_base',
                'taker_buy_quote', 'ignore'
            ])
            
            # Sadece gerekli kolonları al
            df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
            
            # Veri tiplerini düzelt
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Timestamp'i tarihe çevir
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            
            # Veri durumunu güncelle
            self._update_data_status(symbol, True)
            
            return df
            
        except Exception as e:
            binance_error = str(e)
            self.log(f"binance: {formatted_symbol} denendi -> Veri alınamadı\nbinance hata mesajı: {binance_error}")
            self._update_data_status(symbol, False)
            return pd.DataFrame()
            
    def _update_data_status(self, symbol, has_data):
        """Binance Futures için veri durumunu günceller"""
        try:
            conn = self.db.connect()
            if not conn:
                self.log(f"{symbol} için veritabanı bağlantısı kurulamadı (veri_var güncellemesi)")
                return
                
            cursor = conn.cursor()
            
            # Önce mevcut durumu kontrol et
            cursor.execute("""
                SELECT veri_var 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                WHERE parite = ?
            """, (symbol,))
            
            row = cursor.fetchone()
            if row:
                mevcut_durum = row[0]
                yeni_durum = 1 if has_data else 0
                
                if mevcut_durum != yeni_durum:
                    # Sadece değişiklik varsa güncelle
                    cursor.execute("""
                        UPDATE p
                        SET p.veri_var = ?, p.borsa = ?, p.kayit_tarihi = GETDATE()
                        FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                        WHERE p.parite = ?
                    """, (yeni_durum, 'BINANCE', symbol))
                    
                    conn.commit()
            else:
                self.log(f"{symbol} futures çifti veritabanında bulunamadı")
            
        except Exception as e:
            self.log(f"Veri durumu güncellenemedi ({symbol}) - Hata: {str(e)}")
            if conn:
                conn.rollback()
                
    def save_candles(self, symbol, df):
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
                    cursor.execute("""
                        IF NOT EXISTS (
                            SELECT 1 FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                            WHERE parite = ? AND [interval] = ? AND tarih = ?
                        )
                        INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar] (
                            parite, [interval], tarih, fiyat, dolar_karsiligi, borsa, tip, ulke
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, 
                    (
                        # WHERE koşulu için parametreler
                        symbol, '1d', tarih,
                        # INSERT için parametreler
                        symbol, '1d', tarih, float(row['close']), float(row['close']), 'BINANCE', 'FUTURES', 'Global'
                    ))
                    
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
        """Tüm Futures verilerini toplar"""
        self.log("="*50)
        
        pairs = self.get_active_pairs()
        if not pairs:
            self.log("İşlenecek Futures verisi yok")
            return
            
        for pair in pairs:
            symbol = pair['symbol']
            
            try:
                # Son kayıt tarihini kontrol et
                conn = self.db.connect()
                if not conn:
                    continue
                    
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT MAX(tarih) as son_tarih
                    FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                    WHERE parite = ?
                """, (symbol,))
                
                row = cursor.fetchone()
                son_tarih = row[0] if row and row[0] else None
                
                if son_tarih is None:
                    # Hiç veri yoksa başlangıç tarihinden itibaren al
                    # Ama bugünkü kapanmamış mumları hariç tut
                    simdi = datetime.now(timezone.utc)
                    bugun_baslangic = datetime.combine(simdi.date(), datetime.min.time()).replace(tzinfo=timezone.utc)
                    dun_sonu = bugun_baslangic - timedelta(seconds=1)
                    
                    veriler = self.collect_data(symbol, self.baslangic_tarihi, dun_sonu)
                    if not veriler.empty:
                        self.save_candles(symbol, veriler)
                else:
                    # Son tarihten sonraki verileri al
                    simdi = datetime.now(timezone.utc)
                    # Bugünün başlangıcını hesapla (UTC gece yarısı)
                    bugun_baslangic = datetime.combine(simdi.date(), datetime.min.time()).replace(tzinfo=timezone.utc)
                    # Dün gününün son anını al (bugünün başlangıcından 1 saniye önce)
                    dun_sonu = bugun_baslangic - timedelta(seconds=1)
                    # Dün gününü al
                    dun = dun_sonu.date()
                    son_guncelleme = datetime.combine(son_tarih.date(), datetime.min.time()).replace(tzinfo=timezone.utc)
                    
                    if son_guncelleme.date() < dun:
                        veriler = self.collect_data(
                            symbol,
                            son_guncelleme + timedelta(days=1),
                            dun_sonu
                        )
                        if not veriler.empty:
                            self.save_candles(symbol, veriler)
                    else:
                        continue
                
            except Exception as e:
                self.log(f"İşlem hatası ({symbol}): {str(e)}")
                continue 