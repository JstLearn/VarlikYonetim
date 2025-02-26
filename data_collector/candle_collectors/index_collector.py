"""
Endeks veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
from utils.database import Database
from utils.config import COLLECTION_CONFIG


class IndexCollector:
    def __init__(self):
        self.db = Database()
        self.baslangic_tarihi = datetime.strptime(COLLECTION_CONFIG['start_date'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        
    def log(self, message):
        """Zaman damgalı log mesajı yazdırır"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")
        
    def run(self):
        """Tüm endeks verilerini toplar"""
        self.log("="*50)
        
        # Veritabanı bağlantısını aç
        conn = self.db.connect()
        if not conn:
            self.log("Veritabanı bağlantısı kurulamadı")
            return
            
        cursor = conn.cursor()
        
        try:
            # Aktif pariteleri al
            pairs = self.get_active_pairs(conn, cursor)
            if not pairs:
                self.log("İşlenecek endeks verisi yok")
                return
                
            self.log(f"Toplam {len(pairs)} endeks işlenecek")
            
            for pair in pairs:
                symbol = pair['symbol']
                ulke = pair['ulke']
                
                # Her parite için bağlantının durumunu kontrol et
                if conn is None or cursor is None or conn.closed:
                    self.log(f"Bağlantı kesilmiş, yeniden bağlanılıyor...")
                    conn = self.db.connect()
                    if not conn:
                        self.log("Veritabanı bağlantısı kurulamadı, işlem sonlandırılıyor")
                        break
                    cursor = conn.cursor()
                
                try:
                    # Son kayıt tarihini kontrol et
                    cursor.execute("""
                        SELECT MAX(tarih) as son_tarih
                        FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                        WHERE parite = ?
                    """, (symbol,))
                    
                    row = cursor.fetchone()
                    son_tarih = row[0] if row and row[0] else None
                    
                    if son_tarih is None:
                        # Hiç veri yoksa başlangıç tarihinden itibaren al
                        self.log(f"{symbol} için hiç veri yok, başlangıçtan itibaren alınacak")
                        veriler = self.collect_data(symbol, self.baslangic_tarihi, None, conn, cursor)
                        if not veriler.empty:
                            self.save_candles(symbol, veriler, ulke, conn, cursor)
                    else:
                        # Son tarihten sonraki verileri al
                        simdi = datetime.now(timezone.utc)
                        son_guncelleme = datetime.combine(son_tarih.date(), datetime.min.time()).replace(tzinfo=timezone.utc)
                        
                        if son_guncelleme.date() < simdi.date():
                            self.log(f"{symbol} için son güncelleme: {son_guncelleme.date()}, güncel veriler alınacak")
                            veriler = self.collect_data(
                                symbol,
                                son_guncelleme + timedelta(days=1),
                                simdi,
                                conn,
                                cursor
                            )
                            if not veriler.empty:
                                self.save_candles(symbol, veriler, ulke, conn, cursor)
                        else:
                            self.log(f"{symbol} için veriler zaten güncel (Son: {son_guncelleme.date()})")
                            continue
                    
                    # İşlem başarılı olduysa, değişiklikleri kaydet
                    conn.commit()
                    
                except Exception as e:
                    self.log(f"İşlem hatası ({symbol}): {str(e)}")
                    # Hata durumunda rollback yap
                    try:
                        if not conn.closed:
                            conn.rollback()
                    except Exception as e:
                        self.log(f"Rollback hatası: {str(e)}")
                        # Bağlantı kapalıysa, yeniden açmayı dene
                        try:
                            if conn.closed:
                                conn = self.db.connect()
                                if conn:
                                    cursor = conn.cursor()
                        except:
                            pass
                        
            self.log("Endeks veri toplama tamamlandı.")
                
        except Exception as e:
            self.log(f"Genel hata: {str(e)}")
        finally:
            # Bağlantıyı kapat
            try:
                if cursor and not getattr(cursor, 'closed', False):
                    cursor.close()
                if conn and not conn.closed:
                    conn.close()
                self.log("Veritabanı bağlantısı kapatıldı")
            except Exception as e:
                self.log(f"Bağlantı kapatma hatası: {str(e)}")
            
    def get_active_pairs(self, conn=None, cursor=None):
        """Aktif endeks paritelerini getirir"""
        close_conn = False
        own_conn = None
        own_cursor = None
        
        try:
            # Bağlantı yönetimi
            if conn is None or cursor is None or getattr(conn, 'closed', False):
                close_conn = True
                own_conn = self.db.connect()
                if not own_conn:
                    return []
                own_cursor = own_conn.cursor()
                
            # Kullanılacak bağlantı ve cursor
            working_conn = conn if conn and not getattr(conn, 'closed', False) else own_conn
            working_cursor = cursor if cursor and not getattr(cursor, 'closed', False) else own_cursor
            
            # Sorguyu çalıştır
            working_cursor.execute("""
                SELECT parite, borsa, veriler_guncel, ulke 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                WHERE tip = 'INDEX' 
                AND aktif = 1 
                AND (veri_var = 1 OR veri_var IS NULL)
            """)
            
            pairs = []
            for row in working_cursor.fetchall():
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
        finally:
            # Sadece kendimiz açtığımız bağlantıyı kapatırız
            if close_conn:
                try:
                    if own_cursor and not getattr(own_cursor, 'closed', False):
                        own_cursor.close()
                    if own_conn and not getattr(own_conn, 'closed', False):
                        own_conn.close()
                except Exception as e:
                    self.log(f"Bağlantı kapatma hatası: {str(e)}")
            
    def collect_data(self, symbol, start_date, end_date=None, conn=None, cursor=None):
        """Endeks verilerini yfinance'den toplar"""
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
                self._update_data_status(symbol, False, conn, cursor)
                return pd.DataFrame()
            
            if df is None or df.empty:
                self.log(f"yf: {symbol} denendi -> Veri alınamadı")
                self.log("yfinance hata mesajı: Veri bulunamadı")
                self._update_data_status(symbol, False, conn, cursor)
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
                self._update_data_status(symbol, False, conn, cursor)
                return pd.DataFrame()
            
            if 'volume' not in df.columns:
                df['volume'] = 0
            
            if df[required_columns].isnull().any().any():
                self.log(f"yf: {symbol} denendi -> Veri alınamadı")
                self.log("yfinance hata mesajı: Eksik değerler var")
                self._update_data_status(symbol, False, conn, cursor)
                return pd.DataFrame()
            
            # Veri durumunu güncelle
            self._update_data_status(symbol, True, conn, cursor)
            
            return df
            
        except Exception as e:
            self.log(f"yf: {symbol} denendi -> Veri alınamadı")
            self.log(f"yfinance hata mesajı: {str(e)}")
            self._update_data_status(symbol, False, conn, cursor)
            return pd.DataFrame()
            
    def _update_data_status(self, symbol, has_data, conn=None, cursor=None):
        """Endeks için veri durumunu günceller"""
        close_conn = False
        own_conn = None
        own_cursor = None
        
        try:
            # Bağlantı yönetimi
            if conn is None or cursor is None or getattr(conn, 'closed', False):
                close_conn = True
                own_conn = self.db.connect()
                if not own_conn:
                    self.log(f"{symbol} için veritabanı bağlantısı kurulamadı (veri_var güncellemesi)")
                    return
                own_cursor = own_conn.cursor()
                
            # Kullanılacak bağlantı ve cursor
            working_conn = conn if conn and not getattr(conn, 'closed', False) else own_conn
            working_cursor = cursor if cursor and not getattr(cursor, 'closed', False) else own_cursor
            
            # Her durumda güncelle
            yeni_durum = 1 if has_data else 0
            working_cursor.execute("""
                UPDATE p
                SET p.veri_var = ?, p.borsa = ?
                FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                WHERE p.parite = ?
            """, (yeni_durum, symbol, symbol))
            
            # Eğer dışarıdan bir bağlantı aldıysak commit yapmıyoruz
            # Çünkü işlem bütünlüğünü korumak için ana metodun commit yapması gerekiyor
            if close_conn:
                working_conn.commit()
                
            self.log(f"{symbol} için veri_var = {yeni_durum} olarak güncellendi")
            
        except Exception as e:
            self.log(f"Veri durumu güncellenemedi ({symbol}) - Hata: {str(e)}")
            if close_conn and own_conn and not getattr(own_conn, 'closed', False):
                try:
                    own_conn.rollback()
                except Exception as ex:
                    self.log(f"Rollback hatası: {str(ex)}")
        finally:
            # Sadece kendimiz açtığımız bağlantıyı kapatırız
            if close_conn:
                try:
                    if own_cursor and not getattr(own_cursor, 'closed', False):
                        own_cursor.close()
                    if own_conn and not getattr(own_conn, 'closed', False):
                        own_conn.close()
                except Exception as e:
                    self.log(f"Bağlantı kapatma hatası: {str(e)}")
            
    def get_dolar_karsiligi(self, fiyat, ulke, conn=None, cursor=None):
        """Endeksin dolar karşılığını hesaplar"""
        if ulke == 'USA':  # Amerikan endeksleri zaten dolar bazında
            return fiyat
            
        # Veritabanından döviz kurunu al
        close_conn = False
        own_conn = None
        own_cursor = None
        
        try:
            # Bağlantı yönetimi
            if conn is None or cursor is None or getattr(conn, 'closed', False):
                close_conn = True
                own_conn = self.db.connect()
                if not own_conn:
                    return None
                own_cursor = own_conn.cursor()
                
            # Kullanılacak bağlantı ve cursor
            working_conn = conn if conn and not getattr(conn, 'closed', False) else own_conn
            working_cursor = cursor if cursor and not getattr(cursor, 'closed', False) else own_cursor
            
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
                
            working_cursor.execute("""
                SELECT TOP 1 fiyat
                FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                WHERE parite = ? AND borsa = 'FOREX'
                ORDER BY tarih DESC
            """, (f"{currency}/USD",))
            
            row = working_cursor.fetchone()
            if row:
                currency_usd = float(row[0])
                return fiyat / currency_usd  # Endeks değerini dolara çevir
            
            return None
            
        except Exception as e:
            self.log(f"Dolar karşılığı hesaplama hatası: {str(e)}")
            return None
        finally:
            # Sadece kendimiz açtığımız bağlantıyı kapatırız
            if close_conn:
                try:
                    if own_cursor and not getattr(own_cursor, 'closed', False):
                        own_cursor.close()
                    if own_conn and not getattr(own_conn, 'closed', False):
                        own_conn.close()
                except Exception as e:
                    self.log(f"Bağlantı kapatma hatası: {str(e)}")

    def save_candles(self, symbol, df, ulke, conn=None, cursor=None):
        """Mum verilerini veritabanına kaydeder"""
        if df.empty:
            return False
            
        close_conn = False
        own_conn = None
        own_cursor = None
        
        try:
            # Bağlantı yönetimi
            if conn is None or cursor is None or getattr(conn, 'closed', False):
                close_conn = True
                own_conn = self.db.connect()
                if not own_conn:
                    return False
                own_cursor = own_conn.cursor()
                
            # Kullanılacak bağlantı ve cursor
            working_conn = conn if conn and not getattr(conn, 'closed', False) else own_conn
            working_cursor = cursor if cursor and not getattr(cursor, 'closed', False) else own_cursor
            
            # Borsa bilgisini al
            yf_symbol = symbol.split('/')[0]
            index = yf.Ticker(yf_symbol)
            info = index.info
            exchange = info.get("exchange", "INDEX").upper() if info else "INDEX"
            
            # Döviz kuru için bir kere sorgula
            currency_map = {
                'Turkey': 'TRY',
                'Japan': 'JPY',
                'UK': 'GBP',
                'Europe': 'EUR',
                # Diğer ülkeler eklenebilir
            }
            
            currency_usd = None
            if ulke != 'USA' and ulke in currency_map:  # Amerikan endeksleri zaten dolar bazında
                currency = currency_map.get(ulke)
                working_cursor.execute("""
                    SELECT TOP 1 fiyat
                    FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                    WHERE parite = ? AND borsa = 'FOREX'
                    ORDER BY tarih DESC
                """, (f"{currency}/USD",))
                
                row = working_cursor.fetchone()
                if row:
                    currency_usd = float(row[0])
            
            kayit_sayisi = 0
            
            for tarih, row in df.iterrows():
                try:
                    fiyat = float(row['close'])
                    
                    # Dolar karşılığı hesapla
                    if ulke == 'USA':
                        dolar_karsiligi = fiyat
                    elif currency_usd is not None:
                        dolar_karsiligi = fiyat / currency_usd
                    else:
                        self.log(f"Dolar karşılığı hesaplanamadı: {symbol}")
                        continue
                        
                    working_cursor.execute("""
                        IF NOT EXISTS (
                            SELECT 1 FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                            WHERE parite = ? AND [interval] = ? AND tarih = ?
                        )
                        INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar] (
                            parite, [interval], tarih, fiyat, dolar_karsiligi, borsa, tip, ulke
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, 
                    (symbol, '1d', tarih, 
                     symbol, '1d', tarih, fiyat, dolar_karsiligi, exchange, 'INDEX', ulke))
                    
                    kayit_sayisi += 1
                    
                except Exception as e:
                    self.log(f"Kayıt hatası ({symbol}, {tarih}): {str(e)}")
                    continue
                    
            # Sadece kendimiz bağlantı açtıysak commit yap
            if close_conn and not getattr(working_conn, 'closed', False):
                working_conn.commit()
            
            if kayit_sayisi > 0:
                self.log(f"{symbol} için {kayit_sayisi} yeni kayıt eklendi")
                # Veri başarıyla kaydedildi, veri_var'ı 1 yap ve borsa bilgisini güncelle
                try:
                    working_cursor.execute("""
                        UPDATE p
                        SET p.veri_var = ?, p.borsa = ?
                        FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                        WHERE p.parite = ?
                    """, (1, exchange, symbol))
                    
                    if close_conn and not getattr(working_conn, 'closed', False):
                        working_conn.commit()
                except Exception as e:
                    self.log(f"Parite durumu güncellenemedi ({symbol}): {str(e)}")
                
            return True
            
        except Exception as e:
            self.log(f"Veri kaydetme hatası ({symbol}): {str(e)}")
            if close_conn and own_conn and not getattr(own_conn, 'closed', False):
                try:
                    own_conn.rollback()
                except Exception as ex:
                    self.log(f"Rollback hatası: {str(ex)}")
            return False
        finally:
            # Sadece kendimiz açtığımız bağlantıyı kapatırız
            if close_conn:
                try:
                    if own_cursor and not getattr(own_cursor, 'closed', False):
                        own_cursor.close()
                    if own_conn and not getattr(own_conn, 'closed', False):
                        own_conn.close()
                except Exception as e:
                    self.log(f"Bağlantı kapatma hatası: {str(e)}") 