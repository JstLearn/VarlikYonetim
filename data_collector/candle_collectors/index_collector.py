"""
Endeks veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
import investpy
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
                        veriler = self.collect_data(symbol, self.baslangic_tarihi, None, conn, cursor, ulke)
                        if not veriler.empty:
                            self.save_candles(symbol, veriler, ulke, conn, cursor)
                    else:
                        # Son tarihten sonraki verileri al
                        simdi = datetime.now(timezone.utc)
                        son_guncelleme = datetime.combine(son_tarih.date(), datetime.min.time()).replace(tzinfo=timezone.utc)
                        
                        # Bugünün bir gün öncesini al, bugünün verilerini toplamayalım
                        dun = (simdi - timedelta(days=1)).date()
                        
                        if son_guncelleme.date() < dun:
                            veriler = self.collect_data(
                                symbol,
                                son_guncelleme + timedelta(days=1),
                                datetime.combine(dun, datetime.max.time()).replace(tzinfo=timezone.utc),  # Dünün son anına kadar
                                conn,
                                cursor,
                                ulke
                            )
                            if not veriler.empty:
                                self.save_candles(symbol, veriler, ulke, conn, cursor)
                        else:
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
            
    def collect_data(self, symbol, start_date, end_date=None, conn=None, cursor=None, ulke=None):
        """Endeks verilerini önce yfinance, sonra investpy'dan toplar"""
        # Bugünün tarihini alalım (end_date None ise)
        end_date = end_date or datetime.now(timezone.utc)
        
        # Tarihleri string formatına çevir
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        # 1. ADIM: yfinance'dan veri almayı dene
        try:
            
            # Tüm uyarıları bastır
            import warnings, sys, io
            warnings.filterwarnings('ignore')
            
            # yfinance hata mesajlarını yakala
            stderr = sys.stderr
            sys.stderr = io.StringIO()
            
            # Symbol formatını kontrol et ve ayarla
            yf_symbol = symbol.split('/')[0] if '/' in symbol else symbol
            
            # yfinance'den veri çek
            df = yf.download(
                tickers=yf_symbol,
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
            
            # Verileri kontrol et
            if (not df.empty and 
                'Open' in df.columns and 
                'High' in df.columns and 
                'Low' in df.columns and 
                'Close' in df.columns):
                
                # DataFrame'i düzenle
                df = df.rename(columns={
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                })
                
                # Volume yoksa ekle
                if 'volume' not in df.columns:
                    df['volume'] = 0
                
                # Verilerin geçerliliğini kontrol et
                required_cols = ['open', 'high', 'low', 'close']
                if not df[required_cols].isnull().any().any():
                    self._update_data_status(symbol, True, conn, cursor)
                    return df
                        
        except Exception as e:
            self.log(f"yf: {symbol} veri alma hatası: {str(e)}")
        
        # 2. ADIM: investpy'dan veri almayı dene
        try:
            
            # Symbol ve ülke formatını kontrol et
            index_symbol = symbol.split('/')[0] if '/' in symbol else symbol
            
            # Ülke adını düzelt - investpy küçük harf bekler
            country = ulke.lower() if ulke else None
            
            # Özel durumlar için ülke adı kontrolü
            if country == "usa":
                country = "united states"
            elif country == "uk":
                country = "united kingdom"
            
            # Tarih formatını ayarla (investpy için d/m/Y formatı)
            from_date = start_date.strftime('%d/%m/%Y')
            to_date = end_date.strftime('%d/%m/%Y')
            
            # Endeks verisini al
            if country:
                try:
                    # İlk ülkeye göre arama yap
                    historical_data = investpy.indices.get_index_historical_data(
                        index=index_symbol,
                        country=country,
                        from_date=from_date,
                        to_date=to_date
                    )
                    
                    if not historical_data.empty:
                        # DataFrame'i düzenle
                        historical_data = historical_data.rename(columns={
                            'Open': 'open',
                            'High': 'high',
                            'Low': 'low',
                            'Close': 'close',
                            'Volume': 'volume'
                        })
                        
                        # Verilerin geçerliliğini kontrol et
                        required_cols = ['open', 'high', 'low', 'close']
                        if all(col in historical_data.columns for col in required_cols):
                            if not historical_data[required_cols].isnull().any().any():
                                self._update_data_status(symbol, True, conn, cursor)
                                return historical_data
                    
                except Exception:
                    pass
                
                try:
                    # Ülkeye göre arama başarısız olduysa, sembol adı ile arama yap
                    search_results = investpy.search_indices(
                        by='name',
                        value=index_symbol
                    )
                    
                    if not search_results.empty:
                        found_index = search_results.iloc[0]
                        found_country = found_index['country']
                        found_name = found_index['name']
                        
                        historical_data = investpy.indices.get_index_historical_data(
                            index=found_name,
                            country=found_country,
                            from_date=from_date,
                            to_date=to_date
                        )
                        
                        if not historical_data.empty:
                            # DataFrame'i düzenle
                            historical_data = historical_data.rename(columns={
                                'Open': 'open',
                                'High': 'high',
                                'Low': 'low',
                                'Close': 'close',
                                'Volume': 'volume'
                            })
                            
                            # Verilerin geçerliliğini kontrol et
                            required_cols = ['open', 'high', 'low', 'close']
                            if all(col in historical_data.columns for col in required_cols):
                                if not historical_data[required_cols].isnull().any().any():
                                    self._update_data_status(symbol, True, conn, cursor)
                                    return historical_data
                except Exception:
                    pass
                            
        except Exception as e:
            self.log(f"invest: {symbol} veri alma hatası: {str(e)}")
        
        # Her iki API de başarısız oldu, veri durumunu false olarak güncelle
        self._update_data_status(symbol, False, conn, cursor)
        return pd.DataFrame()  # Boş veri döndür
            
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
                        # Döviz kuru bulunamadıysa, varsayılan olarak fiyatın kendisini kullan
                        # ve uyarı mesajı göster
                        dolar_karsiligi = fiyat
                        
                    working_cursor.execute("""
                        INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar] (parite, [interval], tarih, fiyat, dolar_karsiligi, borsa, tip, ulke)
                        SELECT ?, ?, ?, ?, ?, ?, ?, ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                            WHERE parite = ? AND [interval] = ? AND tarih = ?
                        )
                    """, 
                    (symbol, '1d', tarih, fiyat, dolar_karsiligi, exchange, 'INDEX', ulke, 
                     symbol, '1d', tarih))
                    
                    kayit_sayisi += 1
                    
                except Exception as e:
                    pass
                    
            # Her durumda commit yap - sadece bağlantı açık ise
            if not getattr(working_conn, 'closed', False):
                working_conn.commit()
            
            if kayit_sayisi > 0:
                self.log(f"{symbol} için {kayit_sayisi} yeni kayıt eklendi")
                # Veri başarıyla kaydedildi, veri_var'ı 1 yap ve borsa bilgisini güncelle
                try:
                    working_cursor.execute("""
                        UPDATE p
                        SET p.veri_var = ?, p.borsa = ?, p.kayit_tarihi = GETDATE()
                        FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                        WHERE p.parite = ?
                    """, (1, exchange, symbol))
                    
                    # Her durumda commit yap
                    if not getattr(working_conn, 'closed', False):
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