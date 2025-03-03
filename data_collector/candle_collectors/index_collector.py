"""
Endeks veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta, date
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
        """Çalışma metodu"""
        self.log(f"Endeks verileri toplanıyor...")
        
        try:
            # Veritabanı bağlantısı
            conn = self.db.connect()
            if not conn:
                self.log("Veritabanına bağlanılamadı")
                return False
                
            cursor = conn.cursor()
            if not cursor:
                self.log("Cursor oluşturulamadı")
                conn.close()
                return False
                
            # Aktif çiftleri al
            cursor.execute("""
                SELECT parite, ulke, veri_var, ISNULL(CONVERT(DATE, kayit_tarihi), '1900-01-01') as kayit_tarihi
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH(NOLOCK)
                WHERE aktif = 1 AND tip = 'INDEX'
            """)
            
            rows = cursor.fetchall()
            if not rows:
                self.log("İşlenecek parite bulunamadı")
                cursor.close()
                conn.close()
                return False
                
            # İşlem yapılacak endeks sayısı
            processed_count = 0
            error_count = 0
            updated_count = 0
            skipped_count = 0
            
            # Toplam endeks sayısını log
            self.log(f"Toplam {len(rows)} endeks işlenecek")
            
            # Şu anki UTC zamanı
            now = datetime.now(timezone.utc)
            today = now.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday = today - timedelta(days=1)
            yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Her bir endeksi işle
            for row in rows:
                symbol, ulke, veri_var, kayit_tarihi = row
                
                try:
                    # Veritabanındaki son kapanış tarihini kontrol et
                    cursor.execute("""
                        SELECT TOP 1 tarih
                        FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH(NOLOCK)
                        WHERE parite = ? AND [interval] = '1d'
                        ORDER BY tarih DESC
                    """, (symbol,))
                    
                    son_tarih_row = cursor.fetchone()
                    son_tarih = None if son_tarih_row is None else son_tarih_row[0]
                    
                    if veri_var == 0 or son_tarih is None:
                        # Hiç veri yoksa, başlangıç tarihinden dünün sonuna kadar veri topla
                        result = self.collect_data(symbol, ulke, self.baslangic_tarihi, yesterday_end.strftime('%Y-%m-%d'), conn, cursor)
                        
                        if result:
                            updated_count += 1
                        else:
                            error_count += 1
                            
                    else:
                        # Son kayıt tarihini datetime'a çevir
                        son_tarih_dt = son_tarih.replace(tzinfo=timezone.utc) if son_tarih.tzinfo is None else son_tarih
                        
                        # Son tarih dünden önceyse yeni veri topla
                        if son_tarih_dt < yesterday_start:
                            # Son tarihten sonraki günden dünün sonuna kadar veri topla
                            yeni_baslangic = (son_tarih_dt + timedelta(days=1)).strftime('%Y-%m-%d')
                            result = self.collect_data(symbol, ulke, yeni_baslangic, yesterday_end.strftime('%Y-%m-%d'), conn, cursor)
                            
                            if result:
                                updated_count += 1
                            else:
                                error_count += 1
                        else:
                            # Veritabanı güncel, yeni veri çekmeye gerek yok
                            self.log(f"{symbol} için veritabanı güncel (son veri tarihi: {son_tarih.strftime('%Y-%m-%d')}), yeni veri çekilmiyor")
                            skipped_count += 1
                    
                    processed_count += 1
                    
                except Exception as e:
                    error_count += 1
                    self.log(f"Endeks işleme hatası ({symbol}): {str(e)}")
                    continue
                    
            # İşlem sonucunu log
            self.log(f"İşlem tamamlandı. Toplam: {len(rows)}, İşlenen: {processed_count}, Atlanılan: {skipped_count}, Güncellenen: {updated_count}, Hata: {error_count}")
            
            # Bağlantıyı kapat
            cursor.close()
            conn.close()
            
            return True
            
        except Exception as e:
            self.log(f"Veritabanı hatası: {str(e)}")
            return False
        
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
                    if own_cursor and not getattr(own_cursor, 'closed', True):
                        own_cursor.close()
                    if own_conn and not getattr(own_conn, 'closed', True):
                        own_conn.close()
                except Exception as e:
                    self.log(f"Bağlantı kapatma hatası: {str(e)}")
            
    def collect_data(self, symbol, ulke, start_date, end_date=None, conn=None, cursor=None):
        """Endeks verilerini yfinance'den toplar ve veritabanına kaydeder"""
        try:
            # Başlangıç tarihini datetime.date formatından datetime formatına dönüştür
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            elif isinstance(start_date, date) and not isinstance(start_date, datetime):
                start_date = datetime.combine(start_date, datetime.min.time()).replace(tzinfo=timezone.utc)
                
            # Bitiş tarihini kontrol et
            if end_date is None:
                # UTC+0'a göre dünün sonunu al
                now = datetime.now(timezone.utc)
                yesterday = (now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1))
                end_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
                self.log(f"Bitiş tarihi belirtilmemiş, {end_date.strftime('%Y-%m-%d %H:%M:%S')} kullanılıyor")
            elif isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
                # Eğer saat bilgisi yoksa, günün sonunu al
                if end_date.hour == 0 and end_date.minute == 0 and end_date.second == 0:
                    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
                end_date = end_date.replace(tzinfo=timezone.utc)
                
            # Tarihleri string formatına çevir
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')  # yfinance bitiş tarihini dahil etmiyor, +1 gün ekle
            
            self.log(f"{symbol} veri toplanıyor: {start_str} -> {end_date.strftime('%Y-%m-%d')}")
            
            # yfinance'dan veri almayı dene
            try:
                # Tüm uyarıları bastır
                import warnings, sys, io
                warnings.filterwarnings('ignore')
                
                # Symbol formatını kontrol et ve ayarla
                yf_symbol = symbol.split('/')[0] if '/' in symbol else symbol
                
                # yfinance'den veri çek
                df = yf.download(
                    tickers=yf_symbol,
                    start=start_str,
                    end=end_str,  # Bitiş tarihine +1 gün eklenmiş halde
                    interval='1d',
                    progress=False,
                    auto_adjust=True,
                    prepost=False,
                    threads=False
                )
                
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
                    
                    # Index ismi 'Date' olacak şekilde düzenle
                    df.index.name = 'Date'
                    
                    # Veritabanına kaydet
                    result = self.save_candles(symbol, df, ulke, conn, cursor)
                    
                    # Başarılı ise True döndür
                    if result:
                        self.log(f"{symbol} için {len(df)} kayıt bulundu ve işlendi")
                        return True
                    else:
                        self.log(f"{symbol} verileri kaydedilirken hata oluştu")
                        return False
                else:
                    self.log(f"{symbol} için yfinance'den veri bulunamadı")
                    return False
                
            except Exception as e:
                self.log(f"{symbol} için yfinance hatası: {str(e)}")
                return False
                
        except Exception as e:
            self.log(f"{symbol} için veri toplama hatası: {str(e)}")
            return False
            
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
                    FROM [VARLIK_YONETIM].[dbo].[kurlar]
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
                        dolar_karsiligi = fiyat
                        
                    working_cursor.execute("""
                        INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar] (parite, [interval], tarih, fiyat, dolar_karsiligi, borsa, tip, ulke)
                        SELECT ?, ?, ?, ?, ?, ?, ?, ?
                        WHERE NOT EXISTS (
                            SELECT 1 FROM [VARLIK_YONETIM].[dbo].[kurlar]
                            WHERE parite = ? AND [interval] = ? AND tarih = ?
                        )
                    """, 
                    (symbol, '1d', tarih, fiyat, dolar_karsiligi, exchange, 'INDEX', ulke, 
                     symbol, '1d', tarih))
                    
                    if working_cursor.rowcount > 0:
                        kayit_sayisi += 1
                        # Yeni kapanış fiyatlarını logla
                        self.log(f"🔍 YENİ VERİ - {symbol} için {tarih.strftime('%Y-%m-%d')}: Kapanış fiyatı = {fiyat}, USD karşılığı = {dolar_karsiligi:.2f}")
                    
                except Exception as e:
                    self.log(f"Kayıt hatası ({symbol}, {tarih}): {str(e)}")
                    continue
                    
            # Veriler kaydedildi, commit yap - sadece bağlantı açık ise
            if not getattr(working_conn, 'closed', False):
                working_conn.commit()
            
            # Veri başarıyla kaydedildi, veri_var'ı 1 olarak güncelle (kayıt sayısı 0 olsa bile)
            try:
                # SQL sorgusunu basitleştir, NOLOCK kaldır
                working_cursor.execute("""
                    UPDATE [VARLIK_YONETIM].[dbo].[pariteler]
                    SET veri_var = 1, 
                        borsa = ?, 
                        kayit_tarihi = GETDATE()
                    WHERE parite = ?
                """, (exchange, symbol))
                
                # Etkilenen satır sayısını al
                row_count = working_cursor.rowcount
                
                # Her durumda commit yap
                if not getattr(working_conn, 'closed', False):
                    working_conn.commit()
                    
                # Güncelleme başarılı oldu mu kontrol et
                if row_count > 0:
                    self.log(f"{symbol} için veri_var = 1 olarak güncellendi")
                else:
                    self.log(f"{symbol} için güncelleme yapılamadı (etkilenen satır: {row_count})")
                
            except Exception as e:
                self.log(f"Parite durumu güncellenemedi ({symbol}): {str(e)}")
                
            if kayit_sayisi > 0:
                self.log(f"{symbol} için {kayit_sayisi} yeni kayıt eklendi")
            else:
                self.log(f"{symbol} için veritabanı güncel, yeni kayıt yok")
                
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
                    if own_cursor and not getattr(own_cursor, 'closed', True):
                        own_cursor.close()
                    if own_conn and not getattr(own_conn, 'closed', True):
                        own_conn.close()
                except Exception as e:
                    self.log(f"Bağlantı kapatma hatası: {str(e)}") 