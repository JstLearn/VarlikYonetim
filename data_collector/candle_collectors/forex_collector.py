"""
Forex veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
from utils.database import Database
from utils.config import COLLECTION_CONFIG
import time
import traceback
import sys

class ForexCollector:
    def __init__(self):
        self.db = Database()
        self.baslangic_tarihi = datetime.strptime(COLLECTION_CONFIG['start_date'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        self.usd_pairs_cache = {}  # USD parite önbelleği
        self.connection_recovery_time = 60  # saniye - bağlantı havuzu toparlanma süresi
        
    def log(self, message):
        """Zaman damgalı log mesajı yazdırır"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")
        
    def _safe_close(self, cursor=None, connection=None):
        """Cursor ve Connection'ı güvenli bir şekilde kapatır"""
        try:
            if cursor:
                cursor.close()
        except Exception as e:
            print(f"Cursor kapatma hatası: {str(e)}")
            
        try:
            if connection:
                connection.close()
        except Exception as e:
            print(f"Bağlantı kapatma hatası: {str(e)}")
        
    def _get_connection(self):
        """Veritabanı bağlantısını oluşturur ve yeniden deneme stratejisi uygular"""
        max_retries = 5  # Daha fazla deneme
        retry_delay = 2  # saniye
        
        for attempt in range(1, max_retries + 1):
            try:
                conn = self.db.connect()
                if conn:
                    if attempt > 1:
                        self.log(f"Bağlantı {attempt}. denemede başarıyla kuruldu.")
                    return conn
                else:
                    self.log(f"Bağlantı oluşturulamadı ({attempt}/{max_retries})")
            except Exception as e:
                self.log(f"Bağlantı hatası ({attempt}/{max_retries}): {str(e)}")
            
            # Son deneme değilse bekle
            if attempt < max_retries:
                # Exponential backoff - her denemede bekleme süresini 2 katına çıkar
                wait_time = retry_delay * (2 ** (attempt - 1))
                self.log(f"Yeniden denemeden önce {wait_time} saniye bekleniyor...")
                time.sleep(wait_time)
        
        self.log(f"Maksimum deneme sayısına ulaşıldı ({max_retries}), bağlantı kurulamadı.")
        return None
        
    def _execute_query(self, query, params=None, fetch_one=False, fetch_all=False):
        """Sorgulama işlemlerini güvenli şekilde yapar"""
        conn = None
        cursor = None
        result = None
        
        try:
            conn = self._get_connection()
            if not conn:
                self.log("Sorgu yürütülemedi - bağlantı kurulamadı")
                return None
                
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            if fetch_one:
                result = cursor.fetchone()
            elif fetch_all:
                result = cursor.fetchall()
            else:
                conn.commit()
                result = True
                
            return result
            
        except Exception as e:
            self.log(f"Sorgu yürütme hatası: {str(e)}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            return None
        finally:
            self._safe_close(cursor, conn)
        
    def _load_usd_pairs(self):
        """Tüm USD kurlarını bir kerede yükler ve önbelleğe alır"""
        # İlk önce, mevcut USD çiftlerini kullanılabilir tutalım
        temp_cache = self.usd_pairs_cache.copy() if self.usd_pairs_cache else {}
        
        try:
            query = """
                SELECT p.parite, k.fiyat
                FROM [VARLIK_YONETIM].[dbo].[kurlar] k WITH (NOLOCK)
                INNER JOIN (
                    SELECT parite, MAX(tarih) as son_tarih
                    FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                    WHERE (parite LIKE 'USD/%' OR parite LIKE '%/USD')
                    AND borsa = 'FOREX'
                    GROUP BY parite
                ) AS son ON k.parite = son.parite AND k.tarih = son.son_tarih
                WHERE k.borsa = 'FOREX'
            """
            
            rows = self._execute_query(query, fetch_all=True)
            if not rows:
                self.log("USD kurları yüklenemedi - Sorgu sonuç döndürmedi")
                return False
                
            # Sonuçları önbelleğe al
            self.usd_pairs_cache = {}  # Önbelleği temizle
            row_count = 0
            for row in rows:
                parite = row[0]
                fiyat = float(row[1])
                self.usd_pairs_cache[parite] = fiyat
                row_count += 1
                
            self.log(f"Toplam {row_count} USD paritesi önbelleğe alındı")
            
            # Eğer hiçbir şey yüklenemezse, eski önbelleği geri yükle
            if row_count == 0 and temp_cache:
                self.usd_pairs_cache = temp_cache
                self.log("Yeni USD kurları yüklenemedi, eski kurlar kullanılıyor")
                
            return row_count > 0
            
        except Exception as e:
            self.log(f"USD kurları yüklenirken hata oluştu: {str(e)}")
            # Hata durumunda eski önbelleği geri yükle
            if temp_cache:
                self.usd_pairs_cache = temp_cache
                self.log("Hata nedeniyle eski USD kurları kullanılıyor")
            return False
                
    def get_active_pairs(self):
        """Aktif Forex paritelerini getirir"""
        try:
            query = """
                SELECT parite, borsa, veriler_guncel, ulke, veri_var
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                WHERE borsa = 'FOREX' AND tip = 'SPOT' 
                AND aktif = 1 
                AND (veri_var = 1 OR veri_var IS NULL)
            """
            
            rows = self._execute_query(query, fetch_all=True)
            if not rows:
                self.log("Forex pariteleri sorgulandı fakat sonuç bulunamadı")
                return []
                
            pairs = []
            for row in rows:
                pairs.append({
                    'symbol': row[0],
                    'exchange': row[1] if row[1] else 'FOREX',
                    'ulke': row[3],
                    'veri_var': row[4]  # veri_var değerini de pairs listesine ekliyoruz
                })
                
            if pairs:
                self.log(f"Toplam {len(pairs)} Forex çifti işlenecek")
                
            return pairs
            
        except Exception as e:
            self.log(f"Hata: Forex pariteleri alınamadı - {str(e)}")
            # Hatanın detayını da logla
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
            self.log("Hata detayı: " + "".join(error_details))
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
                threads=False,  # Tek thread kullan
                timeout=10      # 10 saniye timeout
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
            import time
            
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
            
            max_retries = 3
            retry_delay = 5  # saniye
            
            for retry in range(max_retries):
                try:
                    # Investing.com'dan veri çek
                    df = investpy.get_currency_cross_historical_data(
                        currency_cross=f'{base}/{quote}',
                        from_date=start_str,
                        to_date=end_str
                    )
                    
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        return df, True
                        
                except Exception as retry_error:
                    if "ERR#0015" in str(retry_error) and retry < max_retries - 1:
                        # Rate limit hatası, bekle ve tekrar dene
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Her denemede bekleme süresini 2 katına çıkar
                        continue
                    raise  # Diğer hataları veya son denemeyi yukarı fırlat
                
        except Exception as e:
            inv_error = str(e)
            if "currency_cross" in inv_error.lower():
                # Bilinen hata, log basma
                pass
            elif "YFTzMissingError" in str(yf_error):
                # Bilinen hata, log basma
                pass
            elif "ERR#0015" in inv_error:
                # Rate limit hatası, log basma
                pass
            else:
                # Beklenmeyen hata, log bas
                self.log(f"yf: {yf_symbol}   inv:{symbol} denendi -> Veri alınamadı")
                self.log(f"yfinance hata mesajı: {yf_error}")
                self.log(f"investing hata mesajı: {inv_error}")
            
        # Her iki kaynaktan da veri alınamadıysa veri_var değerini 0 yap
        self._execute_query(
            "UPDATE p SET p.veri_var = ? FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK) WHERE p.parite = ?",
            (0, symbol)
        )
                    
        return pd.DataFrame(), False
            
    def _update_data_status(self, symbol, has_data):
        """Parite için veri durumunu günceller"""
        yeni_durum = 1 if has_data else 0
        result = self._execute_query(
            "UPDATE p SET p.veri_var = ? FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK) WHERE p.parite = ?",
            (yeni_durum, symbol)
        )
        
        if result:
            self.log(f"{symbol} için veri_var = {yeni_durum} olarak güncellendi")
        else:
            self.log(f"{symbol} için veri_var güncellenemedi")
            
        return result
                
    def save_candles(self, symbol, df, ulke):
        """Mum verilerini veritabanına kaydeder"""
        if df.empty:
            return False
        
        # Dolar çevrimleri için önbellek yüklü değilse yükle    
        if not self.usd_pairs_cache:
            self._load_usd_pairs()
            
        conn = None    
        cursor = None
        try:
            conn = self._get_connection()
            if not conn:
                return False
                
            cursor = conn.cursor()
            kayit_sayisi = 0
            
            # Parite çiftini ayır
            base, quote = symbol.split('/')
            
            # USD doğrudan hesaplama - USD paritesi için çevrim gerekmez
            is_usd_quote = (quote == 'USD')
            
            # USD karşılığı hesapla (bağlantı açıp kapatmamak için burada hesapla)
            quote_usd = None
            usd_quote = None
            
            if not is_usd_quote:
                # Önbellekten kontrol et
                if f"{quote}/USD" in self.usd_pairs_cache:
                    quote_usd = self.usd_pairs_cache[f"{quote}/USD"]
                elif f"USD/{quote}" in self.usd_pairs_cache:
                    usd_quote = self.usd_pairs_cache[f"USD/{quote}"]
                else:
                    # Önbellekte yoksa veritabanından sorgula
                    try:
                        # Önce QUOTE/USD formatında ara
                        cursor.execute("""
                            SELECT TOP 1 fiyat
                            FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                            WHERE parite = ? AND borsa = 'FOREX'
                            ORDER BY tarih DESC
                        """, (f"{quote}/USD",))
                        
                        row = cursor.fetchone()
                        if row:
                            quote_usd = float(row[0])
                            # Önbelleğe ekle
                            self.usd_pairs_cache[f"{quote}/USD"] = quote_usd
                        else:
                            # Bulunamazsa USD/QUOTE formatında ara
                            cursor.execute("""
                                SELECT TOP 1 fiyat
                                FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                                WHERE parite = ? AND borsa = 'FOREX'
                                ORDER BY tarih DESC
                            """, (f"USD/{quote}",))
                            
                            row = cursor.fetchone()
                            if row:
                                usd_quote = float(row[0])
                                # Önbelleğe ekle
                                self.usd_pairs_cache[f"USD/{quote}"] = usd_quote
                    except Exception as e:
                        self.log(f"USD karşılığı sorgusu hatası ({symbol}): {str(e)}")
            
            # DataFrame'deki her satır için
            for tarih, row in df.iterrows():
                try:
                    fiyat = float(row['Close'])
                    
                    # Dolar karşılığını hesapla
                    if is_usd_quote:
                        # USD paritesi ise doğrudan fiyatı kullan
                        dolar_karsiligi = fiyat
                    elif quote_usd is not None:
                        # QUOTE/USD formatı bulunduysa kullan
                        dolar_karsiligi = fiyat * quote_usd
                    elif usd_quote is not None:
                        # USD/QUOTE formatı bulunduysa tersini kullan
                        dolar_karsiligi = fiyat * (1 / usd_quote)
                    else:
                        # Dolar karşılığı hesaplanamadı, bu kaydı atla
                        self.log(f"{symbol} -> Dolar karşılığı hesaplanamadı, bu kayıt atlanıyor")
                        continue
                    
                    # Önce kaydın var olup olmadığını kontrol et
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                        WHERE parite = ? AND [interval] = ? AND tarih = ?
                    """, (symbol, '1d', tarih))
                    
                    row = cursor.fetchone()
                    count = row[0] if row else 0
                    
                    if count == 0:  # Kayıt yoksa ekle
                        cursor.execute("""
                            INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar] (
                                parite, [interval], tarih, fiyat, dolar_karsiligi, borsa, tip, ulke
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, 
                        (symbol, '1d', tarih, fiyat, dolar_karsiligi, 'FOREX', 'SPOT', ulke))
                        kayit_sayisi += 1
                    
                except Exception as e:
                    self.log(f"SQL Kayıt hatası ({symbol}, {tarih}): {str(e)}")
                    continue
                    
            conn.commit()
            
            if kayit_sayisi > 0:
                self.log(f"{symbol} için {kayit_sayisi} yeni kayıt eklendi")
                # Veri başarıyla kaydedildi, veri_var'ı 1 yap
                cursor.execute("""
                    UPDATE p
                    SET p.veri_var = ?
                    FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                    WHERE p.parite = ?
                """, (1, symbol))
                conn.commit()
                
            return True
            
        except Exception as e:
            self.log(f"Veritabanı hatası ({symbol}): {str(e)}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            return False
        finally:
            self._safe_close(cursor, conn)
            
    def process_pair(self, pair):
        """Tek bir forex paritesini işler"""
        symbol = pair['symbol']
        ulke = pair['ulke']
        veri_var = pair.get('veri_var')
        
        # Çok sık bağlantı açmadan önce biraz bekle
        time.sleep(1)
        
        try:
            # Son kayıt tarihini sorgula
            query = "SELECT MAX(tarih) as son_tarih FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK) WHERE parite = ?"
            result = self._execute_query(query, (symbol,), fetch_one=True)
            
            if result is None:
                self.log(f"{symbol} için son tarih sorgulanamadı. İşleme atlanıyor.")
                return False
                
            son_tarih = result[0] if result and result[0] else None
            
            # Bugünün ve dünün tarihini al
            simdi = datetime.now()
            bugun = simdi.replace(hour=0, minute=0, second=0, microsecond=0)
            dun = bugun - timedelta(days=1)
            
            # İşleme gerekli mi karar ver, gerekli değilse erken dön
            islem_gerekli = False
            baslangic = None
            bitis = dun
            
            # Eğer son tarih varsa, gün kısmını al
            if son_tarih is not None:
                son_guncelleme_gunu = son_tarih.replace(hour=0, minute=0, second=0, microsecond=0)
                
                # Eğer son güncelleme bugünse, bu veriyi atla
                if son_guncelleme_gunu.date() == bugun.date():
                    self.log(f"{symbol} -> Veriler zaten bugün için güncel (Son güncelleme: {son_guncelleme_gunu.date()})")
                    return True
                
                # Eğer son güncelleme dünse, bugünün verileri henüz tam olmayabilir, atla
                if son_guncelleme_gunu.date() == dun.date():
                    self.log(f"{symbol} -> Dünün verileri güncel, bugünün verileri henüz işlenmeyecek (Son güncelleme: {son_guncelleme_gunu.date()})")
                    return True
                    
                # Eğer veri_var = 1 ise ve son tarih bugün veya dün değilse, verileri güncelle
                if veri_var == 1:
                    if son_guncelleme_gunu.date() < dun.date():
                        self.log(f"{symbol} -> Son güncelleme: {son_guncelleme_gunu.date()}, dünün verilerine kadar alınacak")
                        baslangic = son_guncelleme_gunu + timedelta(days=1)  # Son güncellemeden sonraki gün
                        islem_gerekli = True
                else:
                    # Son tarihten sonraki verileri dünün sonuna kadar al
                    son_guncelleme = datetime.combine(son_tarih.date(), datetime.min.time())
                    
                    if son_guncelleme.date() < dun.date():
                        self.log(f"{symbol} -> Son güncelleme: {son_guncelleme.date()}, dünün sonuna kadar veriler alınacak")
                        baslangic = son_guncelleme + timedelta(days=1)
                        islem_gerekli = True
            else:
                # Hiç veri yoksa başlangıç tarihinden itibaren dünün sonuna kadar al
                self.log(f"{symbol} -> Hiç veri yok, başlangıçtan dünün sonuna kadar alınacak")
                baslangic = self.baslangic_tarihi
                islem_gerekli = True
                
            # İşlem gerekli değilse erken dön
            if not islem_gerekli:
                return True
                
            # Başlangıç tarihi geçersizse düzelt
            if baslangic.date() > dun.date():
                baslangic = dun
            
            # Harici veri topla - bağlantı kapalıyken
            veriler, has_data = self.collect_data(symbol, baslangic, bitis)
            
            # Veri başarıyla toplandıysa kaydet
            if has_data:
                return self.save_candles(symbol, veriler, ulke)
            else:
                self.log(f"{symbol} -> Veri toplanamadı")
                return False
                
        except Exception as e:
            self.log(f"İşlem hatası ({symbol}): {str(e)}")
            return False
            
        return False
            
    def run(self):
        """Tüm Forex verilerini toplar"""
        self.log("="*50)
        
        # Bağlantı havuzunu test et
        test_conn = self._get_connection()
        if not test_conn:
            self.log("Veritabanı bağlantısı kurulamadı. İşlem iptal ediliyor.")
            return
        self._safe_close(None, test_conn)
        
        # USD kurlarını önden yükle
        self.log("USD kurları yükleniyor...")
        self._load_usd_pairs()
        
        # Aktif pariteleri al
        pairs = self.get_active_pairs()
        if not pairs:
            self.log("İşlenecek Forex verisi yok")
            return
            
        # İşlenecek toplam parite sayısı
        total_pairs = len(pairs)
        self.log(f"Toplam {total_pairs} Forex paritesi işlenecek")
        
        # Pariteleri küçük gruplara böl
        group_size = 10  # Çok daha küçük gruplar
        wait_time_between_groups = 30  # saniye - daha uzun bekleme
        
        # İlerleme için sayaçlar
        processed_pairs = 0
        successful_pairs = 0
        
        # Grupları oluştur
        for i in range(0, total_pairs, group_size):
            # Her grup başlangıcında bağlantı havuzunu dinlendirmek için daha uzun bekle
            if i > 0:
                self.log(f"Yeni grup başlamadan önce bağlantı havuzunu dinlendirmek için bekleniyor...")
                time.sleep(30)  # 30 saniye bekle
                
            # Bağlantı havuzunu test et
            test_conn = self._get_connection()
            if not test_conn:
                self.log("Veritabanı bağlantısı kurulamadı. Bağlantı havuzu toparlanması için bekleniyor...")
                time.sleep(self.connection_recovery_time)
                self.connection_recovery_time *= 2  # Her bağlantı hatası sonrası bekleme süresini artır
                continue
            self._safe_close(None, test_conn)
                
            group_pairs = pairs[i:i+group_size]
            group_count = len(group_pairs)
            group_number = i // group_size + 1
            total_groups = (total_pairs + group_size - 1) // group_size
            
            self.log(f"Grup {group_number}/{total_groups} işleniyor... ({group_count} parite)")
            
            # Her 3 grupta bir USD kurlarını yenile
            if group_number % 3 == 1:
                self.log("USD kurları yeniden yükleniyor...")
                self._load_usd_pairs()
            
            # Bu gruptaki pariteleri işle
            for j, pair in enumerate(group_pairs):
                try:
                    # Her paritede bir kısa bekleme yap
                    if j > 0:
                        time.sleep(5)  # 5 saniye bekle (daha uzun)
                        
                    # Pariteyi işle
                    success = self.process_pair(pair)
                    if success:
                        successful_pairs += 1
                        
                    # İşlenen parite sayısını artır
                    processed_pairs += 1
                    
                    # İlerleme durumunu göster (her 5 paritede bir veya grubun sonunda)
                    if j % 5 == 0 or j == group_count - 1:
                        percent = (processed_pairs / total_pairs) * 100
                        self.log(f"İlerleme: {processed_pairs}/{total_pairs} parite işlendi (%{percent:.1f}) - Başarılı: {successful_pairs}")
                        
                except Exception as e:
                    self.log(f"Parite işleme hatası: {str(e)}")
                    processed_pairs += 1
                    continue
                    
            # Grubun son paritesi işlendikten sonra bekle (son grup hariç)
            if group_number < total_groups:
                self.log(f"Grup {group_number} tamamlandı. Bağlantı havuzu yenilenmesi için {wait_time_between_groups} saniye bekleniyor...")
                time.sleep(wait_time_between_groups)
                
        # Tüm işlem tamamlandıktan sonra özet göster
        self.log(f"Forex veri toplama işlemi tamamlandı. Toplam {successful_pairs}/{total_pairs} parite başarıyla işlendi.") 