"""
Hisse senedi veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
from utils.database import Database
from utils.config import COLLECTION_CONFIG

class StockCollector:
    def __init__(self):
        self.db = Database()
        self.baslangic_tarihi = datetime.strptime(COLLECTION_CONFIG['start_date'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        
    def log(self, message):
        """Zaman damgalı log mesajı yazdırır"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")
        
    def get_active_pairs(self):
        """Aktif hisse senetlerini getirir"""
        try:
            conn = self.db.connect()
            if not conn:
                return []
                
            cursor = conn.cursor()
            cursor.execute("""
                SELECT parite, borsa, veriler_guncel, ulke 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                WHERE tip = 'STOCK'
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
                self.log(f"Toplam {len(pairs)} hisse senedi işlenecek")
                
            return pairs
            
        except Exception as e:
            self.log(f"Hata: Hisse senetleri alınamadı - {str(e)}")
            return []
            
    def get_stock_suffix(self, symbol):
        """Hisse senedi için doğru borsa suffix'ini belirler"""
        try:
            # Önce suffix olmadan dene
            stock = yf.Ticker(symbol)
            info = stock.info
            
            if not info:
                return None
                
            exchange = info.get("exchange", "").upper()
            market = info.get("market", "").upper()
            
            self.log(f"yfinance: {symbol} için borsa bilgisi -> Exchange: {exchange}, Market: {market}")
            
            # Exchange bilgisinden suffix'i çıkar
            # Örnek: "IST" -> ".IS", "BUE" -> ".BA" gibi
            if exchange:
                # Exchange adının son kısmını al (örn: "ISTANBUL" -> "IST")
                exchange_code = exchange.split()[-1]
                if exchange_code:
                    # Exchange kodunun ilk harfi ile suffix oluştur
                    suffix = f".{exchange_code[0]}"
                    if len(exchange_code) > 1:
                        # İkinci harf varsa ekle (örn: "IST" -> ".IS")
                        suffix += exchange_code[1]
                    return suffix
            
            return None
            
        except Exception as e:
            self.log(f"Suffix belirleme hatası ({symbol}): {str(e)}")
            return None
            
    def collect_data(self, symbol, start_date, end_date=None, ulke=None):
        """Hisse senedi verilerini toplar"""
        yf_error = None
        inv_error = None
        df = None
        
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
            
        # Beklenen gün sayısını hesapla
        beklenen_gun = (end_date.date() - start_date.date()).days + 1
        self.log(f"{symbol} için {start_date.date()} - {end_date.date()} arası veri toplanıyor ({beklenen_gun} gün)")
            
        # Önce yfinance'den dene
        try:
            # Para birimi kısmını kaldır (ABC/USD -> ABC)
            yf_symbol = symbol.split('/')[0]
            
            # Borsa suffix'ini belirle
            suffix = self.get_stock_suffix(yf_symbol)
            if suffix:
                yf_symbol += suffix
                self.log(f"yfinance: {yf_symbol} sembolü deneniyor...")
            else:
                self.log(f"yfinance: {yf_symbol} için borsa suffix'i bulunamadı, suffix'siz deneniyor...")
            
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
                auto_adjust=True,
                prepost=False,
                threads=False
            )
            
            # Hata mesajını al
            error_output = sys.stderr.getvalue()
            sys.stderr = stderr
            
            if "1 Failed download" in error_output:
                yf_error = error_output.split(']:')[1].strip() if ']:' in error_output else error_output
                df = None
            
            if df is not None and not df.empty:
                gelen_gun = len(df)
                self.log(f"yfinance: {symbol} için {gelen_gun} günlük veri alındı")
                if gelen_gun < beklenen_gun:
                    self.log(f"yfinance: Beklenen {beklenen_gun} gün, alınan {gelen_gun} gün")
                return df, True
                
        except Exception as e:
            if not yf_error:
                yf_error = str(e)
            
        # yfinance'den alınamadıysa investing.com'dan dene
        try:
            import investpy
            import time
            
            # Para birimi ve sembolü ayır
            symbol_parts = symbol.split('/')
            stock_symbol = symbol_parts[0]
            
            # Investing.com için tarihleri ayarla (dd/mm/yyyy formatında)
            start_str = start_date.strftime('%d/%m/%Y')
            end_str = end_date.strftime('%d/%m/%Y')
            
            max_retries = 3
            retry_delay = 5  # saniye
            
            for retry in range(max_retries):
                try:
                    # Investing.com'dan veri çek
                    df = investpy.get_stock_historical_data(
                        stock=stock_symbol,
                        country=ulke,
                        from_date=start_str,
                        to_date=end_str
                    )
                    
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        gelen_gun = len(df)
                        self.log(f"investing: {symbol} için {gelen_gun} günlük veri alındı")
                        if gelen_gun < beklenen_gun:
                            self.log(f"investing: Beklenen {beklenen_gun} gün, alınan {gelen_gun} gün")
                        return df, True
                        
                except Exception as retry_error:
                    if "ERR#0015" in str(retry_error) and retry < max_retries - 1:
                        # Rate limit hatası, bekle ve tekrar dene
                        self.log(f"investing: Rate limit hatası, {retry_delay} saniye bekleniyor...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Her denemede bekleme süresini 2 katına çıkar
                        continue
                    raise  # Diğer hataları veya son denemeyi yukarı fırlat
                
        except Exception as e:
            inv_error = str(e)
            
        # Her iki kaynaktan da veri alınamadıysa hataları logla
        if "stock" in str(inv_error).lower() or "YFTzMissingError" in str(yf_error):
            # Bilinen hatalar, sessizce geç
            pass
        elif "ERR#0015" in str(inv_error):
            # Rate limit hatası
            self.log(f"investing: {symbol} için rate limit hatası")
        else:
            # Beklenmeyen hatalar
            if yf_error:
                self.log(f"yfinance: {symbol} denendi -> {yf_error}")
            if inv_error:
                self.log(f"investing: {symbol} denendi -> {inv_error}")
            
        # Her iki kaynaktan da veri alınamadıysa veri_var'ı 0 yap
        conn = None
        try:
            conn = self.db.connect()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE p
                    SET p.veri_var = ?
                    FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                    WHERE p.parite = ?
                """, (0, symbol))
                conn.commit()
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
                    
        return pd.DataFrame(), False
            
    def _update_data_status(self, symbol, has_data):
        """Hisse senedi için veri durumunu günceller"""
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
                UPDATE p
                SET p.veri_var = ?
                FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                WHERE p.parite = ?
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
                
    def get_dolar_karsiligi(self, symbol, fiyat, ulke):
        """Hisse senedinin dolar karşılığını hesaplar"""
        # Para birimini al (ABC/USD -> USD)
        currency = symbol.split('/')[1]
        
        if currency == 'USD':  # Direkt USD karşılığı
            return fiyat
            
        try:
            conn = self.db.connect()
            if not conn:
                return None
                
            cursor = conn.cursor()
            
            # Önce CURRENCY/USD formatında ara
            cursor.execute("""
                SELECT TOP 1 fiyat
                FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                WHERE parite = ? AND borsa = 'FOREX'
                ORDER BY tarih DESC
            """, (f"{currency}/USD",))
            
            row = cursor.fetchone()
            if row:
                currency_usd = float(row[0])
                return fiyat * currency_usd
                
            # Bulunamazsa USD/CURRENCY formatında ara ve tersini al
            cursor.execute("""
                SELECT TOP 1 fiyat
                FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                WHERE parite = ? AND borsa = 'FOREX'
                ORDER BY tarih DESC
            """, (f"USD/{currency}",))
            
            row = cursor.fetchone()
            if row:
                currency_usd = float(row[0])
                return fiyat * (1 / currency_usd)
                
        except Exception as e:
            self.log(f"Dolar karşılığı hesaplama hatası ({symbol}): {str(e)}")
            
        return None

    def save_candles(self, symbol, df, ulke):
        """Mum verilerini veritabanına kaydeder"""
        if df.empty:
            return False
            
        try:
            # Borsa bilgisini al
            yf_symbol = symbol.split('/')[0]
            stock = yf.Ticker(yf_symbol)
            info = stock.info
            exchange = info.get("exchange", "STOCK").upper() if info else "STOCK"
            
            conn = self.db.connect()
            if not conn:
                return False
                
            cursor = conn.cursor()
            kayit_sayisi = 0
            
            for tarih, row in df.iterrows():
                try:
                    fiyat = float(row['Close'])
                    dolar_karsiligi = self.get_dolar_karsiligi(symbol, fiyat, ulke)
                    
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
                        (symbol, '1d', tarih, fiyat, dolar_karsiligi, exchange, 'STOCK', ulke))
                        kayit_sayisi += 1
                    
                except Exception as e:
                    self.log(f"SQL Kayıt hatası ({symbol}, {tarih}): {str(e)}")
                    self.log(f"Hata detayı: {e.__class__.__name__}: {str(e)}")
                    self.log(f"Değerler: parite={symbol}, interval=1d, tarih={tarih}, fiyat={fiyat}, dolar={dolar_karsiligi}, borsa={exchange}, tip=STOCK, ulke={ulke}")
                    continue
                    
            conn.commit()
            
            if kayit_sayisi > 0:
                self.log(f"{symbol} için {kayit_sayisi} yeni kayıt eklendi")
                # Veri başarıyla kaydedildi, veri_var'ı 1 yap ve borsa bilgisini güncelle
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
            self.log(f"Hata detayı: {e.__class__.__name__}: {str(e)}")
            return False
            
    def run(self):
        """Tüm hisse senedi verilerini toplar"""
        self.log("="*50)
        
        pairs = self.get_active_pairs()
        if not pairs:
            self.log("İşlenecek hisse senedi verisi yok")
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
                    FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                    WHERE parite = ?
                """, (symbol,))
                
                row = cursor.fetchone()
                son_tarih = row[0] if row and row[0] else None
                
                # Bağlantıyı kapat
                cursor.close()
                conn.close()
                
                # Bugünün tarihini al
                simdi = datetime.now()
                
                if son_tarih is None:
                    # Hiç veri yoksa başlangıç tarihinden itibaren al
                    baslangic = self.baslangic_tarihi
                    if baslangic.date() > simdi.date():
                        baslangic = simdi
                    veriler, has_data = self.collect_data(symbol, baslangic, simdi, ulke)
                    if has_data:
                        self.save_candles(symbol, veriler, ulke)
                else:
                    # Son tarihten sonraki verileri al
                    son_guncelleme = datetime.combine(son_tarih.date(), datetime.min.time())
                    
                    if son_guncelleme.date() < simdi.date():
                        baslangic = son_guncelleme + timedelta(days=1)
                        if baslangic.date() > simdi.date():
                            baslangic = simdi
                        veriler, has_data = self.collect_data(symbol, baslangic, simdi, ulke)
                        if has_data:
                            self.save_candles(symbol, veriler, ulke)
                    else:
                        continue
                
            except Exception as e:
                self.log(f"İşlem hatası ({symbol}): {str(e)}")
                continue 