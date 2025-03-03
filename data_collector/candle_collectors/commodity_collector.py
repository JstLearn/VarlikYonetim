"""
Emtia veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta
import pandas as pd
import yfinance as yf
import warnings
import logging
import time
from utils.database import Database
from utils.config import COLLECTION_CONFIG, YAHOO_CONFIG

# Yahoo Finance sembol eşleştirmeleri
YAHOO_SYMBOLS = {
    # Ana Emtia Futures
    'GOLD/USD': 'GC=F',           # Altın Futures
    'SILVER/USD': 'SI=F',         # Gümüş Futures
    'COPPER/USD': 'HG=F',         # Bakır Futures
    'PALLADIUM/USD': 'PA=F',      # Palladyum Futures
    'PLATINUM/USD': 'PL=F',       # Platin Futures
    'NATURAL_GAS/USD': 'NG=F',    # Doğal Gaz Futures
    'BRENT_OIL/USD': 'BZ=F',      # Brent Petrol Futures
    'CRUDE_OIL/USD': 'CL=F',      # Ham Petrol Futures
    'GASOLINE_RBOB/USD': 'RB=F',  # Benzin RBOB Futures
    'HEATING_OIL/USD': 'HO=F',    # Isıtma Yağı Futures
    'US_COFFEE_C/USD': 'KC=F',    # Kahve Futures
    'US_COCOA/USD': 'CC=F',       # Kakao Futures
    'US_SUGAR/USD': 'SB=F',       # Şeker Futures
    'US_COTTON/USD': 'CT=F',      # Pamuk Futures
    'US_CORN/USD': 'ZC=F',        # Mısır Futures
    'US_WHEAT/USD': 'ZW=F',       # Buğday Futures
    'US_SOYBEAN/USD': 'ZS=F',     # Soya Fasulyesi Futures
    'LIVE_CATTLE/USD': 'LE=F',    # Canlı Sığır Futures
    'LEAN_HOGS/USD': 'HE=F',      # Domuz Futures
    'ROUGH_RICE/USD': 'ZR=F',     # Pirinç Futures
    'LUMBER/USD': 'LBS=F',        # Kereste Futures
    
    # Metaller
    'ALUMINUM/USD': 'ALI=F',      # Alüminyum Futures
    'ZINC/USD': 'ZNC=F',          # Çinko Futures
    'LEAD/USD': 'LED=F',          # Kurşun Futures
    'NICKEL/USD': 'NIC=F',        # Nikel Futures
    'TIN/USD': 'TIN=F',           # Kalay Futures
    
    # ETF Alternatifleri
    'XETRA-GOLD/EUR': 'GLD',      # SPDR Gold Shares
    'LONDON_GOLD/GBP': 'IGLN.L',  # iShares Physical Gold
    
    # Alternatif Semboller
    'CRUDE_OIL_WTI/USD': 'CL=F',  # WTI Ham Petrol
    'US_SOYBEANS/USD': 'ZS=F',    # Soya Fasulyesi
}

class CommodityCollector:
    def __init__(self):
        """Sınıf başlatıcı"""
        self.db = Database()
        self.baslangic_tarihi = datetime.strptime(
            COLLECTION_CONFIG['start_date'], 
            '%Y-%m-%d %H:%M:%S'
        ).replace(tzinfo=timezone.utc)
        self.timeout = YAHOO_CONFIG['timeout']
        self.max_retries = YAHOO_CONFIG['max_retries']
        self.retry_delay = 2  # 2 saniye bekleme süresi
        self._collect_log_mesaj = []  # Log mesajlarını saklamak için
        
        # Logging ayarları
        logging.basicConfig(level=logging.WARNING)
        self.logger = logging.getLogger('yfinance')
        self.logger.setLevel(logging.ERROR)
        
    def get_yahoo_symbol(self, symbol):
        """Yahoo Finance sembolünü döndürür"""
        return YAHOO_SYMBOLS.get(symbol, symbol)
        
    def log(self, message):
        """Zaman damgalı log mesajı yazdırır"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")
        
    def get_active_pairs(self):
        """Aktif emtia paritelerini getirir"""
        try:
            query = """
                SELECT parite, borsa, veriler_guncel, ulke, veri_var
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                WHERE tip = 'COMMODITY' 
                AND aktif = 1 
                AND (veri_var = 1 OR veri_var IS NULL)
            """
            
            results = self.db.fetch_all(query)
            if not results:
                return []
                
            pairs = []
            for row in results:
                veri_var = row[4]
                pairs.append({
                    'symbol': row[0],
                    'exchange': row[1],
                    'ulke': row[3],
                    'veri_var': veri_var  # veri_var değerini de pairs listesine ekliyoruz
                })
                
            return pairs
            
        except Exception as e:
            self.log(f"Hata: Emtia pariteleri alınamadı - {str(e)}")
            return []
            
    def collect_data(self, symbol, start_date, end_date=None):
        """Emtia verilerini önce yfinance'den, başarısız olursa investing'den toplar"""
        try:
            # Yahoo Finance sembolünü al
            yahoo_symbol = self.get_yahoo_symbol(symbol)
            self._collect_log_mesaj = []  # Log mesajlarını sıfırla
            
            if yahoo_symbol != symbol:
                self._collect_log_mesaj.append(f"Sembol: {yahoo_symbol}")
            
            # Eğer end_date belirtilmemişse, UTC+0'a göre dünün sonunu kullan
            if end_date is None:
                simdi = datetime.now(timezone.utc)
                self._collect_log_mesaj.append(f"UTC şimdi: {simdi.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # UTC+0'a göre bugünün başlangıcını bul 
                utc_bugun = simdi.replace(hour=0, minute=0, second=0, microsecond=0)
                # UTC+0'a göre dünün sonuna kadar verileri al
                end_date = (utc_bugun - timedelta(days=1)).replace(hour=23, minute=59, second=59)
                
                self._collect_log_mesaj.append(f"UTC+0 dünü: {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
            
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')  # Bitiş tarihine 1 gün ekle - yfinance end_date'i dahil etmiyor
            
            
            # Uyarıları bastır
            warnings.filterwarnings('ignore')
            self.logger.disabled = True
            
            df = pd.DataFrame()  # Boş DataFrame oluştur
            
            # 1. ADIM: Sadece bir kez yfinance'dan deneme yap
            try:
                
                result = yf.download(
                    tickers=yahoo_symbol,
                    start=start_str,
                    end=end_str,
                    interval='1d',
                    progress=False,
                    auto_adjust=True,
                    prepost=False,
                    threads=False,
                    timeout=self.timeout  # Sadece bir kez deneneceği için tek timeout
                )
                
                if result is not None and not result.empty:
                    df = result
                    self._collect_log_mesaj.append("yfinance'dan veri alındı")
                else:
                    self._collect_log_mesaj.append("yfinance'dan veri alınamadı")
                    # yfinance başarısız oldu, investing'e geçilecek
                    raise Exception("yfinance'dan veri alınamadı")
                    
            except Exception as e:
                # 2. ADIM: yfinance başarısız olduysa investing.com'u dene
                try:
                    
                    try:
                        import investpy
                    except ImportError:
                        self._collect_log_mesaj.append("investpy modülü yüklü değil. 'pip install investpy' komutu ile yükleyebilirsiniz.")
                        self._update_data_status(symbol, False)
                        return pd.DataFrame()
                    
                    # Sembolü investing.com formatına çevir
                    # Örnek: "GOLD/USD" -> "Gold" ya da benzer bir formata
                    investing_symbol = None
                    
                    if "/USD" in symbol:
                        # "/USD" içeren sembollerin başındaki kısmını al
                        commodity_name = symbol.split('/')[0]
                        
                        # Özel durum kontrolleri - Investing.com formatları
                        if commodity_name == "GOLD":
                            investing_symbol = "Gold"
                        elif commodity_name == "SILVER":
                            investing_symbol = "Silver"
                        elif commodity_name == "COPPER":
                            investing_symbol = "Copper"
                        elif commodity_name == "PALLADIUM":
                            investing_symbol = "Palladium"
                        elif commodity_name == "PLATINUM":
                            investing_symbol = "Platinum"
                        elif commodity_name == "CRUDE_OIL":
                            investing_symbol = "Crude Oil WTI"
                        elif commodity_name == "CRUDE_OIL_WTI":
                            investing_symbol = "Crude Oil WTI"
                        elif commodity_name == "BRENT_OIL":
                            investing_symbol = "Brent Oil"
                        elif commodity_name == "NATURAL_GAS":
                            investing_symbol = "Natural Gas"
                        elif commodity_name == "HEATING_OIL":
                            investing_symbol = "Heating Oil"
                        elif commodity_name == "GASOLINE_RBOB":
                            investing_symbol = "Gasoline RBOB"
                        elif commodity_name == "US_COFFEE_C":
                            investing_symbol = "Coffee"
                        elif commodity_name == "US_COCOA":
                            investing_symbol = "Cocoa"
                        elif commodity_name == "US_SUGAR":
                            investing_symbol = "Sugar"
                        elif commodity_name == "US_COTTON":
                            investing_symbol = "Cotton"
                        elif commodity_name == "US_CORN":
                            investing_symbol = "Corn"
                        elif commodity_name == "US_WHEAT":
                            investing_symbol = "Wheat"
                        elif commodity_name == "US_SOYBEAN" or commodity_name == "US_SOYBEANS":
                            investing_symbol = "Soybeans"
                        elif commodity_name == "LIVE_CATTLE":
                            investing_symbol = "Live Cattle"
                        elif commodity_name == "LEAN_HOGS":
                            investing_symbol = "Lean Hogs"
                        elif commodity_name == "ROUGH_RICE":
                            investing_symbol = "Rough Rice"
                        elif commodity_name == "LUMBER":
                            investing_symbol = "Lumber"
                        elif commodity_name == "ALUMINUM":
                            investing_symbol = "Aluminum"
                        elif commodity_name == "ZINC":
                            investing_symbol = "Zinc"
                        elif commodity_name == "LEAD":
                            investing_symbol = "Lead"
                        elif commodity_name == "NICKEL":
                            investing_symbol = "Nickel"
                        elif commodity_name == "TIN":
                            investing_symbol = "Tin"
                        # Eğer doğrudan eşleşen yoksa, log mesajı ekle
                        else:
                            self._collect_log_mesaj.append(f"Bilinmeyen emtia: {commodity_name}")
                    
                    if investing_symbol:
                        # investing.com'dan tarihleri istenen formatta ayarla
                        from_date = start_date.strftime('%d/%m/%Y')
                        to_date = end_date.strftime('%d/%m/%Y')  # Bitiş tarihi için +1 gün ekleme - investing end_date'i dahil ediyor
                        
                        self._collect_log_mesaj.append(f"Investing tarih aralığı: {from_date} -> {to_date}")
                        
                        result = investpy.get_commodity_historical_data(
                            commodity=investing_symbol,
                            from_date=from_date,
                            to_date=to_date
                        )
                        
                        if result is not None and not result.empty:
                            # Sütun isimlerini yfinance ile uyumlu hale getir
                            result = result.rename(columns={
                                'Open': 'open',
                                'High': 'high',
                                'Low': 'low',
                                'Close': 'close',
                                'Volume': 'volume' if 'Volume' in result.columns else None
                            })
                            # Volume yoksa ekle
                            if 'volume' not in result.columns or result['volume'].isnull().all():
                                result['volume'] = 0
                                
                            df = result
                            self._collect_log_mesaj.append(f"investing.com'dan '{investing_symbol}' verisi alındı ({len(df)} kayıt)")
                        else:
                            self._collect_log_mesaj.append(f"investing.com'dan '{investing_symbol}' verisi alınamadı (boş DataFrame)")
                            # Her iki kaynaktan da veri alınamadı
                            self._update_data_status(symbol, False)
                            return pd.DataFrame()
                    else:
                        self._collect_log_mesaj.append(f"Sembol dönüşümü yapılamadı: {symbol}")
                        # Sembol dönüşümü yapılamadıysa bir sonraki kaynağa geç
                        self._update_data_status(symbol, False)
                        return pd.DataFrame()
                        
                except Exception as e2:
                    self._collect_log_mesaj.append(f"investing.com hatası: {str(e2)}")
                    # Her iki kaynaktan da veri alınamadı
                    self._update_data_status(symbol, False)
                    return pd.DataFrame()
            
            # Logging'i geri aç
            self.logger.disabled = False
            
            if df.empty:
                self._collect_log_mesaj.append("Veri alınamadı")
                self._update_data_status(symbol, False)
                return df
            
            # DataFrame içinde yfinance sütunları varsa yeniden adlandır
            if 'Open' in df.columns:
                df = df.rename(columns={
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                })
            
            required_columns = ['open', 'high', 'low', 'close']
            if not all(col in df.columns for col in required_columns):
                self._collect_log_mesaj.append("Gerekli kolonlar eksik")
                self._update_data_status(symbol, False)
                return pd.DataFrame()
            
            if 'volume' not in df.columns:
                df['volume'] = 0
            
            if df[required_columns].isnull().any().any():
                self._collect_log_mesaj.append("Eksik değerler var")
                self._update_data_status(symbol, False)
                return pd.DataFrame()
            
            self._update_data_status(symbol, True)
            return df
            
        except Exception as e:
            self.log(f"{symbol} -> Hata: {str(e)}")
            self._update_data_status(symbol, False)
            return pd.DataFrame()
            
    def _update_data_status(self, symbol, has_data):
        """Emtia için veri durumunu günceller"""
        try:
            query = """
                SELECT veri_var 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                WHERE parite = ?
            """
            
            row = self.db.fetch_one(query, (symbol,))
            if row:
                mevcut_durum = row[0]
                
                # Eğer veri_var = 1 ise ve has_data = False olsa bile, veri_var değerini 1 olarak koru
                if mevcut_durum == 1 and not has_data:
                    return mevcut_durum
                
                yeni_durum = 1 if has_data else 0
                
                if mevcut_durum != yeni_durum:
                    update_query = """
                        UPDATE p
                        SET p.veri_var = ?, p.borsa = ?, p.kayit_tarihi = GETDATE()
                        FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                        WHERE p.parite = ?
                    """
                    
                    if self.db.execute_non_query(update_query, (yeni_durum, "COMMODITY", symbol)):
                        return yeni_durum
            else:
                self.log(f"{symbol} -> Veritabanında bulunamadı")
            
        except Exception as e:
            self.log(f"{symbol} -> Güncelleme hatası: {str(e)}")
            
    def get_dolar_karsiligi(self, symbol, fiyat, ulke):
        """Emtianın dolar karşılığını hesaplar"""
        return fiyat

    def save_candles(self, symbol, df, ulke):
        """Mum verilerini veritabanına kaydeder"""
        if df.empty:
            # Mevcut veri_var değerini kontrol et
            check_query = """
                SELECT veri_var 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                WHERE parite = ?
            """
            row = self.db.fetch_one(check_query, (symbol,))
            
            # Eğer veri_var zaten 1 ise, değiştirme
            if row and row[0] == 1:
                return False
            
            # Eğer veri_var 1 değilse (0 veya NULL), 0 olarak güncelle
            update_query = """
                UPDATE p
                SET p.veri_var = 0
                FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                WHERE p.parite = ?
            """

            return False
            
        try:
            kayit_sayisi = 0
            
            for tarih, row in df.iterrows():
                try:
                    fiyat = float(row['close'])
                    dolar_karsiligi = self.get_dolar_karsiligi(symbol, fiyat, ulke)
                    
                    if dolar_karsiligi is None:
                        self._collect_log_mesaj.append("Dolar karşılığı hesaplanamadı")
                        continue
                    
                    # Tarih kontrolünü DAY_HOUR olarak değil, sadece gün olarak kontrol et
                    check_query = """
                        SELECT COUNT(*) as count 
                        FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                        WHERE parite = ? 
                        AND [interval] = ? 
                        AND CONVERT(date, tarih) = CONVERT(date, ?)
                    """
                    
                    check_params = (symbol, '1d', tarih)
                    row_count = self.db.fetch_one(check_query, check_params)
                    
                    # Eğer o gün için kayıt yoksa ekle
                    if row_count and row_count[0] == 0:
                        insert_query = """
                            INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar] (
                                parite, [interval], tarih, fiyat, dolar_karsiligi, borsa, tip, ulke
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        params = (
                            symbol, '1d', tarih, fiyat, dolar_karsiligi, 'COMMODITY', 'COMMODITY', ulke
                        )
                        
                        if self.db.execute_non_query(insert_query, params):
                            kayit_sayisi += 1
                    # Eğer o gün için kayıt varsa güncelle (opsiyonel)
                    elif row_count and row_count[0] > 0:
                        # Kayıt var, güncelleme yapılmasını istiyorsanız buraya güncelleme sorgusu eklenebilir
                        # Bu örnekte güncelleme yapmıyoruz, çünkü günlük verilerin değişmemesi gerekir
                        pass
                    
                except Exception as e:
                    self._collect_log_mesaj.append(f"Kayıt hatası ({tarih}): {str(e)}")
                    continue
            
            # Veri erişimi başarılı olduğunda (df boş değil), veri_var değerini 1 olarak güncelle
            # Bu, yeni kayıt olmasa bile, veriye erişilebildiğini gösterir
            update_query = """
                UPDATE p
                SET p.veri_var = 1
                FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                WHERE p.parite = ?
            """
            if self.db.execute_non_query(update_query, (symbol,)):
                self._collect_log_mesaj.append("Parite veri durumu aktif edildi (veri_var = 1)")
                    
            if kayit_sayisi > 0:
                self._collect_log_mesaj.append(f"{kayit_sayisi} yeni kayıt")
            else:
                pass                
            if self._collect_log_mesaj:
                self.log(f"{symbol} için {kayit_sayisi} yeni kayıt eklendi")
                
            return True
            
        except Exception as e:
            self.log(f"{symbol} -> Veri kaydetme hatası: {str(e)}")
            
            # Mevcut veri_var değerini kontrol et
            check_query = """
                SELECT veri_var 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                WHERE parite = ?
            """
            row = self.db.fetch_one(check_query, (symbol,))
            
            # Eğer veri_var zaten 1 ise, değiştirme
            if row and row[0] == 1:
                return False
                
            # Hata durumunda veri_var değerini 0 yap (eğer 1 değilse)
            update_query = """
                UPDATE p
                SET p.veri_var = 0
                FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                WHERE p.parite = ?
            """
            self.db.execute_non_query(update_query, (symbol,))
            return False
            
    def run(self):
        """Tüm emtia verilerini toplar"""
        self.log("="*50)
        
        pairs = self.get_active_pairs()
        if not pairs:
            self.log("İşlenecek emtia verisi yok")
            return
            
        self.log(f"Toplam {len(pairs)} emtia işlenecek")
        
        # UTC+0 zaman dilimine göre günleri belirle
        simdi = datetime.now(timezone.utc)
        
        bugun_utc = simdi.replace(hour=0, minute=0, second=0, microsecond=0)  # UTC+0'da bugünün başlangıcı
        dun_utc = bugun_utc - timedelta(days=1)  # UTC+0'da dünün başlangıcı
        dun_sonu_utc = dun_utc.replace(hour=23, minute=59, second=59)  # UTC+0'da dünün sonu
        
        
        for pair in pairs:
            symbol = pair['symbol']
            ulke = pair['ulke']
            veri_var = pair.get('veri_var')  # veri_var değerini alıyoruz
            
            try:
                # Veritabanındaki en son tarihi al
                query = """
                    SELECT MAX(tarih) as son_tarih
                    FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH (NOLOCK)
                    WHERE parite = ?
                """
                
                row = self.db.fetch_one(query, (symbol,))
                son_tarih = row[0] if row and row[0] else None
                
                # Eğer hiç veri yoksa başlangıç tarihinden itibaren tüm verileri al
                if son_tarih is None:
                    veriler = self.collect_data(symbol, self.baslangic_tarihi, dun_sonu_utc)
                    if not veriler.empty:
                        self.save_candles(symbol, veriler, ulke)
                    continue
                
                # Son tarih varsa, UTC'de gün başlangıcını al
                son_guncelleme_gunu = son_tarih.replace(hour=0, minute=0, second=0, microsecond=0)
                
                # Son güncelleme tarihi dünden daha yeniyse (bugün veya dün), veri çekme
                if son_guncelleme_gunu.date() >= dun_utc.date():
                    continue
                    
                # Önce yfinance'den son kapanış tarihini kontrol et
                # Son güncelleme gününden sonraki günden itibaren bak
                baslangic_tarihi = son_guncelleme_gunu + timedelta(days=1)
                
                # Yfinance'den bu sembol için son durumu kontrol et
                # Yeni veri var mı yok mu önce bir bak
                yahoo_symbol = self.get_yahoo_symbol(symbol)
                
                # Gereksiz API çağrılarını önlemek için önce son mumları kontrol et
                try:
                    warnings.filterwarnings('ignore')
                    self.logger.disabled = True
                    
                    # Sadece son güncelleme gününden günümüze kadar olan kısmı kontrol et
                    # Bunun için tarih aralığını sınırlıyoruz
                    check_start = baslangic_tarihi.strftime('%Y-%m-%d')
                    check_end = (dun_sonu_utc + timedelta(days=1)).strftime('%Y-%m-%d')
                    
                    # Yfinance'den sadece son mumları kontrol et
                    check_data = yf.download(
                        tickers=yahoo_symbol,
                        start=check_start,
                        end=check_end,
                        interval="1d",
                        progress=False
                    )
                    
                    # Eğer yeni veri yoksa, güncelleme yapma
                    if check_data.empty:
                        continue
                        
                    # Eğer yeni veri varsa, en yeni tarihi kontrol et
                    son_yfinance_tarihi = check_data.index[-1]
                    
                    # Son tarih UTC saat dilimine dönüştürülüyor
                    son_tarih_utc = son_tarih.replace(tzinfo=timezone.utc)
                    
                    # Eğer yfinance'deki son tarih veritabanındaki son tarihten daha yeniyse veri al
                    if son_yfinance_tarihi.date() <= son_tarih_utc.date():
                        continue
                    
                    self.log(f"{symbol} -> Yeni veri bulundu! DB: {son_tarih.strftime('%Y-%m-%d')}, YFinance: {son_yfinance_tarihi.strftime('%Y-%m-%d')}")
                    
                    # Yeni bulunan verilerin kapanış fiyatlarını logla
                    for tarih, row in check_data.iterrows():
                        # Sadece veritabanındaki son tarihten sonraki verileri göster
                        if tarih.date() > son_tarih_utc.date():
                            tarih_str = tarih.strftime('%Y-%m-%d')
                            fiyat = float(row['Close'])
                            self.log(f"🔍 YENİ VERİ: {symbol} - {tarih_str}: Kapanış fiyatı = {fiyat}")
                    
                    # Burada yeni veri var demektir, veriyi çekip kaydet
                    veriler = self.collect_data(symbol, baslangic_tarihi, dun_sonu_utc)
                    if not veriler.empty:
                        self.save_candles(symbol, veriler, ulke)
                    
                except Exception as check_error:
                    self.log(f"{symbol} -> YFinance kontrol hatası: {str(check_error)}")
                    
                    # Hata durumunda normal akışla devam et
                    veriler = self.collect_data(symbol, baslangic_tarihi, dun_sonu_utc)
                    if not veriler.empty:
                        self.log(f"{symbol} için veri bulundu: {len(veriler)} kayıt")
                        self.save_candles(symbol, veriler, ulke)
                
            except Exception as e:
                self.log(f"{symbol} -> İşlem hatası: {str(e)}")
                continue

if __name__ == "__main__":
    collector = CommodityCollector()
    collector.run() 