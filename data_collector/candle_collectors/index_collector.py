"""
Endeks veri toplama işlemleri
"""

from datetime import datetime, timezone, timedelta, date
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
        
    def get_yfinance_symbol(self, symbol, ulke=None):
        """Veritabanı sembolünü yfinance'in beklediği formata dönüştürür"""
        # Eğer sembol bir / içeriyorsa, / öncesini al
        base_symbol = symbol.split('/')[0] if '/' in symbol else symbol
        
        # Temel format dönüşümleri - yfinance'in genel beklentileri
        if base_symbol.startswith('.'):
            # Nokta ile başlayan sembolleri ^ ile değiştir
            return f"^{base_symbol[1:]}"
        elif ulke == 'Turkey' and not base_symbol.endswith('.IS'):
            # Türkiye endeksleri için .IS eki ekle
            return f"{base_symbol}.IS"
        elif ulke == 'UK' and not base_symbol.endswith('.L'):
            # İngiltere endeksleri için .L eki ekle
            return f"{base_symbol}.L"
        
        # Diğer durumlarda sembolü olduğu gibi kullan
        return base_symbol
        
    def get_investing_symbol(self, symbol, ulke=None):
        """Veritabanı sembolünü investing.com'un beklediği formata dönüştürür"""
        # Eğer sembol bir / içeriyorsa, / öncesini al
        base_symbol = symbol.split('/')[0] if '/' in symbol else symbol
        
        # Basit format dönüşümleri
        # Nokta ile başlayan sembolleri düzelt
        if base_symbol.startswith('.'):
            return base_symbol[1:]
            
        # Ülke bazlı ek kaldırmaları
        if ulke == 'Turkey' and base_symbol.endswith('.IS'):
            return base_symbol[:-3] # .IS ekini kaldır
            
        if ulke == 'UK' and base_symbol.endswith('.L'):
            return base_symbol[:-2] # .L ekini kaldır
            
        return base_symbol
        
    def run(self):
        """Çalışma metodu"""
        self.log("="*50)
        
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
                
            # Aktif çiftleri al - veri_var NULL veya 1 olanları getir
            cursor.execute("""
                SELECT parite, ulke, ISNULL(veri_var, 0) as veri_var, ISNULL(CONVERT(DATE, kayit_tarihi), '1900-01-01') as kayit_tarihi
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH(NOLOCK)
                WHERE aktif = 1 AND tip = 'INDEX' AND (veri_var = 1 OR veri_var IS NULL)
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
            
    def update_data_status(self, symbol, has_data, conn=None, cursor=None):
        """Endeks için veri durumunu günceller"""
        local_conn = False
        try:
            # Bağlantı yoksa yeni bir bağlantı oluştur
            if conn is None or cursor is None:
                conn = self.db.connect()
                cursor = conn.cursor()
                local_conn = True
                
            # Veri durumunu güncelle
            # MSSQL için ? parametrelerini kullan
            query = """
                UPDATE [VARLIK_YONETIM].[dbo].[pariteler]
                SET veri_var = ?, kayit_tarihi = GETDATE()
                WHERE parite = ?
            """
            cursor.execute(query, (1 if has_data else 0, symbol))
            conn.commit()
            
            # Log
            self.log(f"{symbol} için veri_var = {1 if has_data else 0} olarak güncellendi")
            
        except Exception as e:
            if conn:
                conn.rollback()
            self.log(f"Veri durumu güncellenirken hata: {str(e)}")
        finally:
            # Yerel bağlantıyı kapat
            if local_conn and conn:
                cursor.close()
                conn.close()
                    
    def collect_data(self, symbol, ulke, start_date, end_date=None, conn=None, cursor=None):
        """Endeks verilerini yfinance ve investing.com'dan toplar"""
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
            elif isinstance(end_date, str):
                end_date = datetime.strptime(end_date, '%Y-%m-%d')
                # Eğer saat bilgisi yoksa, günün sonunu al
                if end_date.hour == 0 and end_date.minute == 0 and end_date.second == 0:
                    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
                end_date = end_date.replace(tzinfo=timezone.utc)
                
            # Tarihleri string formatına çevir
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')  # yfinance bitiş tarihini dahil etmiyor, +1 gün ekle
            
            df = None
            
            # 1. ADIM: yfinance'dan veri almayı dene
            # Farklı yfinance sembol formatlarını deneyeceğiz
            yfinance_deneme_sembolleri = []
            
            # 1. İlk temel formatta sembol ekle
            base_symbol = symbol.split('/')[0] if '/' in symbol else symbol
            yfinance_deneme_sembolleri.append(base_symbol)
            
            # 2. Dönüştürülmüş sembolü ekle
            yf_symbol = self.get_yfinance_symbol(symbol, ulke)
            if yf_symbol != base_symbol and yf_symbol not in yfinance_deneme_sembolleri:
                yfinance_deneme_sembolleri.append(yf_symbol)
            
            # 3. Genel bilinen dönüşümleri ekle
            if base_symbol == 'SPX':
                yfinance_deneme_sembolleri.append('^GSPC')
            elif base_symbol == 'DJI':
                yfinance_deneme_sembolleri.append('^DJI')
            elif base_symbol == 'IXIC':
                yfinance_deneme_sembolleri.append('^IXIC')
            elif base_symbol == 'BIST100':
                yfinance_deneme_sembolleri.append('^XU100')
            elif base_symbol == 'BIST30':
                yfinance_deneme_sembolleri.append('^XU030')
            elif base_symbol == 'DAX':
                yfinance_deneme_sembolleri.append('^GDAXI')
            elif base_symbol == 'FTSE':
                yfinance_deneme_sembolleri.append('^FTSE')
            elif base_symbol == 'N225':
                yfinance_deneme_sembolleri.append('^N225')
            
            # 4. Nokta ile başlayan sembolleri ^ ile değiştir
            if base_symbol.startswith('.') and f"^{base_symbol[1:]}" not in yfinance_deneme_sembolleri:
                yfinance_deneme_sembolleri.append(f"^{base_symbol[1:]}")
            
            # Yfinance sembollerini dene
            yf_success = False
            yf_used_symbol = None
            
            for deneme_symbol in yfinance_deneme_sembolleri:
                try:
                    # Tüm uyarıları bastır
                    import warnings
                    import sys
                    import io
                    
                    # Özellikle yfinance'in auto_adjust uyarısını filtreleme
                    warnings.filterwarnings('ignore', category=UserWarning)
                    warnings.filterwarnings('ignore', message='.*auto_adjust.*')
                    
                    # stdout ve stderr'i geçici olarak yönlendir
                    old_stdout = sys.stdout
                    old_stderr = sys.stderr
                    sys.stdout = io.StringIO()
                    sys.stderr = io.StringIO()
                    
                    try:
                        # yfinance'den veri çek
                        df = yf.download(
                            tickers=deneme_symbol,
                            start=start_str,
                            end=end_str,
                            interval='1d',
                            progress=False,
                            auto_adjust=True,
                            prepost=False,
                            threads=False
                        )
                    finally:
                        # stdout ve stderr'i eski haline getir
                        sys.stdout = old_stdout
                        sys.stderr = old_stderr
                    
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
                        
                        yf_success = True
                        yf_used_symbol = deneme_symbol
                        break
                except Exception:
                    continue
            
            # Yfinance başarılı olduysa veya değilse log
            format_str = ', '.join(yfinance_deneme_sembolleri)
            if yf_success and yf_used_symbol:
                # Veritabanına kaydet
                result = self.save_candles(symbol, df, ulke, conn, cursor)
                
                # Başarılı ise True döndür
                if result:
                    self.log(f"[yfinance] {symbol} için {yf_used_symbol} formatında veri bulundu ({len(df)} kayıt)")
                    # Veri bulundu ve kaydedildi - run metodu dışarıdan çağırmışsa buradan return
                    return True
                return False
            else:
                self.log(f"[yfinance] {symbol} için {format_str} formatlarında denedim veri bulamadım")
            
            # 2. ADIM: investing.com'dan veri almayı dene
            try:
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
                
                # Investing.com için sembol formatları hazırla
                investing_deneme_sembolleri = []
                
                # 1. İlk olarak temel sembolü ekle
                investing_symbol = self.get_investing_symbol(symbol, ulke)
                investing_deneme_sembolleri.append(investing_symbol)
                
                # 2. Genel bilinen dönüşümleri ekle
                if base_symbol == 'SPX':
                    investing_deneme_sembolleri.append('S&P 500')
                elif base_symbol == 'DJI':
                    investing_deneme_sembolleri.append('Dow 30')
                elif base_symbol == 'IXIC':
                    investing_deneme_sembolleri.append('Nasdaq')
                elif base_symbol == 'BIST100':
                    investing_deneme_sembolleri.append('BIST 100')
                elif base_symbol == 'BIST30':
                    investing_deneme_sembolleri.append('BIST 30')
                elif base_symbol == 'DAX':
                    investing_deneme_sembolleri.append('DAX 30')
                    investing_deneme_sembolleri.append('DAX')
                elif base_symbol == 'FTSE':
                    investing_deneme_sembolleri.append('FTSE 100')
                elif base_symbol == 'N225':
                    investing_deneme_sembolleri.append('Nikkei 225')
                
                # Her bir deneme sembolü için döngü
                invest_success = False
                invest_used_symbol = None
                
                # Direkt sorgu denemesi
                for investing_symbol in investing_deneme_sembolleri:
                    if country:
                        try:
                            # İlk olarak ülke ile direkt sorgulama yap
                            historical_data = investpy.indices.get_index_historical_data(
                                index=investing_symbol,
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
                                        invest_success = True
                                        invest_used_symbol = investing_symbol
                                        
                                        # Veritabanına kaydet
                                        result = self.save_candles(symbol, historical_data, ulke, conn, cursor)
                                        
                                        # Başarılı ise True döndür
                                        if result:
                                            self.log(f"[investing] {symbol} için {investing_symbol} formatında veri bulundu ({len(historical_data)} kayıt)")
                                            # Veri bulundu ve kaydedildi
                                            return True
                        except Exception:
                            continue
                
                # Sembol adı arama ile dene
                if not invest_success:
                    try:
                        for search_term in investing_deneme_sembolleri:
                            try:
                                # Endeks araması yap
                                search_results = investpy.search_indices(
                                    by='name',
                                    value=search_term
                                )
                                
                                if not search_results.empty:
                                    # İlk bulunan endeksi kullan
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
                                                invest_success = True
                                                invest_used_symbol = found_name
                                                
                                                # Veritabanına kaydet
                                                result = self.save_candles(symbol, historical_data, ulke, conn, cursor)
                                                
                                                # Başarılı ise True döndür
                                                if result:
                                                    self.log(f"[investing] {symbol} için {found_name} formatında veri bulundu ({len(historical_data)} kayıt)")
                                                    # Veri bulundu ve kaydedildi
                                                    return True
                            except Exception:
                                continue
                    except Exception:
                        pass
                
                # Investing.com başarısız log
                format_str = ', '.join(investing_deneme_sembolleri)
                if not invest_success:
                    self.log(f"[investing] {symbol} için {format_str} formatlarında denedim veri bulamadım")
                
            except Exception:
                self.log(f"[investing] {symbol} için {investing_symbol} formatında denedim veri bulamadım")
            
            # Her iki API de başarısız oldu
            # Veri bulunamadı, veri_var = 0 olarak güncelle ve sadece bir kez log yap
            try:
                self.update_data_status(symbol, False, conn, cursor)
            except Exception:
                # Hata olursa ekstra log yapmaya gerek yok, zaten update_data_status içinde log var
                pass
            
            return False
            
        except Exception as e:
            self.log(f"❌ {symbol} için veri toplama hatası: {str(e)}")
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
            
            # Borsa bilgisini al - birkaç yaygın sembolü dene
            exchange = "INDEX"  # Varsayılan değer
            
            try:
                # Uyarıları bastır
                import warnings
                import sys
                import io
                
                warnings.filterwarnings('ignore')
                old_stdout = sys.stdout
                old_stderr = sys.stderr
                sys.stdout = io.StringIO()
                sys.stderr = io.StringIO()
                
                try:
                    # Farklı sembol formatlarını deneyelim
                    ticker_deneme_sembolleri = []
                    base_symbol = symbol.split('/')[0] if '/' in symbol else symbol
                    
                    # 1. Temel sembol
                    ticker_deneme_sembolleri.append(base_symbol)
                    
                    # 2. Yfinance sembolü
                    yf_symbol = self.get_yfinance_symbol(symbol, ulke)
                    if yf_symbol not in ticker_deneme_sembolleri:
                        ticker_deneme_sembolleri.append(yf_symbol)
                    
                    # 3. Özel semboller
                    if base_symbol == 'SPX':
                        ticker_deneme_sembolleri.append('^GSPC')
                    elif base_symbol == 'DJI':
                        ticker_deneme_sembolleri.append('^DJI')
                    elif base_symbol == 'IXIC':
                        ticker_deneme_sembolleri.append('^IXIC')
                    
                    for deneme_symbol in ticker_deneme_sembolleri:
                        try:
                            # yfinance'den borsa bilgisini sorgula
                            ticker = yf.Ticker(deneme_symbol)
                            info = ticker.info
                            
                            if info and 'exchange' in info:
                                exchange = info['exchange'].upper()
                                self.log(f"{symbol} için borsa bilgisi bulundu: {exchange} (yfinance: {deneme_symbol})")
                                break
                        except:
                            continue
                finally:
                    # stdout ve stderr'i eski haline getir
                    sys.stdout = old_stdout
                    sys.stderr = old_stderr
            except Exception as e:
                self.log(f"{symbol} için borsa bilgisi alınamadı: {str(e)}")
            
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