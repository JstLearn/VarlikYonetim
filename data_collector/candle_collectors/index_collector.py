"""
Endeks veri toplama işlemleri
"""

import sys
import os
# Projenin ana dizinini Python yoluna ekle
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone, timedelta, date
import pandas as pd
import yfinance as yf
from utils.database import Database
from utils.config import COLLECTION_CONFIG
import time
import requests


class IndexCollector:
    def __init__(self):
        """Sınıfı başlatır"""
        self.db = Database()
        self.baslangic_tarihi = datetime(2000, 1, 1)
        
        # Rate Limiting için bekleme süresi ve yeniden deneme sayısı
        self.retry_count = 3  # En fazla 3 kez yeniden dene
        self.retry_delay = 5  # Her denemede 5 saniye bekle
        self.rate_limit_cooldown = 60  # Rate limit hatası durumunda 60 saniye bekle
        
        # Son istek zamanı - rate limiting için
        self.last_request_time = 0
        self.request_delay = 1.0  # Saniye cinsinden istekler arası bekleme süresi
        
    def log(self, message):
        """Zaman damgalı log mesajı yazdırır"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        print(f"[{timestamp}] {message}")
        
    def test_index_symbol(self, symbol):
        """Verilen endeks sembolünü YFinance ile test eder"""
        try:
            # Uyarıları gizle
            import warnings
            import yfinance as yf
            warnings.filterwarnings('ignore')
            
            # Rate Limiting için bekle
            self._wait_for_rate_limit()
            
            # Ticker nesnesini oluştur
            ticker = None
            retry_count = 0
            
            while retry_count < self.retry_count:
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.info
                    break
                except requests.exceptions.HTTPError as e:
                    # 404 hatalarını sessizce geç
                    if "404 Client Error" in str(e):
                        return False
                    elif "Too Many Requests" in str(e):
                        retry_count += 1
                        self.log(f"⚠️ Rate limit aşıldı. {self.retry_delay} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                        time.sleep(self.rate_limit_cooldown)  # Rate limit hatası durumunda daha uzun bekle
                    else:
                        self.log(f"❌ {symbol} için hata: {str(e)}")
                        return False
                except Exception as e:
                    if "Too Many Requests" in str(e):
                        retry_count += 1
                        self.log(f"⚠️ Rate limit aşıldı. {self.retry_delay} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                        time.sleep(self.rate_limit_cooldown)  # Rate limit hatası durumunda daha uzun bekle
                    else:
                        self.log(f"❌ {symbol} için hata: {str(e)}")
                        return False
                        
            if retry_count >= self.retry_count:
                self.log(f"❌ {symbol} için maksimum yeniden deneme sayısına ulaşıldı.")
                return False
            
            # Endeks bilgilerini kontrol et
            if info and isinstance(info, dict):
                if 'shortName' in info or 'longName' in info:
                    name = info.get('shortName', info.get('longName', 'Unknown'))
                    
                    # Son 5 günlük veriyi çekmeyi dene
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=5)
                    
                    # Rate Limiting için bekle
                    self._wait_for_rate_limit()
                    
                    retry_count = 0
                    while retry_count < self.retry_count:
                        try:
                            hist = ticker.history(start=start_date, end=end_date)
                            break
                        except requests.exceptions.HTTPError as e:
                            # 404 hatalarını sessizce geç
                            if "404 Client Error" in str(e):
                                return False
                            elif "Too Many Requests" in str(e):
                                retry_count += 1
                                self.log(f"⚠️ Rate limit aşıldı. {self.retry_delay} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                                time.sleep(self.rate_limit_cooldown)
                            else:
                                self.log(f"❌ {symbol} için hata: {str(e)}")
                                return False
                        except Exception as e:
                            if "Too Many Requests" in str(e):
                                retry_count += 1
                                self.log(f"⚠️ Rate limit aşıldı. {self.retry_delay} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                                time.sleep(self.rate_limit_cooldown)
                            else:
                                self.log(f"❌ {symbol} için hata: {str(e)}")
                                return False
                    
                    if retry_count >= self.retry_count:
                        self.log(f"❌ {symbol} için maksimum yeniden deneme sayısına ulaşıldı.")
                        return False
                    
                    if not hist.empty and 'Close' in hist.columns:
                        exchange = info.get('exchange', 'Unknown')
                        currency = info.get('currency', 'Unknown')
                        lastPrice = info.get('regularMarketPrice', info.get('previousClose', 'Unknown'))
                        
                        self.log(f"✅ {symbol} - {name} geçerli bir endeks sembolü")
                        self.log(f"   Borsa: {exchange}, Para Birimi: {currency}, Son Fiyat: {lastPrice}")
                        self.log(f"   Tarih Aralığı: {hist.index[0]} - {hist.index[-1]}")
                        return True
            
            return False
        except Exception as e:
            return False
        
    def get_country_code(self, country_name):
        """Ülke adına göre Yahoo Finance için ülke kodunu döndürür"""
        country_codes = {
            # Kuzey Amerika
            'USA': '',  # ABD endeksleri için genellikle uzantı kullanılmaz
            'United States': '',
            'US': '',
            'Canada': 'TO',  # Toronto
            'Mexico': 'MX',
            
            # Güney Amerika
            'Brazil': 'SA',  # Sao Paulo
            'Argentina': 'BA',  # Buenos Aires
            'Chile': 'SN',  # Santiago
            'Colombia': 'CL',  # Colombia
            'Peru': 'LM',  # Lima
            
            # Avrupa
            'UK': 'L',  # Londra
            'United Kingdom': 'L',
            'Germany': 'DE',  # Frankfurt
            'France': 'PA',  # Paris
            'Italy': 'MI',  # Milan
            'Spain': 'MC',  # Madrid
            'Portugal': 'LS',  # Lisbon
            'Switzerland': 'SW',  # Switzerland
            'Netherlands': 'AS',  # Amsterdam
            'Belgium': 'BR',  # Brussels
            'Austria': 'VI',  # Vienna
            'Greece': 'AT',  # Athens
            'Sweden': 'ST',  # Stockholm
            'Norway': 'OL',  # Oslo
            'Denmark': 'CO',  # Copenhagen
            'Finland': 'HE',  # Helsinki
            'Ireland': 'IR',  # Ireland
            'Poland': 'WA',  # Warsaw
            'Turkey': 'IS',  # Istanbul
            'Türkiye': 'IS',  # Istanbul
            'Russia': 'ME',  # Moscow
            
            # Asya-Pasifik
            'Japan': 'T',  # Tokyo
            'China': 'SS',  # Shanghai
            'Hong Kong': 'HK',
            'Taiwan': 'TW',  # Taiwan
            'South Korea': 'KS',  # Korea
            'Singapore': 'SI',
            'Malaysia': 'KL',  # Kuala Lumpur
            'Indonesia': 'JK',  # Jakarta
            'Thailand': 'BK',  # Bangkok
            'Philippines': 'PS',  # Philippines
            'Vietnam': 'VN',
            'India': 'NS',  # NSE (National Stock Exchange)
            'Pakistan': 'KA',  # Karachi
            'Australia': 'AX',  # Australia
            'New Zealand': 'NZ',
            
            # Ortadoğu ve Afrika
            'Israel': 'TA',  # Tel Aviv
            'Saudi Arabia': 'SR',  # Saudi
            'UAE': 'AD',  # Abu Dhabi
            'Qatar': 'QA',  # Qatar
            'Kuwait': 'KW',
            'Egypt': 'CA',  # Cairo
            'South Africa': 'JO',  # Johannesburg
            'Nigeria': 'LG',  # Lagos
            'Kenya': 'NR'  # Nairobi
        }
        
        # Ülke adı büyük küçük harf duyarlı olmadan eşleşme yap
        for country, code in country_codes.items():
            if country.lower() == country_name.lower():
                return code
                
        # Eşleşme bulunamazsa None döndür
        return None
        
    def update_symbol_in_db(self, parite, symbol):
        """Veritabanında sembol bilgisini günceller"""
        try:
            conn = self.db.connect()
            cursor = conn.cursor()
            
            # Endeks sembolleri tablosunu oluştur (yoksa)
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'endeks_semboller')
                BEGIN
                    CREATE TABLE [VARLIK_YONETIM].[dbo].[endeks_semboller] (
                        parite VARCHAR(50) NOT NULL,
                        yf_symbol VARCHAR(50) NOT NULL,
                        son_kontrol DATETIME DEFAULT GETDATE(),
                        PRIMARY KEY (parite)
                    )
                END
            """)
            
            # Sembol bilgisini güncelle
            cursor.execute("""
                IF EXISTS (SELECT 1 FROM [VARLIK_YONETIM].[dbo].[endeks_semboller] WHERE parite = ?)
                BEGIN
                    UPDATE [VARLIK_YONETIM].[dbo].[endeks_semboller]
                    SET yf_symbol = ?,
                        son_kontrol = GETDATE()
                    WHERE parite = ?
                END
                ELSE
                BEGIN
                    INSERT INTO [VARLIK_YONETIM].[dbo].[endeks_semboller]
                        (parite, yf_symbol)
                    VALUES
                        (?, ?)
                END
            """, (parite, symbol, parite, parite, symbol))
            
            conn.commit()
            self.log(f"✅ {parite} için {symbol} sembolü kaydedildi")
            return True
        except Exception as e:
            self.log(f"❌ {parite} sembol güncellemesi başarısız: {str(e)}")
            if 'conn' in locals() and conn:
                conn.rollback()
            return False
        finally:
            if 'conn' in locals() and conn:
                conn.close()
        
    def get_common_indices_symbols(self):
        """Yaygın endekslerin sembollerini döndürür"""
        return {
            # ABD Endeksleri
            'SPX': ['^GSPC', 'SPX', 'SP500', 'S&P500', '.SPX', '.INX'],
            'DJI': ['^DJI', 'DJI', 'DJIA', '.DJI'],
            'IXIC': ['^IXIC', 'COMP', 'NASDAQ', '.IXIC'],
            'RUT': ['^RUT', 'RUT', 'RUSSELL', '.RUT'],
            'NYA': ['^NYA', 'NYA', 'NY-COMPOSITE'],
            'VIX': ['^VIX', 'VIX', 'VOLATILITY'],
            'OEX': ['^OEX', 'OEX', 'S&P100'],
            'MID': ['^MID', 'MDY', 'S&P400'],
            'GSPTSE': ['^GSPTSE', 'GSPTSE', 'TSX'],
            'NDX': ['^NDX', 'NDX', 'NASDAQ100'],
            'DWCF': ['^DWCF', 'RUSSELL-3000'],
            'DJT': ['^DJT', 'DJT', 'DOW-TRANSPORT'],
            'DJU': ['^DJU', 'DJU', 'DOW-UTILITIES'],
            'RUI': ['^RUI', 'RUI', 'RUSSELL-1000'],
            'SOX': ['^SOX', 'SOX', 'SEMICONDUCTOR'],
            
            # Türkiye Endeksleri
            'BIST100': ['^XU100', 'XU100.IS', 'BIST100.IS', '.XU100'],
            'BIST30': ['^XU030', 'XU030.IS', 'BIST30.IS', '.XU030'],
            'BIST50': ['^XU050', 'XU050.IS', 'BIST50.IS'],
            'BISTBANK': ['^XBANK', 'XBANK.IS'],
            'BISTMALI': ['^XUMAL', 'XUMAL.IS'],
            'BISTSANAYI': ['^XUSIN', 'XUSIN.IS'],
            'BISTULAŞIM': ['^XULAS', 'XULAS.IS'],
            'BISTHIZMET': ['^XUHIZ', 'XUHIZ.IS'],
            'BISTTEKNOLOJI': ['^XUTEK', 'XUTEK.IS'],
            'BISTTICARET': ['^XTCRT', 'XTCRT.IS'],
            'BISTMETAL': ['^XMESY', 'XMESY.IS'],
            'BISTGIDA': ['^XGIDA', 'XGIDA.IS'],
            'BISTENERJI': ['^XELKT', 'XELKT.IS'],
            'BISTKIMYA': ['^XKMYA', 'XKMYA.IS'],
            'BISTTEKSTIL': ['^XTEKS', 'XTEKS.IS'],
            'XUTUM': ['^XUTUM', 'XUTUM.IS'], # BIST TÜM
            
            # İngiltere Endeksleri
            'FTSE100': ['^FTSE', 'UKX', 'FTSE.L', '.FTSE'],
            'FTSE': ['^FTSE', 'UKX', 'FTSE.L', '.FTSE'],
            'FTSE250': ['^FTMC', 'MCX', 'FTMC.L'],
            'FTSEAIM': ['^FTAI', 'AXX', 'FTAI.L'],
            'FTSE350': ['^FTLC', 'NMX', 'FTLC.L'],
            'FTSEALLSHARE': ['^FTAS', 'ASX', 'FTAS.L'],
            
            # Almanya Endeksleri
            'DAX': ['^GDAXI', 'DAX', 'DAX.DE', '.GDAXI'],
            'MDAX': ['^MDAXI', 'MDAX.DE', 'MDAXI.DE'],
            'SDAX': ['^SDAXI', 'SDAX.DE', 'SDAXI.DE'],
            'TECDAX': ['^TECDAX', 'TECDAX.DE'],
            'HDAX': ['^HDAX', 'HDAX.DE'],
            'CDAX': ['^CDAX', 'CDAX.DE'],
            
            # Fransa Endeksleri
            'CAC40': ['^FCHI', 'CAC', 'CAC.PA', '.FCHI'],
            'CAC': ['^FCHI', 'CAC', 'CAC.PA', '.FCHI'],
            'SBF120': ['^SBF120', 'SBF120.PA'],
            'CAC NEXT20': ['^CN20', 'CN20.PA'],
            'CAC MID60': ['^CACMI', 'CACMI.PA'],
            'CAC SMALL': ['^CACS', 'CACS.PA'],
            
            # İtalya Endeksleri
            'FTSEMIB': ['^FTSEMIB', 'FTSEMIB.MI', 'FTMIB.MI'],
            
            # İspanya Endeksleri
            'IBEX35': ['^IBEX', 'IBEX', 'IBEX.MC'],
            
            # İsviçre Endeksleri
            'SMI': ['^SSMI', 'SMI', 'SMI.SW'],
            
            # Hollanda Endeksleri
            'AEX': ['^AEX', 'AEX', 'AEX.AS'],
            
            # Belçika Endeksleri
            'BEL20': ['^BFX', 'BFX', 'BFX.BR'],
            
            # İsveç Endeksleri
            'OMXS30': ['^OMX', 'OMXS30', 'OMXS30.ST'],
            
            # Japonya Endeksleri
            'N225': ['^N225', 'NKY', 'N225.T', '.N225'],
            'TOPIX': ['^TOPX', 'TPX', 'TOPX.T'],
            'JPX400': ['^JPXN', 'JPX400.T'],
            'NIKKEI500': ['^N500', 'N500.T'],
            
            # Hong Kong Endeksleri
            'HSI': ['^HSI', 'HSI', 'HSI.HK', '.HSI'],
            'HSCEI': ['^HSCE', 'HSCE', 'HSCE.HK'],
            
            # Çin Endeksleri
            'SSEC': ['^SSEC', 'SSEC', 'SHCOMP', '000001.SS', '.SSEC'],
            'SZSC': ['^SZSC', 'SZSC', 'SZCOMP', '399001.SZ', '.SZSC'],
            'CSI300': ['^CSI300', 'SHSZ300.CI', 'CSI300.SS', '000300.SS', '.CSI300'],
            'CSI500': ['^CSI500', 'CSI500.SS', '000905.SS'],
            'CSI1000': ['^CSI1000', 'CSI1000.SS', '000852.SS'],
            'FTXIN9': ['^FTXIN9', 'FTXIN9.SS'], # FTSE China A50
            'SHSZ300': ['^CSI300', 'SHSZ300.CI', 'CSI300.SS', '000300.SS'],
            'HSCE': ['^HSCE', 'HSCE.HK', '.HSCE'],
            'SHCOMP': ['^SSEC', 'SSEC', 'SHCOMP', '000001.SS', '.SHCOMP'],
            
            # Tayvan Endeksleri
            'TWII': ['^TWII', 'TWII.TW', '.TWII'],
            'TAIEX': ['^TWII', 'TWII.TW', '.TWII'],
            
            # Güney Kore Endeksleri
            'KOSPI': ['^KS11', 'KOSPI', 'KS11.KS', '.KS11', '.KOSPI'],
            'KS200': ['^KS200', 'KS200', 'KS200.KS', '.KS200'],
            'KOSDAQ': ['^KQ11', 'KOSDAQ', 'KQ11.KS'],
            
            # Hindistan Endeksleri
            'SENSEX': ['^BSESN', 'SENSEX.BO', 'SENSEX', '.BSESN'],
            'NIFTY': ['^NSEI', 'NIFTY.NS', 'NIFTY', '.NSEI'],
            'NIFTYNEXT50': ['^NSMIDCP', 'NIFMID50.NS', '.NSMIDCP'],
            'BANKNIFTY': ['^NSEBANK', 'BANKNIFTY.NS'],
            'NIFTY500': ['^CRSLDX', 'NIFTY500.NS'],
            'NIFTYMIDCAP': ['^NIFMDCP', 'NIFTYMID.NS'],
            'NIFTYSMALLCAP': ['^NIFSMCP', 'NIFTYSML.NS'],
            
            # Avustralya Endeksleri 
            'AXJO': ['^AXJO', 'AS51', 'XJO.AX', '.AXJO'],
            'ASX200': ['^AXJO', 'AS51', 'XJO.AX', '.AXJO'],
            'AORD': ['^AORD', 'AORD', 'XAO.AX', '.AORD'],
            'ASX300': ['^AXKO', 'XKO.AX'],
            'ASX20': ['^ATLI', 'XTL.AX'],
            'ASX50': ['^AFLI', 'XFL.AX'],
            'ASX100': ['^ATOI', 'XTO.AX'],
            
            # Yeni Zelanda Endeksleri
            'NZ50': ['^NZ50', 'NZ50.NZ', '.NZ50'],
            
            # Kanada Endeksleri 
            'TSX': ['^GSPTSE', 'SPTSX', 'GSPTSE.TO', '.GSPTSE'],
            'SPTSX60': ['^TX60', 'TX60.TO'],
            
            # Brezilya Endeksleri
            'BVSP': ['^BVSP', 'IBOV', 'BVSP.SA', '.BVSP'],
            'IBOVESPA': ['^BVSP', 'IBOV', 'BVSP.SA', '.BVSP'],
            
            # Meksika Endeksleri
            'MXX': ['^MXX', 'MEXBOL', 'MXX.MX', '.MXX'],
            'IPC': ['^MXX', 'MEXBOL', 'MXX.MX', '.MXX'],
            
            # Arjantin Endeksleri
            'MERV': ['^MERV', 'MERV.BA', '.MERV'],
            
            # Şili Endeksleri
            'IPSA': ['^IPSA', 'IPSA.SN', '.IPSA'],
            
            # Singapur Endeksleri 
            'STI': ['^STI', 'STI.SI', '.STI'],
            
            # Malezya Endeksleri
            'KLSE': ['^KLSE', 'FBMKLCI.KL', '.KLSE'],
            
            # Endonezya Endeksleri
            'JKSE': ['^JKSE', 'JKSE.JK', '.JKSE'],
            
            # Tayland Endeksleri
            'SET': ['^SET.BK', 'SET.BK', '.SET'],
            
            # Vietnam Endeksleri
            'VNI': ['^VNINDEX', 'VNINDEX.VN', '.VNI'],
            'HNX': ['^HASTC', 'HASTC.VN', 'HNX.VN'],
            
            # Filipinler Endeksleri
            'PSI': ['^PSI', 'PSEI.PS', '.PSI'],
            
            # Suudi Arabistan Endeksleri
            'TASI': ['^TASI', 'TASI.SR', '.TASI'],
            
            # BAE Endeksleri
            'ADI': ['^ADI', 'ADI.AD', '.ADI'],
            'DFMGI': ['^DFMGI', 'DFMGI.DU', '.DFMGI'],
            
            # Katar Endeksleri
            'QSI': ['^QSI', 'QSI.QA', '.QSI'],
            
            # Kuveyt Endeksleri
            'KWSE': ['^KWSE', 'KWSE.KW', '.KWSE'],
            
            # Mısır Endeksleri
            'EGX30': ['^EGX30', 'EGX30.CA', '.EGX30'],
            
            # Güney Afrika Endeksleri
            'JTOPI': ['^JTOPI', 'JTOPI.JO', '.JTOPI'],
            'TOP40': ['^JTOPI', 'JTOPI.JO', '.JTOPI'],
            
            # Nijerya Endeksleri
            'NGSE': ['^NGSE', 'NGSE.LG', '.NGSE'],
            
            # İsrail Endeksleri
            'TA35': ['^TA35', 'TA35.TA', '.TA35'],
            'TA125': ['^TA125', 'TA125.TA', '.TA125'],
            
            # Rusya Endeksleri
            'IMOEX': ['^IMOEX', 'IMOEX.ME'],
            'RTSI': ['^RTSI', 'RTSI.ME'],
            
            # Polonya Endeksleri
            'WIG20': ['^WIG20', 'WIG20.WA'],
            'WIG30': ['^WIG30', 'WIG30.WA'],
            
            # Uluslararası Endeksler
            'FTEU3': ['^STOXX', 'STOXX50E', '.STOXX'],
            'STOXX50': ['^STOXX50E', 'STOXX50E', '.STOXX50E'],
            'STOXX600': ['^STOXX600', 'STOXX600E', '.STOXX600'],
            'EUROSTOXX50': ['^STOXX50E', 'SX5E', '.STOXX50E'],
            'EUROSTOXX': ['^STOXX', 'SXXP', '.STOXX'],
            'MSCI_WORLD': ['^MSCI', 'MSCI.WORLD'],
            'MSCI_EMERGING': ['^MSCI.EM', 'MSCI.EM'],
            'MSCI_EUROPE': ['^MSCI.EU', 'MSCI.EU'],
            'MSCI_ACWI': ['^MSCI.ACWI', 'MSCI.ACWI'],
            'MSCI_EAFE': ['^MSCI.EAFE', 'MSCI.EAFE'],
            'MSCI_ASIA': ['^MSCI.ASIA', 'MSCI.ASIA'],
            'MSCI_LATAM': ['^MSCI.LATAM', 'MSCI.LATAM'],
            'FTSE_GLOBAL_ALL_CAP': ['^FTGAC', 'FTGAC']
        }
        
    def _wait_for_rate_limit(self):
        """Rate limiting için bekler"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        
        if elapsed < self.request_delay:
            sleep_time = self.request_delay - elapsed
            time.sleep(sleep_time)
            
        self.last_request_time = time.time()
        
    def collect_data(self, parite, symbol, para_birimi, ulke=None):
        """Endeks verisini toplar ve veritabanına kaydeder"""
        try:
            self.log(f"ℹ️ {parite} ({symbol}) endeks verisi toplanıyor...")
            
            # Tarihleri belirle
            end_date = datetime.now()
            start_date = self.baslangic_tarihi
            
            # Veritabanı bağlantısı
            conn = self.db.connect()
            cursor = conn.cursor()
            
            # Son veri tarihini kontrol et
            cursor.execute("""
                SELECT TOP 1 tarih 
                FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH(NOLOCK)
                WHERE parite = ? AND [interval] = '1d'
                ORDER BY tarih DESC
            """, (parite,))
            
            son_veri = cursor.fetchone()
            
            if son_veri:
                son_tarih = son_veri[0]
                bugun = datetime.now().date()
                
                # Eğer son tarih bugünse, güncelleme gerekmiyor
                if (bugun - son_tarih.date()).days == 0:
                    self.log(f"✅ {parite} verileri zaten güncel: {son_tarih}")
                    return 0
                    
                # Son tarihten itibaren veri al
                start_date = son_tarih + timedelta(days=1)
            
            # yfinance ile veri çekme
            self.log(f"ℹ️ {parite} verileri çekiliyor... (Tarih: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')})")
            
            try:
                # Rate limiting için bekle
                self._wait_for_rate_limit()
                
                # Veriyi çek - rate limiting için yeniden deneme mekanizması
                retry_count = 0
                data = None
                
                while retry_count < self.retry_count:
                    try:
                        # Veriyi çek
                        data = yf.download(
                            symbol,
                            start=start_date.strftime('%Y-%m-%d'),
                            end=end_date.strftime('%Y-%m-%d'),
                            interval="1d",
                            progress=False,
                            show_errors=False
                        )
                        break
                    except requests.exceptions.HTTPError as e:
                        # 404 hatalarını sessizce geç
                        if "404 Client Error" in str(e):
                            break
                        elif "Too Many Requests" in str(e):
                            retry_count += 1
                            self.log(f"⚠️ Rate limit aşıldı. {self.retry_delay} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                            time.sleep(self.rate_limit_cooldown)
                        else:
                            self.log(f"❌ {symbol} için hata: {str(e)}")
                            retry_count = self.retry_count  # Döngüden çıkmak için
                    except Exception as e:
                        if "Too Many Requests" in str(e):
                            retry_count += 1
                            self.log(f"⚠️ Rate limit aşıldı. {self.retry_delay} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                            time.sleep(self.rate_limit_cooldown)
                        else:
                            self.log(f"❌ {symbol} için hata: {str(e)}")
                            retry_count = self.retry_count  # Döngüden çıkmak için
                
                if retry_count >= self.retry_count:
                    self.log(f"❌ {parite} için maksimum yeniden deneme sayısına ulaşıldı.")
                    return 0
                
                if data is None or data.empty:
                    self.log(f"⚠️ {parite} için veri bulunamadı")
                    return 0
                    
                # Veriyi hazırla
                data.reset_index(inplace=True)
                
                # Tarih sütununu kontrol et
                if 'Date' not in data.columns:
                    self.log(f"❌ {parite} - Veri formatında hata (tarih sütunu bulunamadı)")
                    return 0
                
                self.log(f"✅ {parite} için {len(data)} adet veri çekildi")
                
                # Verileri veritabanına kaydet
                kayit_sayisi = 0
                
                # Basit bir transaction başlat
                cursor.execute("BEGIN TRANSACTION")
                
                # Veri var bilgisini güncelle
                cursor.execute("""
                    UPDATE [VARLIK_YONETIM].[dbo].[pariteler]
                    SET veri_var = 1, 
                        kayit_tarihi = GETDATE()
                    WHERE parite = ?
                """, (parite,))
                
                # Yoksa sembolü kaydet
                self.update_symbol_in_db(parite, symbol)
                
                # Her bir kayıt için
                for _, row in data.iterrows():
                    tarih = row['Date']
                    acilis = row['Open']
                    yuksek = row['High']
                    dusuk = row['Low']
                    kapanis = row['Close']
                    hacim = row['Volume'] if 'Volume' in row else 0
                    
                    # Nan değerleri kontrol et
                    if (pd.isna(acilis) or pd.isna(yuksek) or 
                        pd.isna(dusuk) or pd.isna(kapanis)):
                        continue
                    
                    # Tarih UTC formatına çevir
                    if isinstance(tarih, str):
                        tarih = datetime.strptime(tarih, '%Y-%m-%d')
                    
                    # Veriyi kaydet
                    cursor.execute("""
                        IF NOT EXISTS (
                            SELECT 1 
                            FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH(NOLOCK)
                            WHERE parite = ? AND [interval] = '1d' AND tarih = ?
                        )
                        BEGIN
                            INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar]
                                (parite, [interval], tarih, acilis, yuksek, dusuk, kapanis, hacim, kaynak)
                            VALUES
                                (?, '1d', ?, ?, ?, ?, ?, ?, 'YAHOO')
                        END
                        ELSE
                        BEGIN
                            UPDATE [VARLIK_YONETIM].[dbo].[kurlar]
                            SET acilis = ?,
                                yuksek = ?,
                                dusuk = ?,
                                kapanis = ?,
                                hacim = ?,
                                kaynak = 'YAHOO'
                            WHERE parite = ? AND [interval] = '1d' AND tarih = ?
                        END
                    """, (
                        parite, tarih,
                        parite, tarih, acilis, yuksek, dusuk, kapanis, hacim,
                        acilis, yuksek, dusuk, kapanis, hacim,
                        parite, tarih
                    ))
                    
                    kayit_sayisi += 1
                
                # İşlemi tamamla
                conn.commit()
                self.log(f"✅ {parite} için {kayit_sayisi} yeni kayıt eklendi")
                
                return kayit_sayisi
            except Exception as e:
                self.log(f"❌ {parite} veri toplama hatası: {str(e)}")
                return 0
        except Exception as e:
            self.log(f"❌ {parite} veri toplama hatası: {str(e)}")
            return 0
                
    def run(self):
        """Endeks verilerini toplar ve veri tabanına kaydeder."""
        try:
            # Veritabanı bağlantısı
            conn = self.db.connect()
            cursor = conn.cursor()
            
            # Tüm endeksleri al
            cursor.execute("""
                SELECT parite, aktif, borsa, tip, ulke, aciklama 
                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH(NOLOCK)
                WHERE tip = 'INDEX' AND aktif = 1 
                AND (veri_var = 1 OR veri_var IS NULL)
                ORDER BY parite
            """)
            
            indices = cursor.fetchall()
            total_indices = len(indices)
            
            self.log(f"Toplam {total_indices} endeks için işlem yapılacak.")
            
            # Sonuç istatistikleri
            processed = 0  # İşlenen
            updated = 0    # Güncellenen
            skipped = 0    # Atlanan
            errors = 0     # Hatalı
            
            # Veritabanı bağlantı durumunu kontrol eden fonksiyon
            def check_and_reconnect():
                nonlocal conn, cursor
                try:
                    # Bağlantı durumunu kontrol et
                    cursor.execute("SELECT 1")
                    return True
                except Exception:
                    try:
                        # Bağlantıyı yeniden aç
                        self.log("⚠️ Veritabanı bağlantısı yenileniyor...")
                        conn = self.db.connect()
                        cursor = conn.cursor()
                        return True
                    except Exception as e:
                        self.log(f"❌ Veritabanı bağlantısı kurulamadı: {str(e)}")
                        return False
            
            # Rate limit kontrol için son istek sayacı ve zamanlayıcı
            request_count = 0
            last_reset_time = time.time()
            batch_size = 25  # Her 25 istek sonrası uzun bekleme
            
            # İşleme başla
            for i, (parite, aktif, borsa, tip, ulke, aciklama) in enumerate(indices, 1):
                try:
                    # Her 100 endeks sonrasında bağlantıyı kontrol et
                    if i % 100 == 0 and not check_and_reconnect():
                        self.log("❌ Veritabanı bağlantısı sağlanamadığı için işlem durduruluyor.")
                        break
                    
                    # Rate limit kontrolü - her batch_size istekte bir uzun bekleme
                    request_count += 1
                    if request_count >= batch_size:
                        cooldown_time = 60  # 1 dakika bekle
                        self.log(f"⚠️ Rate limiting için {cooldown_time} saniye bekleniyor...")
                        time.sleep(cooldown_time)
                        request_count = 0
                        last_reset_time = time.time()
                    
                    self.log(f"İşleniyor: {i}/{total_indices} - {parite}")
                    
                    # Endeks için veri topla - şu anlık orijinal metodu kullanıyoruz,
                    # daha sonra parite parametresini kullanacak şekilde güncelleyeceğiz
                    try:
                        # Bağlantının açık olduğundan emin ol
                        if not check_and_reconnect():
                            self.log(f"⚠️ {parite} için veritabanı bağlantısı kurulamadı. Geçiliyor.")
                            errors += 1
                            continue
                            
                        # Tarihleri belirle
                        end_date = datetime.now()
                        # Son tarihi kontrol et
                        
                        # Son veri tarihini kontrol et
                        cursor.execute("""
                            SELECT TOP 1 tarih 
                            FROM [VARLIK_YONETIM].[dbo].[kurlar] WITH(NOLOCK)
                            WHERE parite = ? AND [interval] = '1d'
                            ORDER BY tarih DESC
                        """, (parite,))
                        
                        row = cursor.fetchone()
                        
                        if row:
                            son_tarih = row[0]
                            bugun = datetime.now().date()
                            
                            # Eğer son tarih bugünse veya dünse, güncelleme yok
                            if (bugun - son_tarih.date()).days <= 1:
                                self.log(f"✅ {parite} zaten güncel. Son veri tarihi: {son_tarih}")
                                skipped += 1
                                continue
                            
                            # Son tarihten günümüze kadar veri çek
                            start_date = son_tarih + timedelta(days=1)
                        else:
                            # Hiç veri yoksa, başlangıç tarihinden itibaren çek
                            start_date = self.baslangic_tarihi
                            
                        # Tarihleri string formatına çevir
                        start_str = start_date.strftime('%Y-%m-%d')
                        end_str = end_date.strftime('%Y-%m-%d')
                                                
                        # 1. ADIM: yfinance'den veriyi al - şu anlık sembol tespitini atla
                        # Veritabanında sembol olup olmadığını kontrol et
                        cursor.execute("""
                            IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'endeks_semboller')
                            BEGIN
                                SELECT yf_symbol 
                                FROM [VARLIK_YONETIM].[dbo].[endeks_semboller] WITH(NOLOCK)
                                WHERE parite = ?
                            END
                            ELSE
                            BEGIN
                                SELECT NULL as yf_symbol
                            END
                        """, (parite,))
                        
                        symbol_row = cursor.fetchone()
                        
                        if symbol_row and symbol_row[0]:
                            # Veritabanında kayıtlı sembol varsa, onu kullan
                            yf_symbol = symbol_row[0]
                            self.log(f"{parite} için veritabanında kayıtlı sembol: {yf_symbol}")
                            
                            # Tüm uyarıları bastır
                            import warnings
                            warnings.filterwarnings('ignore')
                            
                            # yfinance'den veri çek
                            # Rate limiting için bekle
                            self._wait_for_rate_limit()
                            
                            # Veriyi çek - rate limiting için yeniden deneme mekanizması
                            retry_count = 0
                            df = None
                            
                            while retry_count < self.retry_count:
                                try:
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
                                    break
                                except requests.exceptions.HTTPError as e:
                                    # 404 hatalarını sessizce geç
                                    if "404 Client Error" in str(e):
                                        break
                                    elif "Too Many Requests" in str(e):
                                        retry_count += 1
                                        self.log(f"⚠️ Rate limit aşıldı. {self.retry_delay} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                                        time.sleep(self.rate_limit_cooldown)
                                    else:
                                        self.log(f"❌ {parite} için hata: {str(e)}")
                                        retry_count = self.retry_count  # Döngüden çıkmak için
                                except Exception as e:
                                    if "Too Many Requests" in str(e):
                                        retry_count += 1
                                        self.log(f"⚠️ Rate limit aşıldı. {self.retry_delay} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                                        time.sleep(self.rate_limit_cooldown)
                                    else:
                                        self.log(f"❌ {parite} için hata: {str(e)}")
                                        retry_count = self.retry_count  # Döngüden çıkmak için
                            
                            if retry_count >= self.retry_count:
                                self.log(f"❌ {parite} için maksimum yeniden deneme sayısına ulaşıldı.")
                                errors += 1
                                continue
                        
                            if not df.empty and 'Close' in df.columns:
                                # DataFrame'i düzenle
                                df = df.rename(columns={
                                    'Open': 'open',
                                    'High': 'high',
                                    'Low': 'low',
                                    'Close': 'close',
                                    'Volume': 'volume'
                                })
                                
                                # Kurlar tablosuna kaydet
                                cursor.execute("""
                                    SET NOCOUNT ON
                                    DECLARE @kayit_sayisi INT = 0
                                """)

                                # BEGIN TRY/CATCH bloklarını kaldırıp normal işlem yapıyoruz
                                cursor.execute("BEGIN TRANSACTION")
                                
                                for tarih, row in df.iterrows():
                                    fiyat = float(row['close'])
                                    dolar_karsiligi = fiyat  # Şimdilik aynı değer
                                    
                                    cursor.execute("""
                                        INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar] (parite, [interval], tarih, fiyat, dolar_karsiligi, borsa, tip, ulke)
                                        SELECT ?, ?, ?, ?, ?, ?, ?, ?
                                        WHERE NOT EXISTS (
                                            SELECT 1 FROM [VARLIK_YONETIM].[dbo].[kurlar]
                                            WHERE parite = ? AND [interval] = ? AND tarih = ?
                                        )
                                    """, 
                                    (parite, '1d', tarih, fiyat, dolar_karsiligi, 'INDEX', 'INDEX', ulke, 
                                    parite, '1d', tarih))
                                    
                                    # @kayit_sayisi'ni artır
                                    if cursor.rowcount > 0:
                                        kayit_sayisi = kayit_sayisi + 1 if 'kayit_sayisi' in locals() else 1
                                
                                # Veri başarıyla kaydedildi, veri_var'ı 1 olarak güncelle
                                cursor.execute("""
                                    UPDATE [VARLIK_YONETIM].[dbo].[pariteler]
                                    SET veri_var = 1, 
                                        kayit_tarihi = GETDATE()
                                    WHERE parite = ?
                                """, (parite,))
                                
                                # İşlemi tamamla
                                try:
                                    conn.commit()
                                    kayit_sayisi = kayit_sayisi if 'kayit_sayisi' in locals() else 0
                                    self.log(f"✅ {parite} için {kayit_sayisi} yeni kayıt eklendi")
                                    updated += 1
                                except Exception as e:
                                    conn.rollback()
                                    self.log(f"❌ {parite} için veri kaydedilirken hata: {str(e)}")
                                    errors += 1
                        
                        # Veritabanında sembol yok, sembol tespit etmeye çalış
                        self.log(f"⚠️ {parite} için veritabanında sembol bulunamadı, sembol tespit ediliyor...")
                        
                        found_symbol = False
                        
                        # 1. Adım: Yaygın endeksler içinde ara
                        common_indices = self.get_common_indices_symbols()
                        if parite in common_indices:
                            for test_symbol in common_indices[parite]:
                                if self.test_index_symbol(test_symbol):
                                    # Doğru sembol bulundu, veritabanına kaydet
                                    self.update_symbol_in_db(parite, test_symbol)
                                    self.log(f"✅ {parite} için sembol tespit edildi: {test_symbol}")
                                    
                                    # Rate limiting için bekle
                                    self._wait_for_rate_limit()
                                    
                                    # Veriyi indir
                                    retry_count = 0
                                    df = None
                                    
                                    while retry_count < self.retry_count:
                                        try:
                                            df = yf.download(
                                                tickers=test_symbol,
                                                start=start_str,
                                                end=end_str,
                                                interval='1d',
                                                progress=False,
                                                auto_adjust=True,
                                                prepost=False,
                                                threads=False
                                            )
                                            break
                                        except Exception as e:
                                            if "Too Many Requests" in str(e):
                                                retry_count += 1
                                                self.log(f"⚠️ Rate limit aşıldı. {self.rate_limit_cooldown} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                                                time.sleep(self.rate_limit_cooldown)
                                            else:
                                                raise e
                                    
                                    if retry_count >= self.retry_count:
                                        self.log(f"❌ {parite} için maksimum yeniden deneme sayısına ulaşıldı.")
                                        continue
                                    
                                    if not df.empty and 'Close' in df.columns:
                                        # DataFrame'i düzenle
                                        df = df.rename(columns={
                                            'Open': 'open',
                                            'High': 'high',
                                            'Low': 'low',
                                            'Close': 'close',
                                            'Volume': 'volume'
                                        })
                                        
                                        # Kurlar tablosuna kaydet
                                        cursor.execute("""
                                            SET NOCOUNT ON
                                            DECLARE @kayit_sayisi INT = 0
                                        """)

                                        # BEGIN TRY/CATCH bloklarını kaldırıp normal işlem yapıyoruz
                                        cursor.execute("BEGIN TRANSACTION")
                                        
                                        for tarih, row in df.iterrows():
                                            fiyat = float(row['close'])
                                            dolar_karsiligi = fiyat  # Şimdilik aynı değer
                                            
                                            cursor.execute("""
                                                INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar] (parite, [interval], tarih, fiyat, dolar_karsiligi, borsa, tip, ulke)
                                                SELECT ?, ?, ?, ?, ?, ?, ?, ?
                                                WHERE NOT EXISTS (
                                                    SELECT 1 FROM [VARLIK_YONETIM].[dbo].[kurlar]
                                                    WHERE parite = ? AND [interval] = ? AND tarih = ?
                                                )
                                            """, 
                                            (parite, '1d', tarih, fiyat, dolar_karsiligi, 'INDEX', 'INDEX', ulke, 
                                            parite, '1d', tarih))
                                            
                                            # @kayit_sayisi'ni artır
                                            if cursor.rowcount > 0:
                                                kayit_sayisi = kayit_sayisi + 1 if 'kayit_sayisi' in locals() else 1
                                        
                                        # Veri başarıyla kaydedildi, veri_var'ı 1 olarak güncelle
                                        cursor.execute("""
                                            UPDATE [VARLIK_YONETIM].[dbo].[pariteler]
                                            SET veri_var = 1, 
                                                kayit_tarihi = GETDATE()
                                            WHERE parite = ?
                                        """, (parite,))
                                        
                                        # İşlemi tamamla
                                        try:
                                            conn.commit()
                                            kayit_sayisi = kayit_sayisi if 'kayit_sayisi' in locals() else 0
                                            self.log(f"✅ {parite} için {kayit_sayisi} yeni kayıt eklendi")
                                            updated += 1
                                        except Exception as e:
                                            conn.rollback()
                                            self.log(f"❌ {parite} için veri kaydedilirken hata: {str(e)}")
                                            errors += 1
                                    
                                    found_symbol = True
                                    break
                            
                            if found_symbol:
                                continue
                        
                        # 2. Adım: Ülke kodu ekleyerek dene
                        if ulke:
                            country_code = self.get_country_code(ulke)
                            if country_code:
                                # Farklı ülke kodlarını kullanarak sembol oluştur ve dene
                                country_symbols = []
                                
                                # Temel sembol
                                country_symbols.append(f"{parite}.{country_code}")
                                
                                # Şaretler ile sembol
                                if not parite.startswith('^'):
                                    country_symbols.append(f"^{parite}")
                                    country_symbols.append(f"^{parite}.{country_code}")
                                
                                # Nokta ile sembol
                                if not parite.startswith('.'):
                                    country_symbols.append(f".{parite}")
                                    country_symbols.append(f".{parite}.{country_code}")
                                
                                for country_symbol in country_symbols:
                                    if self.test_index_symbol(country_symbol):
                                        # Doğru sembol bulundu, veritabanına kaydet
                                        self.update_symbol_in_db(parite, country_symbol)
                                        self.log(f"✅ {parite} için ülke kodlu sembol tespit edildi: {country_symbol}")
                                        
                                        # Rate limiting için bekle 
                                        self._wait_for_rate_limit()
                                        
                                        # Veriyi indir
                                        retry_count = 0
                                        df = None
                                        
                                        while retry_count < self.retry_count:
                                            try:
                                                df = yf.download(
                                                    tickers=country_symbol,
                                                    start=start_str,
                                                    end=end_str,
                                                    interval='1d',
                                                    progress=False,
                                                    auto_adjust=True,
                                                    prepost=False,
                                                    threads=False
                                                )
                                                break
                                            except Exception as e:
                                                if "Too Many Requests" in str(e):
                                                    retry_count += 1
                                                    self.log(f"⚠️ Rate limit aşıldı. {self.rate_limit_cooldown} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                                                    time.sleep(self.rate_limit_cooldown)
                                                else:
                                                    raise e
                                        
                                        if retry_count >= self.retry_count:
                                            self.log(f"❌ {parite} için maksimum yeniden deneme sayısına ulaşıldı.")
                                            continue
                                    
                                    found_symbol = True
                                    break
                                
                                if found_symbol:
                                    continue
                        
                        # Hala bulunamadıysa, direkt sembolü yfinance formatını dene
                        try:
                            # Sembolü direkt olarak dene
                            direct_symbols = [
                                parite,
                                f"^{parite}",
                                f".{parite}"
                            ]
                            
                            for direct_symbol in direct_symbols:
                                if self.test_index_symbol(direct_symbol):
                                    # Doğru sembol bulundu, veritabanına kaydet
                                    self.update_symbol_in_db(parite, direct_symbol)
                                    self.log(f"✅ {parite} için doğrudan sembol tespit edildi: {direct_symbol}")
                                    
                                    # Rate limiting için bekle
                                    self._wait_for_rate_limit()
                                    
                                    # Veriyi indir
                                    retry_count = 0
                                    df = None
                                    
                                    while retry_count < self.retry_count:
                                        try:
                                            df = yf.download(
                                                tickers=direct_symbol,
                                                start=start_str,
                                                end=end_str,
                                                interval='1d',
                                                progress=False,
                                                auto_adjust=True,
                                                prepost=False,
                                                threads=False
                                            )
                                            break
                                        except requests.exceptions.HTTPError as e:
                                            # 404 hatalarını sessizce geç
                                            if "404 Client Error" in str(e):
                                                break
                                            elif "Too Many Requests" in str(e):
                                                retry_count += 1
                                                self.log(f"⚠️ Rate limit aşıldı. {self.retry_delay} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                                                time.sleep(self.rate_limit_cooldown)
                                            else:
                                                self.log(f"❌ {parite} için hata: {str(e)}")
                                                retry_count = self.retry_count  # Döngüden çıkmak için
                                        except Exception as e:
                                            if "Too Many Requests" in str(e):
                                                retry_count += 1
                                                self.log(f"⚠️ Rate limit aşıldı. {self.retry_delay} saniye bekleniyor ({retry_count}/{self.retry_count})...")
                                                time.sleep(self.rate_limit_cooldown)
                                            else:
                                                self.log(f"❌ {parite} için hata: {str(e)}")
                                                retry_count = self.retry_count  # Döngüden çıkmak için
                                    
                                    if retry_count >= self.retry_count:
                                        self.log(f"❌ {parite} için maksimum yeniden deneme sayısına ulaşıldı.")
                                        continue
                                
                                    if not df.empty and 'Close' in df.columns:
                                        # DataFrame'i düzenle
                                        df = df.rename(columns={
                                            'Open': 'open',
                                            'High': 'high',
                                            'Low': 'low',
                                            'Close': 'close',
                                            'Volume': 'volume'
                                        })
                                        
                                        # Kurlar tablosuna kaydet
                                        cursor.execute("""
                                            SET NOCOUNT ON
                                            DECLARE @kayit_sayisi INT = 0
                                        """)

                                        # BEGIN TRY/CATCH bloklarını kaldırıp normal işlem yapıyoruz
                                        cursor.execute("BEGIN TRANSACTION")
                                        
                                        for tarih, row in df.iterrows():
                                            fiyat = float(row['close'])
                                            dolar_karsiligi = fiyat  # Şimdilik aynı değer
                                            
                                            cursor.execute("""
                                                INSERT INTO [VARLIK_YONETIM].[dbo].[kurlar] (parite, [interval], tarih, fiyat, dolar_karsiligi, borsa, tip, ulke)
                                                SELECT ?, ?, ?, ?, ?, ?, ?, ?
                                                WHERE NOT EXISTS (
                                                    SELECT 1 FROM [VARLIK_YONETIM].[dbo].[kurlar]
                                                    WHERE parite = ? AND [interval] = ? AND tarih = ?
                                                )
                                            """, 
                                            (parite, '1d', tarih, fiyat, dolar_karsiligi, 'INDEX', 'INDEX', ulke, 
                                            parite, '1d', tarih))
                                            
                                            # @kayit_sayisi'ni artır
                                            if cursor.rowcount > 0:
                                                kayit_sayisi = kayit_sayisi + 1 if 'kayit_sayisi' in locals() else 1
                                        
                                        # Veri başarıyla kaydedildi, veri_var'ı 1 olarak güncelle
                                        cursor.execute("""
                                            UPDATE [VARLIK_YONETIM].[dbo].[pariteler]
                                            SET veri_var = 1, 
                                                kayit_tarihi = GETDATE()
                                            WHERE parite = ?
                                        """, (parite,))
                                        
                                        # İşlemi tamamla
                                        try:
                                            conn.commit()
                                            kayit_sayisi = kayit_sayisi if 'kayit_sayisi' in locals() else 0
                                            self.log(f"✅ {parite} için {kayit_sayisi} yeni kayıt eklendi")
                                            updated += 1
                                        except Exception as e:
                                            conn.rollback()
                                            self.log(f"❌ {parite} için veri kaydedilirken hata: {str(e)}")
                                            errors += 1
                                    
                                    found_symbol = True
                                    break
                            
                            if found_symbol:
                                continue
                        except Exception as e:
                            self.log(f"⚠️ {parite} sembol denemesi sırasında hata: {str(e)}")
                        
                        # Ya sembol yoksa ya da veri çekilemedi
                        self.log(f"❌ {parite} için veri bulunamadı veya sembol mevcut değil")
                        errors += 1
                        
                        # Bağlantı kontrolü
                        if not check_and_reconnect():
                            self.log(f"⚠️ {parite} için veritabanı bağlantısı kurulamadı. Devam edilemiyor.")
                            continue
                            
                        # Veri_var değerini güncelle - 0 olarak
                        cursor.execute("""
                            UPDATE [VARLIK_YONETIM].[dbo].[pariteler]
                            SET veri_var = 0
                            WHERE parite = ?
                        """, (parite,))
                        conn.commit()
                    except Exception as e:
                        self.log(f"❌ {parite} veri toplama hatası: {str(e)}")
                        errors += 1
                        
                    processed += 1
                    
                except Exception as e:
                    self.log(f"❌ {parite} işlenirken hata: {str(e)}")
                    errors += 1
                    
            self.log(f"Endeks toplama tamamlandı. Toplam: {total_indices}, İşlenen: {processed}, Atlanan: {skipped}, Güncellenen: {updated}, Hata: {errors}")
            
        except Exception as e:
            self.log(f"Endeks veri toplama işleminde hata: {str(e)}")
        finally:
            try:
                # Bağlantıyı kapat
                if 'conn' in locals() and conn:
                    conn.close()
            except Exception:
                pass
                
                
if __name__ == "__main__":
    # IndexCollector sınıfını oluştur ve çalıştır
    try:
        collector = IndexCollector()
        collector.run()
    except KeyboardInterrupt:
        print("\nKullanıcı tarafından durduruldu.")
        sys.exit(0)
    except Exception as e:
        print(f"\nProgram hatası: {str(e)}")
        sys.exit(1)