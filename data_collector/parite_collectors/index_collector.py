"""
TradingView üzerinden borsa endekslerini toplayan sınıf
"""

import requests
from bs4 import BeautifulSoup, Tag
from utils.database import Database
import logging
import time
import random
from datetime import datetime

# Logger ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('index_collector')

# Diğer gereksiz loggerları kapat
for name in logging.root.manager.loggerDict:
    if name != 'index_collector':
        logging.getLogger(name).setLevel(logging.CRITICAL)
        logging.getLogger(name).propagate = False

class IndexCollector:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Referer': 'https://www.tradingview.com/'
        }
        self.tradingview_url = "https://www.tradingview.com/markets/indices/quotes-all/"
        self.session = requests.Session()
        
        # Endeks takibi için eklenen değişkenler
        self.all_indices = []  # TradingView'dan çekilen tüm endeksler
        self.added_indices = []  # Veritabanına başarıyla eklenen endeksler
        self.skipped_indices = []  # Çeşitli nedenlerle atlanmış endeksler
        self.skipped_reasons = {}  # Atlama nedenleri
        
        logger.info("IndexCollector başlatıldı")
        
    def fetch_tradingview_indices(self):
        """
        TradingView'dan tüm dünya endekslerini çeker
        """
        logger.info("TradingView'dan endeksler çekiliyor...")
        
        try:
            # Ana sayfa isteği
            response = self.session.get(self.tradingview_url, headers=self.headers)
            response.raise_for_status()
            
            # Sayfanın yüklenmesini beklemek için kısa bir gecikme
            time.sleep(1)
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Endeks tablosunu bul
            table = soup.find('table', {'class': 'table-Ngq2xrcK'})
            if not table:
                # Alternatif sınıf isimlerini dene
                table = soup.find('table', class_=lambda c: c and 'table-' in c)
                if not table:
                    logger.warning("TradingView'da standart tablo bulunamadı, alternatif arama yapılıyor...")
                    # DOM yapısı için alternatif bir arama
                    table = soup.find('div', class_=lambda c: c and 'listContainer' in c)
                    if not table:
                        logger.error("TradingView'da endeks tablosu bulunamadı!")
                        return []
            
            indices = []
            
            # <tbody> etiketi bulunamaması durumunda doğrudan <tr>'leri ara
            tbody = table.find('tbody')
            if tbody:
                rows = tbody.find_all('tr')
            else:
                # <div> tabanlı liste görünümünü dene
                rows = table.find_all('div', class_=lambda c: c and 'row' in c)
                if not rows:
                    # Son çare olarak tüm <tr> etiketlerini bul
                    rows = table.find_all('tr')
                    if len(rows) > 0:
                        rows = rows[1:]  # Başlık satırını atla
            
            # İlerleme bilgisi için
            total_rows = len(rows)
            logger.info(f"Toplam {total_rows} endeks bulundu, işleniyor...")
            
            for i, row in enumerate(rows):
                try:
                    
                    cells = row.find_all('td')
                    if not cells and hasattr(row, 'find_all'):
                        # <div> tabanlı hücreleri dene
                        cells = row.find_all('div', class_=lambda c: c and 'cell' in c)
                    
                    if len(cells) < 2:
                        continue
                    
                    # Sembol ve isim bilgisi
                    symbol_cell = cells[0]
                    symbol_link = symbol_cell.find('a', {'class': lambda c: c and ('apply-common-tooltip' in c or 'symbol' in c)})
                    
                    if not symbol_link:
                        # Alternatif arama
                        symbol_link = symbol_cell.find('a')
                        if not symbol_link:
                            continue
                    
                    # Sembol adını al
                    symbol = symbol_link.text.strip()
                    if not symbol:
                        continue
                    
                    # Sembol URL'i - detay sayfası için kullanılabilir
                    symbol_url = ""
                    if 'href' in symbol_link.attrs:
                        symbol_url = symbol_link['href']
                        if symbol_url.startswith('/'):
                            symbol_url = f"https://www.tradingview.com{symbol_url}"
                    
                    # İsim ve borsa bilgisi (genellikle üst etiketlerde bulunur)
                    exchange = "UNKNOWN"
                    name = symbol
                    
                    # Üst etiketi kontrol et
                    sup_element = symbol_cell.find('sup')
                    if sup_element:
                        exchange = sup_element.text.strip()
                        name = symbol_link.text.replace(exchange, '').strip()
                    
                    # Ülke bilgisi - bayrak elementinden
                    country = "UNKNOWN"
                    flag_element = symbol_cell.find('i', {'class': lambda c: c and 'flag-' in c})
                    if flag_element and 'class' in flag_element.attrs:
                        flag_classes = flag_element['class']
                        for class_name in flag_classes:
                            if 'flag-' in class_name and not 'flag-CyFdKRxR' in class_name:
                                country = class_name.replace('flag-', '').upper()
                                break
                                
                    # Ülke kodunu standartlaştır - bazı bayrak kodları 2 harf değil
                    if country != "UNKNOWN":
                        # Bazı özel ülke bayrak kodları
                        country_map = {
                            "EU": "EU",  # Avrupa Birliği
                            "UK": "GB",  # Birleşik Krallık
                            "EN": "GB",  # İngiltere
                            "ENGLAND": "GB",
                            "UNITED_KINGDOM": "GB",
                            "CHINA": "CN",
                            "CHN": "CN",
                            "PRC": "CN",
                            "USA": "US",
                            "UNITED_STATES": "US",
                            "AMERICA": "US",
                            "TURKEY": "TR",
                            "TURKIYE": "TR",
                            "JAPAN": "JP",
                            "JAP": "JP",
                            "GERMANY": "DE",
                            "GER": "DE",
                            "FRANCE": "FR",
                            "AUSTRALIA": "AU",
                            "AUS": "AU",
                            "CANADA": "CA",
                            "CAN": "CA",
                            "BRAZIL": "BR",
                            "BRA": "BR",
                            "INDIA": "IN",
                            "IND": "IN",
                            "KOREA": "KR",
                            "KOR": "KR",
                            "SOUTH_KOREA": "KR",
                            "SWITZERLAND": "CH",
                            "SWI": "CH",
                            "SPAIN": "ES",
                            "ESP": "ES",
                            "ITALY": "IT",
                            "ITA": "IT",
                            "NETHERLANDS": "NL",
                            "NET": "NL",
                            "HOLLAND": "NL",
                            "RUSSIA": "RU",
                            "RUS": "RU",
                            "SINGAPORE": "SG",
                            "SIN": "SG",
                            "SWEDEN": "SE",
                            "SWE": "SE"
                        }
                        
                        # Ülke kodunu standartlaştır
                        if country in country_map:
                            country = country_map[country]
                            
                        # Eğer 3 harf veya daha uzunsa ve sözlükte yoksa
                        # Arama yapmaya çalış
                        elif len(country) > 2:
                            for key, value in country_map.items():
                                if key in country.upper() or country.upper() in key:
                                    country = value
                                    break
                    
                    # Son fiyat bilgisi
                    price = "N/A"
                    if len(cells) > 1:
                        price_cell = cells[1]
                        price = price_cell.text.strip()
                    
                    # Para birimi - sembolden çıkarma ve ülkelere göre varsayılan atama
                    currency = self.get_currency_for_country(country)
                    
                    # Sembolden para birimi çıkarmaya çalış
                    if '/' in symbol:
                        parts = symbol.split('/')
                        if len(parts) > 1 and len(parts[1]) == 3 and parts[1].isalpha():
                            currency = parts[1]
                    
                    # Bazı endekslerde para birimi endeks adında olabilir
                    currency_codes = ["USD", "EUR", "JPY", "GBP", "CHF", "CAD", "AUD", "CNY", "HKD", "TRY"]
                    for code in currency_codes:
                        if code in name and len(name) > len(code):
                            currency = code
                            break
                    
                    # Değişim yüzdesi
                    change_percent = "N/A"
                    if len(cells) > 2:
                        change_cell = cells[2]
                        change_percent = change_cell.text.strip()
                    
                    # Ülke koduna göre gerçek borsa ismini belirle
                    if country != 'UNKNOWN':
                        # Normalize_exchange_name fonksiyonunu kullanarak borsa ismini belirle
                        # Bu fonksiyon endeks adlarından gerçek borsa isimlerini tespit eder
                        exchange = self.normalize_exchange_name(exchange, country, symbol)
                    
                    # Endeksi listeye ekle
                    endeks = {
                        'symbol': symbol,
                        'name': name,
                        'exchange': exchange,
                        'price': price,
                        'change_percent': change_percent,
                        'country': country,
                        'currency': currency,
                        'url': symbol_url
                    }
                    
                    indices.append(endeks)
                    
                    # Tüm indeksler listesine de ekle (takip için)
                    self.all_indices.append(endeks)
                    
                except Exception as e:
                    logger.warning(f"Satır işleme hatası: {str(e)}")
                    continue
            
            logger.info(f"{len(indices)} endeks TradingView'dan başarıyla çekildi")
            return indices
            
        except Exception as e:
            logger.error(f"TradingView veri çekme hatası: {str(e)}")
            return []
    
    def fetch_index_details(self, index_url):
        """
        TradingView'dan belirli bir endeksin detay bilgilerini çeker
        """
        if not index_url:
            return {}
            
        try:
            # Sayfayı yüklerken sık istek yapmamak için bekleme
            time.sleep(random.uniform(0.5, 0.9))
            
            response = self.session.get(index_url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            details = {}
            
            # Endeks açıklaması
            description_div = soup.find('div', {'class': lambda c: c and 'description' in c})
            if description_div:
                details['description'] = description_div.text.strip()
            
            # Teknik bilgiler tablosu (varsa)
            info_table = soup.find('table', {'class': lambda c: c and 'overview' in c})
            if info_table:
                rows = info_table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        key = cells[0].text.strip().lower().replace(' ', '_')
                        value = cells[1].text.strip()
                        details[key] = value
            
            return details
            
        except Exception as e:
            logger.warning(f"Endeks detayı çekme hatası: {str(e)}")
            return {}
    
    def get_currency_for_country(self, country):
        """Ülke koduna göre varsayılan para birimini döndürür"""
        country_currencies = {
            "US": "USD",
            "UK": "GBP",
            "GB": "GBP",
            "JP": "JPY",
            "EU": "EUR",
            "DE": "EUR",
            "FR": "EUR",
            "IT": "EUR",
            "ES": "EUR",
            "NL": "EUR",
            "CH": "CHF",
            "CA": "CAD",
            "AU": "AUD",
            "CN": "CNY",
            "HK": "HKD",
            "IN": "INR",
            "BR": "BRL",
            "RU": "RUB",
            "ZA": "ZAR",
            "TR": "TRY",
            "KR": "KRW",
            "SG": "SGD",
            "TW": "TWD",
            "SE": "SEK",
            "NO": "NOK",
            "DK": "DKK",
            "PL": "PLN",
            "NZ": "NZD",
            "MX": "MXN",
            "ID": "IDR",
            "MY": "MYR",
            "TH": "THB",
            "PH": "PHP",
            "QA": "QAR",
            "AE": "AED",
            "SA": "SAR",
            "IL": "ILS"
        }
        
        return country_currencies.get(country, "USD")

    def get_exchange_for_country(self, country_code):
        """Ülke koduna göre ana borsa ismini döndürür"""
        # İki harfli ülke kodlarından tam borsa ismine dönüşüm
        default_exchanges = {
            "US": "NYSE",     # Amerika Birleşik Devletleri
            "UK": "LSE",      # Birleşik Krallık
            "GB": "LSE",      # Büyük Britanya
            "JP": "TSE",      # Japonya
            "CN": "SSE",      # Çin
            "DE": "FWB",      # Almanya
            "FR": "EPA",      # Fransa
            "AU": "ASX",      # Avustralya
            "CA": "TSX",      # Kanada
            "BR": "B3",       # Brezilya
            "IN": "BSE",      # Hindistan
            "KR": "KRX",      # Güney Kore
            "CH": "SIX",      # İsviçre
            "ES": "BME",      # İspanya
            "IT": "MIL",      # İtalya
            "NL": "AMS",      # Hollanda
            "RU": "MOEX",     # Rusya
            "SG": "SGX",      # Singapur
            "SE": "STO",      # İsveç
            "HK": "HKEX",     # Hong Kong
            "TR": "BIST",     # Türkiye
            "ZA": "JSE",      # Güney Afrika
            "TW": "TWSE",     # Tayvan
            "MY": "KLSE",     # Malezya
            "TH": "SET",      # Tayland
            "ID": "IDX",      # Endonezya
            "PH": "PSE",      # Filipinler
            "AT": "VIE",      # Avusturya
            "BE": "BRU",      # Belçika
            "DK": "CPH",      # Danimarka
            "FI": "HEL",      # Finlandiya
            "GR": "ATSE",     # Yunanistan
            "NO": "OSE",      # Norveç
            "PT": "LISB",     # Portekiz
            "PL": "GPW",      # Polonya
            "AE": "ADX",      # Birleşik Arap Emirlikleri
            "SA": "TADAWUL",  # Suudi Arabistan
            "IL": "TASE",     # İsrail
            "EG": "EGX",      # Mısır
            "QA": "QSE",      # Katar
            "KW": "KSE",      # Kuveyt
            "NG": "NGX",      # Nijerya
            "AR": "BCBA",     # Arjantin
            "MX": "BMV",      # Meksika
            "CL": "BCS",      # Şili
            "CO": "BVC",      # Kolombiya
            "PE": "BVL",      # Peru
            "NZ": "NZX"       # Yeni Zelanda
        }
        
        # Özel durumlar - TradingView'dan gelen bazı ülke kodları standard olmayabilir
        special_cases = {
            "USA": "NYSE",
            "UNITED STATES": "NYSE",
            "UNITED_STATES": "NYSE",
            "UNITED KINGDOM": "LSE",
            "UNITED_KINGDOM": "LSE",
            "TURKEY": "BIST",
            "CHINA": "SSE",
            "JAPAN": "TSE",
            "GERMANY": "FWB",
            "FRANCE": "EPA",
            "AUSTRALIA": "ASX",
            "CANADA": "TSX",
            "BRAZIL": "B3",
            "INDIA": "BSE",
            "KOREA": "KRX",
            "SOUTH KOREA": "KRX",
            "SOUTH_KOREA": "KRX",
            "SWITZERLAND": "SIX",
            "SPAIN": "BME",
            "ITALY": "MIL",
            "NETHERLANDS": "AMS",
            "RUSSIA": "MOEX",
            "SINGAPORE": "SGX",
            "SWEDEN": "STO",
            "HONG KONG": "HKEX",
            "HONG_KONG": "HKEX",
            "SOUTH AFRICA": "JSE",
            "SOUTH_AFRICA": "JSE",
            "TAIWAN": "TWSE",
            "MALAYSIA": "KLSE",
            "THAILAND": "SET",
            "INDONESIA": "IDX",
            "PHILIPPINES": "PSE",
            "AUSTRIA": "VIE",
            "BELGIUM": "BRU",
            "DENMARK": "CPH",
            "FINLAND": "HEL",
            "GREECE": "ATSE",
            "NORWAY": "OSE",
            "PORTUGAL": "LISB",
            "POLAND": "GPW",
            "UAE": "ADX",
            "UNITED ARAB EMIRATES": "ADX",
            "UNITED_ARAB_EMIRATES": "ADX",
            "SAUDI ARABIA": "TADAWUL",
            "SAUDI_ARABIA": "TADAWUL",
            "ISRAEL": "TASE",
            "EGYPT": "EGX",
            "QATAR": "QSE",
            "KUWAIT": "KSE",
            "NIGERIA": "NGX",
            "ARGENTINA": "BCBA",
            "MEXICO": "BMV",
            "CHILE": "BCS",
            "COLOMBIA": "BVC",
            "PERU": "BVL",
            "NEW ZEALAND": "NZX",
            "NEW_ZEALAND": "NZX"
        }
        
        # Önce special_cases sözlüğünde arama yap (uzun ülke adları için)
        country_code_upper = country_code.upper()
        if country_code_upper in special_cases:
            return special_cases[country_code_upper]
        
        # Sonra default_exchanges sözlüğünde arama yap (2 harfli ülke kodları için)
        if len(country_code) == 2:
            return default_exchanges.get(country_code.upper(), f"{country_code.upper()}_EXCHANGE")
        
        # Bilinmeyen ülkeler için varsayılan borsa kodu
        return f"{country_code.upper()}_EXCHANGE"

    def normalize_exchange_name(self, exchange, country, symbol):
        """
        Endeks adı veya çeşitli borsa/piyasa bilgilerinden 
        standart borsa kodunu belirler
        """
        # Önce ülke koduna göre varsayılan borsa
        default_exchange = self.get_exchange_for_country(country)
        
        # Boş veya belirsiz değerler için ülkenin ana borsasını kullan
        if not exchange or exchange == "UNKNOWN" or exchange == "INDEX" or exchange == "GLOBAL_INDICES":
            return default_exchange
            
        # Bilinen endeks isimleri için borsa eşleştirmeleri
        index_to_exchange = {
            # ABD Endeksleri
            "S&P": "NYSE",
            "S&P 500": "NYSE",
            "S&P500": "NYSE",
            "DOW": "NYSE",
            "DOW JONES": "NYSE",
            "NASDAQ": "NASDAQ",
            "NASDAQ 100": "NASDAQ",
            "NASDAQ100": "NASDAQ",
            "NYSE": "NYSE",
            "RUSSELL": "NYSE",
            "RUSSELL 2000": "NYSE",
            "RUSSELL2000": "NYSE",
            "DJ": "NYSE",
            "DJIA": "NYSE",
            "US30": "NYSE",
            "US500": "NYSE",
            "US100": "NASDAQ",
            "ES": "CME",
            "SPX": "NYSE",
            "NDX": "NASDAQ",
            "YM": "CBOT",
            "RTY": "CME",
            "NQ": "CME",
            "SP1!": "NYSE",
            "NQ1!": "NASDAQ",
            "SP:": "NYSE",
            "DJ:": "NYSE",
            "$SPX": "NYSE",
            "$SPX.X": "NYSE",
            "$COMPX": "NASDAQ",
            "$NDX": "NASDAQ",
            "$NDX.X": "NASDAQ",
            "$DJI": "NYSE",
            "$DJI.X": "NYSE",
            
            # Avrupa Endeksleri
            "DAX": "FWB",
            "DAX 40": "FWB",
            "DAX40": "FWB",
            "GDAXI": "FWB",
            "XETRA": "FWB",
            "IBEX": "BME",
            "IBEX 35": "BME",
            "IBEX35": "BME",
            "FTSE": "LSE",
            "FTSE 100": "LSE",
            "FTSE100": "LSE",
            "FTSE MIB": "MIL",
            "FTSEMIB": "MIL",
            "CAC": "EPA",
            "CAC 40": "EPA",
            "CAC40": "EPA",
            "AEX": "AMS",
            "AEX 25": "AMS",
            "AEX25": "AMS",
            "SMI": "SIX",
            "ATX": "VIE",
            "BEL": "BRU",
            "BEL 20": "BRU",
            "BEL20": "BRU",
            "MOEX": "MOEX",
            "RTS": "MOEX",
            "OMXS": "STO",
            "OMXC": "CPH",
            "OMXH": "HEL",
            "OSEBX": "OSE",
            "PSI": "LISB",
            "PSI 20": "LISB",
            "PSI20": "LISB",
            "WIG": "GPW",
            "WIG20": "GPW",
            "ISEQ": "ISE",
            "ISEQ ALL SHARE": "ISE",
            "EURO STOXX 50": "STOXX",
            "DE30": "FWB",
            "DB1": "FWB",
            "UK100": "LSE",
            "SX5E": "STOXX",
            
            # Asya Endeksleri
            "NIKKEI": "TSE",
            "NIKKEI 225": "TSE",
            "NIKKEI225": "TSE",
            "N225": "TSE",
            "TOPIX": "TSE",
            "HANG SENG": "HKEX",
            "HANGSENG": "HKEX",
            "HSI": "HKEX",
            "SHANGHAI": "SSE",
            "SHANGHAI COMPOSITE": "SSE",
            "SHCOMP": "SSE",
            "CSI": "SSE",
            "CSI 300": "SSE",
            "CSI300": "SSE",
            "SHENZHEN": "SZSE",
            "KOSPI": "KRX",
            "KOSPI 200": "KRX",
            "KOSPI200": "KRX",
            "SENSEX": "BSE",
            "BSE SENSEX": "BSE",
            "NIFTY": "NSE",
            "NIFTY 50": "NSE",
            "NIFTY50": "NSE",
            "ASX": "ASX",
            "ASX 200": "ASX",
            "ASX200": "ASX",
            "STI": "SGX",
            "STRAITS TIMES": "SGX",
            "TAIEX": "TWSE",
            "JCI": "IDX",
            "JAKARTA": "IDX",
            "KLCI": "KLSE",
            "KUALA LUMPUR": "KLSE",
            "SET": "SET",
            "THAI": "SET",
            "PSE": "PSE",
            "PSEI": "PSE",
            "PHILIPPINE": "PSE",
            "JP225": "TSE",
            "CN50": "SSE",
            "SSEC": "SSE",
            "399001": "SZSE",
            "HK50": "HKEX",
            "IN50": "NSE",
            
            # Diğer Bölgeler
            "BIST": "BIST",
            "BIST 100": "BIST",
            "BIST100": "BIST",
            "XU100": "BIST",
            "BOVESPA": "B3",
            "IBOVESPA": "B3",
            "BVSP": "B3",
            "MERVAL": "BCBA",
            "IPC": "BMV",
            "S&P/BMV IPC": "BMV",
            "IPSA": "BCS",
            "COLCAP": "BVC",
            "S&P/BVL": "BVL",
            "JSE": "JSE",
            "TOP 40": "JSE",
            "TOP40": "JSE",
            "ALSI": "JSE",
            "EGX": "EGX",
            "EGX 30": "EGX",
            "EGX30": "EGX",
            "QE": "QSE",
            "ADX": "ADX",
            "TASI": "TADAWUL",
            "TADAWUL": "TADAWUL",
            "TA-35": "TASE",
            "TA35": "TASE",
            "TASE": "TASE",
            "NZX": "NZX",
            "NZX 50": "NZX",
            "NZX50": "NZX",
            "BR20": "B3",
            "XU30": "BIST",
            "TRY": "BIST",
            "IST": "BIST",
            "MX": "BMV",
            "MX20": "BMV",
            "ZA40": "JSE",
            
            # Özel durumlar - bazı ülkeler için çok uzun borsa adları
            "ICE BOFAML": "ICE",
            "CBOE": "CBOE",
            "CBOE VOLATILITY INDEX": "CBOE",
            "VIX": "CBOE",
            "EURO STOXX": "STOXX",
            "STOXX": "STOXX",
            "STOXX 50": "STOXX",
            "STOXX50": "STOXX",
            "MSCI": "MSCI",
            "FTSE DEVELOPED": "FTSE",
            "FTSE EMERGING": "FTSE",
            "S&P GLOBAL": "SP",
            "DOW JONES GLOBAL": "DJ",
            
            # Volatilite endeksleri
            "MOVE": "ICE",
            "VSTOXX": "STOXX",
            "VDAX": "FWB",
            "VFTSE": "LSE",
            "VNKY": "TSE",
            "VHSI": "HKEX",
            
            # Emtia endeksleri
            "CRB": "NYBOT",
            "CCI": "CCI",
            "GSCI": "CME",
            "S&P GSCI": "CME",
            "BLOOMBERG COMMODITY": "BCOM",
            "THOMSON REUTERS": "TR"
        }
        
        # Exchange adındaki kelimeler için kontrol et
        exchange_upper = exchange.upper()
        for index_name, exchange_code in index_to_exchange.items():
            if index_name.upper() in exchange_upper or exchange_upper in index_name.upper():
                return exchange_code
        
        # Symbol içerisinde endeks adı bulunabilir
        if symbol:
            symbol_upper = symbol.upper()
            for index_name, exchange_code in index_to_exchange.items():
                if index_name.upper() in symbol_upper:
                    return exchange_code
        
        # Uzun veya karmaşık borsa adları için kısaltma yap
        if len(exchange) > 10 and " " in exchange:
            # İlk kelimenin kısaltmasını al
            first_word = exchange.split()[0].upper()
            if len(first_word) >= 2:
                return first_word
        
        # Hala bulunamadıysa ve çok uzunsa kısalt
        if len(exchange) > 10:
            return exchange[:10].upper()
        
        # Diğer tüm durumlarda gelen değeri standartlaştır
        result = exchange.upper()
        
        # Son bir kontrol: veritabanı sütunu için 20 karakter sınırı
        if len(result) > 20:
            return result[:20]
            
        return result

    def sync_pariteler_to_db(self, yeni_pariteler):
        """Pariteleri veritabanına kaydeder"""
        if not yeni_pariteler:
            return (0, 0, 0)
            
        db = None
        try:
            db = Database()
            if not db.connect():
                logger.error("Veritabanına bağlanılamadı!")
                return (0, 0, 0)
                
            cursor = db.cursor()
            if not cursor:
                logger.error("Veritabanı cursor oluşturulamadı!")
                return (0, 0, 0)
                
            eklenen = 0
            
            for parite in yeni_pariteler:
                try:
                    # Kaydetmeden önce parite formatını logla
                    parite_str = parite['parite']
                    logger.debug(f"Parite eklenecek: {parite_str}")
                    
                    # Veritabanında bu parite var mı kontrol et
                    cursor.execute("""
                        SELECT 1 FROM pariteler WITH (NOLOCK)
                        WHERE parite = ? AND tip = 'INDEX'
                    """, (parite['parite'],))
                    
                    exists = cursor.fetchone() is not None
                    
                    if exists:
                        logger.debug(f"Parite zaten mevcut: {parite_str}")
                        continue
                    
                    # Veritabanına ekle
                    cursor.execute("""
                        INSERT INTO pariteler (parite, aktif, borsa, tip, ulke, aciklama)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, 
                    (parite['parite'], parite['aktif'], parite['borsa'], 
                    parite['tip'], parite['ulke'], parite['aciklama']))
                    
                    db.commit()
                    eklenen += 1
                    logger.debug(f"Parite eklendi: {parite['parite']}")
                    
                except Exception as e:
                    logger.warning(f"Parite ekleme hatası ({parite['parite']}): {str(e)}")
                    # Hatayı yukarı aktar
                    raise
                    
            return (eklenen, 0, 0)
            
        except Exception as e:
            logger.error(f"Veritabanı hatası: {str(e)}")
            raise
            
        finally:
            try:
                if db:
                    db.disconnect()
            except Exception as e:
                logger.error(f"Bağlantı kapatma hatası: {str(e)}")
                
    def check_database_connection(self):
        """Veritabanı bağlantısını kontrol eder"""
        try:
            db = Database()
            if db.connect():
                logger.info("Veritabanı bağlantısı başarılı")
                db.disconnect()
                return True
            else:
                logger.error("Veritabanı bağlantısı başarısız!")
                return False
        except Exception as e:
            logger.error(f"Veritabanı kontrol hatası: {str(e)}")
            return False

    def clean_index_records(self):
        """
        Veritabanındaki mevcut tüm INDEX tipindeki kayıtları temizler
        """
        db = None
        try:
            db = Database()
            if not db.connect():
                logger.error("Veritabanına bağlanılamadı!")
                return False
                
            cursor = db.cursor()
            if not cursor:
                logger.error("Veritabanı cursor oluşturulamadı!")
                return False
                
            # Mevcut kayıt sayısını kontrol et
            cursor.execute("SELECT COUNT(*) FROM pariteler WITH (NOLOCK) WHERE tip = 'INDEX'")
            row = cursor.fetchone()
            if row:
                existing_count = row[0]
                logger.info(f"Veritabanında {existing_count} adet INDEX kaydı bulundu")
            
            # INDEX tipindeki tüm kayıtları sil
            cursor.execute("DELETE FROM pariteler WHERE tip = 'INDEX'")
            db.commit()
            
            # Silinen kayıt sayısını logla
            logger.info(f"Veritabanından tüm INDEX kayıtları silindi")
            return True
            
        except Exception as e:
            logger.error(f"Veritabanı temizleme hatası: {str(e)}")
            return False
            
        finally:
            try:
                if db:
                    db.disconnect()
            except Exception as e:
                logger.error(f"Bağlantı kapatma hatası: {str(e)}")

    def collect_pariteler(self):
        """
        TradingView üzerinden endeksleri getirir ve veritabanına ekler
        """
        try:
            # Başlamadan önce takip listelerini temizle
            self.all_indices = []
            self.added_indices = []
            self.skipped_indices = []
            self.skipped_reasons = {}
            
            # Veritabanı bağlantısını kontrol et
            if not self.check_database_connection():
                logger.error("Veritabanı bağlantısı sağlanamadı, işlem durduruluyor!")
                return (0, 0, 0)
            
            # Veritabanından mevcut tüm INDEX kayıtlarını sil
            if not self.clean_index_records():
                logger.error("Veritabanı temizleme başarısız oldu, işlem durduruluyor!")
                return (0, 0, 0)
                
            # TradingView'dan endeksleri çek
            indices = self.fetch_tradingview_indices()
            
            if not indices or len(indices) == 0:
                logger.error("TradingView'dan hiç endeks çekilemedi!")
                return (0, 0, 0)
                
            genel_toplam = 0
            genel_eklenen = 0
            hatali_indeks = 0
            
            # Her endeks için işlem yap
            for i, index in enumerate(indices):
                try:
                    # Kullanıcı deneyimi için ilerleme göstergesi
                    if i % 10 == 0:
                        logger.info(f"İndeksler işleniyor: {i}/{len(indices)}")
                        
                    symbol = index['symbol'].strip().upper() if 'symbol' in index else ''
                    name = index['name'].strip() if 'name' in index else ''
                    exchange = index['exchange'].strip() if 'exchange' in index else 'UNKNOWN'
                    country = index['country'] if 'country' in index else 'UNKNOWN'
                    currency = index['currency'] if 'currency' in index else 'USD'
                    
                    if not symbol:
                        hatali_indeks += 1
                        self.skipped_indices.append(index)
                        self.skipped_reasons[symbol] = "Sembol boş"
                        continue
                    
                    # Borsa adını normalize et - endeks adı yerine gerçek borsa adını kullan
                    exchange = self.normalize_exchange_name(exchange, country, symbol)
                    
                    # Endeks URL'i varsa detayları çek
                    description = ""
                    if 'url' in index and index['url']:
                        details = self.fetch_index_details(index['url'])
                        if details and 'description' in details:
                            description = details['description']
                    
                    # Açıklama yoksa varsayılan oluştur
                    if not description:
                        description = f"{name} - {exchange} Index"
                    
                    # Endeks bilgilerini hazırla
                    index_info = [{
                        'parite': f"{symbol}/{currency}",
                        'aktif': 1,
                        'borsa': exchange,
                        'tip': 'INDEX',
                        'ulke': country,
                        'aciklama': description
                    }]
                    
                    # Borsa adı veritabanı sütun sınırını aşmasın diye kısalt (log'da hata gözüküyor)
                    if len(index_info[0]['borsa']) > 20:
                        logger.warning(f"Borsa adı çok uzun, kısaltılıyor: {index_info[0]['borsa']}")
                        index_info[0]['borsa'] = index_info[0]['borsa'][:20]
                    
                    # Veritabanına kaydet
                    try:
                        eklenen, guncellenen, silinen = self.sync_pariteler_to_db(index_info)
                        
                        if eklenen > 0:
                            genel_eklenen += eklenen
                            genel_toplam += 1
                            self.added_indices.append(index)
                            logger.debug(f"Endeks eklendi: {symbol}/{currency}")
                        else:
                            self.skipped_indices.append(index)
                            self.skipped_reasons[f"{symbol}/{currency}"] = "Veritabanına eklenemedi"
                            logger.debug(f"Endeks eklenemedi: {symbol}/{currency}")
                             
                    except KeyboardInterrupt:
                        logger.warning("Kullanıcı tarafından durduruldu.")
                        return (genel_toplam, genel_eklenen, hatali_indeks)
                    except Exception as e:
                        hatali_indeks += 1
                        self.skipped_indices.append(index)
                        self.skipped_reasons[f"{symbol}/{currency}"] = f"Veritabanı hatası: {str(e)}"
                        logger.warning(f"Endeks kaydetme hatası ({symbol}/{currency}): {str(e)}")
                    
                except KeyboardInterrupt:
                    logger.warning("Kullanıcı tarafından durduruldu.")
                    return (genel_toplam, genel_eklenen, hatali_indeks)
                except Exception as e:
                    hatali_indeks += 1
                    if 'symbol' in index:
                        symbol = index['symbol']
                        self.skipped_indices.append(index)
                        self.skipped_reasons[f"{symbol}"] = f"İşleme hatası: {str(e)}"
                    logger.warning(f"İndeks işleme hatası ({symbol if 'symbol' in index else 'Unknown'}): {str(e)}")
                    continue
            
            # İşlem sonunda kısa bir özet log
            logger.info(f"INDEX: {genel_toplam} endeks bulundu -> {genel_eklenen} yeni eklendi, {hatali_indeks} hatalı")
            
            return genel_toplam, genel_eklenen, hatali_indeks
            
        except KeyboardInterrupt:
            logger.warning("Kullanıcı tarafından durduruldu.")
            return 0, 0, 0
        except Exception as e:
            logger.error(f"Genel hata: {str(e)}")
            return 0, 0, 0

    def run(self):
        """
        Endeks toplayıcı çalıştırma fonksiyonu
        """
        try:
            # Başlangıç mesajını yazdır
            logger.info("="*50)
            logger.info("TradingView Index Collector başlatılıyor...")
            logger.info("="*50)
            
            # Başlangıç zamanını kaydet
            start_time = datetime.now()
            
            # TradingView'dan verileri çek ve veritabanına kaydet
            genel_toplam, genel_eklenen, hatali = self.collect_pariteler()
            
            # Bitiş zamanı ve toplam süre
            end_time = datetime.now()
            duration = end_time - start_time
            
            # Özet log formatı
            logger.info("="*50)
            logger.info(f"TradingView Index Collector sonuçları:")
            logger.info(f"Toplam {len(self.all_indices)} endeks bulundu")
            logger.info(f"Yeni eklenen: {genel_eklenen}")
            logger.info(f"Hatalı: {hatali}")
            logger.info(f"Eklenemeyen: {len(self.all_indices) - genel_eklenen}")
            logger.info(f"Toplam süre: {duration}")
            logger.info("="*50)
            
            # Eklenemeyen endekslerin detaylı özeti
            if len(self.all_indices) > genel_eklenen:
                eksik_sayi = len(self.all_indices) - genel_eklenen
                print(f"\n{'='*50}")
                print(f" EKLENEMEDİ: {eksik_sayi} endeks ({len(self.all_indices)} bulunan, {genel_eklenen} eklenen)")
                print(f"{'='*50}")
                
                # Atlanan endeksleri neden eklenemediği ile birlikte yazdır
                for i, skipped_index in enumerate(self.skipped_indices):
                    if 'symbol' in skipped_index and 'currency' in skipped_index:
                        symbol = skipped_index['symbol']
                        currency = skipped_index['currency']
                        sembol_str = f"{symbol}/{currency}"
                        name = skipped_index.get('name', '')
                        reason = self.skipped_reasons.get(sembol_str, self.skipped_reasons.get(symbol, "Bilinmiyor"))
                        print(f"{i+1:2d}. {sembol_str:15s} - {name:25s} - Neden: {reason}")
                
                print(f"{'='*50}\n")
            
            return genel_toplam, genel_eklenen, hatali
            
        except Exception as e:
            logger.error(f"TradingView endeks toplama hatası: {str(e)}")
            return 0, 0, 0

if __name__ == "__main__":
    try:
        print("\n" + "="*50)
        print("TRADINGVIEW INDEX COLLECTOR BAŞLATILIYOR...")
        print("="*50 + "\n")
        
        collector = IndexCollector()
        total, added, failed = collector.run()
        
        print("\n" + "="*50)
        print(f"TRADINGVIEW INDEX COLLECTOR TAMAMLANDI.")
        print(f"Toplam: {total} | Eklenen: {added} | Hatalı: {failed}")
        print("="*50 + "\n")
    except KeyboardInterrupt:
        print("\nKullanıcı tarafından durduruldu.")
    except Exception as e:
        print(f"\nProgram hatası: {str(e)}") 