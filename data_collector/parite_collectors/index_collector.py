"""
Borsa endekslerini toplayan sınıf
"""

import investpy
import requests
from bs4 import BeautifulSoup, Tag
from utils.database import Database
import logging

# yfinance ve ilgili logger'ları kapat
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

# Diğer logger'ları da kapat
for name in logging.root.manager.loggerDict:
    if 'yfinance' in name or 'urllib3' in name:
        logging.getLogger(name).setLevel(logging.CRITICAL)
        logging.getLogger(name).propagate = False

class IndexCollector:
    def __init__(self):
        pass

    def fetch_currency_list(self):
        """ISO 4217 para birimleri listesini Wikipedia'dan çeker."""
        url = 'https://en.wikipedia.org/wiki/ISO_4217'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'lxml')
        currencies = []
        
        # Ana para birimleri tablosunu bul (ilk büyük tablo)
        tables = soup.find_all('table', {'class': 'wikitable'})
        if not tables:
            return currencies
            
        # İlk tablo aktif para birimlerini içerir
        table = tables[0]
        rows = table.find_all('tr')
        
        for row in rows[1:]:  # Başlık satırını atla
            if isinstance(row, Tag):
                cols = row.find_all('td')
                if len(cols) >= 3:  # En az 3 sütun olmalı
                    try:
                        currency_code = cols[0].text.strip()
                        currency_name = cols[2].text.strip()
                        # Sadece 3 harfli kodları al ve boş olmayanları ekle
                        if len(currency_code) == 3 and currency_code.isalpha():
                            currencies.append((currency_name, currency_code))
                    except:
                        continue
        
        return currencies

    def get_country_currency(self, country_name):
        """
        Ülkenin para birimini Wikipedia'dan alır
        """
        try:
            # Önce ülke adını küçük harfe çevir
            country_name = country_name.lower()
            
            # Para birimi listesini al
            currency_list = self.fetch_currency_list()
            
            # Wikipedia listesinde ara
            for currency_name, currency_code in currency_list:
                currency_name = currency_name.lower()
                if (country_name in currency_name or 
                    country_name.replace(' ', '') in currency_name.replace(' ', '')):
                    return currency_code
            
            # Bulunamazsa USD döndür
            return 'USD'
            
        except:
            return 'USD'  # Hata durumunda USD kullan

    def get_exchange_for_country(self, country):
        """
        Ülkenin ana borsasını investpy'dan alır
        """
        try:
            # Ülkenin endekslerini al
            indices = investpy.get_indices(country=country.lower())
            if len(indices) > 0:
                # Önce ülkeye göre varsayılan borsayı belirle
                country_upper = country.upper()
                default_exchanges = {
                    'TURKEY': 'BIST',
                    'UNITED KINGDOM': 'LSE',
                    'UNITED STATES': 'NYSE',
                    'JAPAN': 'TSE',
                    'CHINA': 'SSE',
                    'GERMANY': 'FWB',
                    'FRANCE': 'EPA',
                    'AUSTRALIA': 'ASX',
                    'CANADA': 'TSX',
                    'BRAZIL': 'B3',
                    'INDIA': 'BSE',
                    'SOUTH KOREA': 'KRX',
                    'SWITZERLAND': 'SIX',
                    'SPAIN': 'BME',
                    'ITALY': 'MIL',
                    'NETHERLANDS': 'AMS',
                    'RUSSIA': 'MOEX',
                    'SINGAPORE': 'SGX',
                    'SWEDEN': 'STO'
                }
                
                # Önce ülkenin varsayılan borsasını kontrol et
                exchange = default_exchanges.get(country_upper)
                
                # Varsayılan borsa yoksa endeks adlarından çıkarmaya çalış
                if not exchange:
                    for _, index in indices.iterrows():
                        name = index['name'].upper()
                        # Bilinen borsa isimlerini kontrol et
                        if 'BIST' in name and country_upper == 'TURKEY':
                            exchange = 'BIST'
                            break
                        elif 'NYSE' in name and country_upper == 'UNITED STATES':
                            exchange = 'NYSE'
                            break
                        elif 'NASDAQ' in name and country_upper == 'UNITED STATES':
                            exchange = 'NASDAQ'
                            break
                        elif 'LSE' in name and country_upper == 'UNITED KINGDOM':
                            exchange = 'LSE'
                            break
                        elif 'SSE' in name and country_upper == 'CHINA':
                            exchange = 'SSE'
                            break
                        elif 'NIKKEI' in name and country_upper == 'JAPAN':
                            exchange = 'TSE'
                            break
                        elif 'DAX' in name and country_upper == 'GERMANY':
                            exchange = 'FWB'
                            break
                        elif 'CAC' in name and country_upper == 'FRANCE':
                            exchange = 'EPA'
                            break
                        elif 'ASX' in name and country_upper == 'AUSTRALIA':
                            exchange = 'ASX'
                            break
                        elif 'TSX' in name and country_upper == 'CANADA':
                            exchange = 'TSX'
                            break
                        elif 'BOVESPA' in name and country_upper == 'BRAZIL':
                            exchange = 'B3'
                            break
                        elif 'SENSEX' in name and country_upper == 'INDIA':
                            exchange = 'BSE'
                            break
                        elif 'KOSPI' in name and country_upper == 'SOUTH KOREA':
                            exchange = 'KRX'
                            break
                        elif 'SMI' in name and country_upper == 'SWITZERLAND':
                            exchange = 'SIX'
                            break
                        elif 'IBEX' in name and country_upper == 'SPAIN':
                            exchange = 'BME'
                            break
                        elif 'MIB' in name and country_upper == 'ITALY':
                            exchange = 'MIL'
                            break
                        elif 'AEX' in name and country_upper == 'NETHERLANDS':
                            exchange = 'AMS'
                            break
                        elif 'MOEX' in name and country_upper == 'RUSSIA':
                            exchange = 'MOEX'
                            break
                        elif 'STI' in name and country_upper == 'SINGAPORE':
                            exchange = 'SGX'
                            break
                        elif 'OMX' in name and country_upper == 'SWEDEN':
                            exchange = 'STO'
                            break
                
                # Hala borsa bulunamadıysa market bilgisini kullan
                if not exchange:
                    market = indices.iloc[0]['market'].upper()
                    if market != 'GLOBAL_INDICES':
                        exchange = market
                    else:
                        exchange = f"{country_upper}_STOCK"
                
                return indices, exchange
            else:
                return None, None
            
        except Exception as e:
            return None, None

    def sync_pariteler_to_db(self, yeni_pariteler):
        """Pariteleri veritabanına kaydeder"""
        if not yeni_pariteler:
            return (0, 0, 0)
            
        db = None
        try:
            db = Database()
            if not db.connect():
                return (0, 0, 0)
                
            cursor = db.cursor()
            if not cursor:
                return (0, 0, 0)
                
            eklenen = 0
            
            for parite in yeni_pariteler:
                try:
                    cursor.execute("""
                        SELECT 1 FROM pariteler 
                        WHERE parite = ? AND tip = 'INDEX'
                    """, (parite['parite'],))
                    
                    exists = cursor.fetchone() is not None
                    
                    if not exists:
                        cursor.execute("""
                            INSERT INTO pariteler (parite, aktif, borsa, tip, ulke, aciklama)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, 
                        parite['parite'], parite['aktif'], parite['borsa'], 
                        parite['tip'], parite['ulke'], parite['aciklama'])
                        
                        db.commit()
                        eklenen += 1
                        
                except:
                    continue
                    
            return (eklenen, 0, 0)
            
        except:
            return (0, 0, 0)
            
        finally:
            if db:
                db.disconnect()

    def collect_pariteler(self):
        """
        Investpy üzerinden endeksleri getirir ve veritabanına ekler
        """
        try:
            # Desteklenen ülkeleri al
            countries = investpy.get_index_countries()
            
            genel_toplam = 0
            genel_eklenen = 0
            
            for country in countries:
                try:
                    # Ülkenin endekslerini al
                    indices_df, exchange = self.get_exchange_for_country(country)
                    if indices_df is None or len(indices_df) == 0:
                        continue
                    
                    # Her endeks için
                    for _, index in indices_df.iterrows():
                        try:
                            # Endeks bilgilerini al
                            symbol = index['symbol'].strip().upper()
                            name = index['name'].strip()
                            country_name = country.upper()
                            currency = index['currency'].upper() if 'currency' in index else self.get_country_currency(country)
                            
                            if not symbol:
                                continue
                                
                            index_info = [{
                                'parite': f"{symbol}/{currency}",
                                'aktif': 1,
                                'borsa': exchange,
                                'tip': 'INDEX',
                                'ulke': country_name,
                                'aciklama': f"{name} - {exchange} Index"
                            }]
                            
                            # Veritabanına kaydet
                            eklenen, guncellenen, silinen = self.sync_pariteler_to_db(index_info)
                            genel_eklenen += eklenen
                            
                        except:
                            continue
                    
                    genel_toplam += len(indices_df)
                    
                except:
                    continue
            
            print(f"Indices: {genel_toplam} endeks bulundu -> {genel_eklenen} yeni eklendi")
            
        except Exception as e:
            print(f"Hata: {str(e)}")

if __name__ == "__main__":
    collector = IndexCollector()
    collector.collect_pariteler()