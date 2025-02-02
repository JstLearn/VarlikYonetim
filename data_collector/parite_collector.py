"""
ccxt kütüphanesi kullanarak Binance borsasından aktif pariteleri veritabanına kaydeder
"""

import ccxt
from database import Database
import sys
import pyodbc
import os
from pathlib import Path
from dotenv import load_dotenv
from config import DB_CONFIG
from datetime import datetime
import time
import investpy
import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

# .env dosyasını yükle
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

def check_sql_driver():
    """SQL Server sürücülerini kontrol eder"""
    try:
        drivers = [x for x in pyodbc.drivers() if x.startswith('SQL Server')]
        if not drivers:
            print("SQL Server sürücüsü bulunamadı!")
            return False
        return True
    except Exception as e:
        print(f"Sürücü kontrolü sırasında hata: {str(e)}")
        return False

def check_db_config():
    """Veritabanı konfigürasyonunu kontrol eder"""
    required_keys = ['server', 'database', 'user', 'password']
    missing_keys = [key for key in required_keys if not DB_CONFIG.get(key)]
    
    if missing_keys:
        print(f"Eksik veritabanı konfigürasyonu: {missing_keys}")
        return False
        
    print("Veritabanı konfigürasyonu tamam")
    return True

def get_binance_pariteler():
    """
    Binance borsasından tüm aktif pariteleri getirir
    """
    try:
        exchange = ccxt.binance()
        markets = exchange.load_markets()
        all_pariteler = []
        
        # Tekrarlanan pariteleri önlemek için set kullan
        eklenen_pariteler = set()
        
        # Tüm pariteleri işle
        for symbol, market in markets.items():
            if market['active']:
                base = market['base']
                quote = market['quote']
                parite = f"{base}/{quote}"
                tip = 'SPOT' if market['spot'] else 'FUTURES'
                
                # Parite+tip kombinasyonunu kontrol et
                parite_key = f"{parite}_{tip}"
                
                if parite_key not in eklenen_pariteler:
                    parite_info = {
                        'parite': parite,
                        'aktif': 1,
                        'borsa': 'BINANCE',
                        'tip': tip,
                        'ulke': 'Global',
                        'aciklama': f"{base}/{quote} {tip} Pair"
                    }
                    all_pariteler.append(parite_info)
                    eklenen_pariteler.add(parite_key)
        
        return all_pariteler
        
    except Exception as e:
        print(f"Binance veri alma hatası: {str(e)}")
        return []

def get_country_currency(country_name):
    """
    Ülkenin para birimini API'den alır
    """
    try:
        # REST Countries API'den ülke bilgilerini al
        url = f"https://restcountries.com/v3.1/name/{country_name}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                # İlk eşleşen ülkenin para birimini al
                currencies = data[0].get('currencies', {})
                if currencies:
                    # İlk para biriminin kodunu al
                    currency_code = list(currencies.keys())[0]
                    return currency_code
        
        # API'den alınamazsa varsayılan değerleri kullan
        defaults = {
            'turkey': 'TRY',
            'united states': 'USD',
            'japan': 'JPY',
            'china': 'CNY',
            'united kingdom': 'GBP',
            'european union': 'EUR',
            'brazil': 'BRL',
            'australia': 'AUD',
            'canada': 'CAD',
            'switzerland': 'CHF'
        }
        return defaults.get(country_name.lower(), 'USD')
        
    except Exception as e:
        print(f"Para birimi alınamadı ({country_name}): {str(e)}")
        return 'USD'  # Hata durumunda USD kullan

def get_stocks():
    """
    Investpy üzerinden hisse senetlerini getirir ve direkt veritabanına ekler
    """
    try:
        countries = investpy.get_stock_countries()
        toplam_eklenen = 0
        
        for country in countries:
            try:
                stocks = investpy.get_stocks(country=country)
                if len(stocks) == 0:
                    continue
                
                currency = get_country_currency(country)
                print(f"{country}: {len(stocks)} hisse bulundu ({currency})", end=" -> ")
                
                country_stocks = []
                for _, stock in stocks.iterrows():
                    stock_info = {
                        'parite': f"{stock['symbol']}/{currency}",
                        'aktif': 1,
                        'borsa': f"{country.upper()}_STOCK",
                        'tip': 'STOCK',
                        'ulke': country.title(),
                        'aciklama': f"{stock['name']} - {country.title()} Stock"
                    }
                    country_stocks.append(stock_info)
                
                eklenen, guncellenen, silinen = sync_pariteler_to_db(country_stocks)
                print(f"{eklenen} yeni, {guncellenen} güncellenen, {silinen} silinen")
                toplam_eklenen += eklenen
            except Exception as e:
                print(f"{country} hatası: {str(e)}")
                continue
        
        
        return []
        
    except Exception as e:
        print(f"Hata: {str(e)}")
        return []

def get_forex_pariteler():
    """
    Temel forex paritelerini ISO 4217 listesinden dinamik olarak tüm çiftleri döndürür.
    """
    try:
        currency_list = fetch_currency_list()
        fetched_codes = list({code for _, code in currency_list})
    except Exception as e:
        print(f"Para birimi listesi alınamadı: {str(e)}")
        return []

    forex_pariteler = []
    for i in range(len(fetched_codes)):
         for j in range(len(fetched_codes)):
              if i != j:
                  base = fetched_codes[i]
                  quote = fetched_codes[j]
                  forex_pariteler.append({
                      'parite': f"{base}/{quote}",
                      'aktif': 1,
                      'borsa': 'FOREX',
                      'tip': 'SPOT',
                      'ulke': 'Global',
                      'aciklama': f"{base}/{quote} Forex Pair"
                  })
    return forex_pariteler

def get_indices():
    """
    Investpy üzerinden endeksleri getirir ve direkt veritabanına ekler
    """
    try:
        # Forex için kullanılan para birimi listesini al
        currency_list = fetch_currency_list()
        # Para birimi kodlarından ülke listesi oluştur
        countries = set()
        
        # Tüm para birimleri için ülkeleri bul
        for _, currency_code in currency_list:
            try:
                # Para birimi ile ülke arama
                url = f"https://restcountries.com/v3.1/currency/{currency_code}"
                response = requests.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    for country in data:
                        # Ülke adını küçük harfe çevir
                        country_name = country.get('name', {}).get('common', '').lower()
                        if country_name:
                            countries.add(country_name)
            except:
                continue
        
        toplam_eklenen = 0
        
        for country in countries:
            try:
                indices = investpy.get_indices(country=country)
                if len(indices) == 0:
                    continue
                
                currency = get_country_currency(country)
                print(f"{country} endeksleri: {len(indices)} endeks bulundu ({currency})", end=" -> ")
                
                country_indices = []
                for _, index in indices.iterrows():
                    index_info = {
                        'parite': f"{index['symbol']}/{currency}",
                        'aktif': 1,
                        'borsa': f"{country.upper()}_INDEX",
                        'tip': 'INDEX',
                        'ulke': country.title(),
                        'aciklama': f"{index['name']} - {country.title()} Index"
                    }
                    country_indices.append(index_info)
                
                eklenen, guncellenen, silinen = sync_pariteler_to_db(country_indices)
                print(f"{eklenen} yeni, {guncellenen} güncellenen, {silinen} silinen")
                toplam_eklenen += eklenen
            except Exception as e:
                error_msg = str(e)
                if "ERR#0034: country" in error_msg and "not found" in error_msg:
                    continue
                else:
                    print(f"{country} endeks hatası: {error_msg}")
                continue
        
        
        return []
        
    except Exception as e:
        print(f"Endeks hatası: {str(e)}")
        return []

def get_commodities():
    """
    Investpy üzerinden emtia verilerini getirir ve direkt veritabanına ekler
    """
    try:
        commodities = investpy.get_commodities()
        if len(commodities) == 0:
            print("Emtia verisi bulunamadı")
            return []
            
        print(f"Emtia: {len(commodities)} emtia bulundu", end=" -> ")
        
        commodity_list = []
        for _, commodity in commodities.iterrows():
            try:
                # Emtianın işlem gördüğü para birimini al
                currency = commodity.get('currency', 'USD')  # Varsayılan USD
                if not currency:
                    currency = 'USD'
                
                # Sembol oluştur
                name = commodity['name'].strip()
                symbol = name.upper().replace(' ', '_')
                
                # Açıklama oluştur
                description = f"{name} - {commodity.get('group', '').title()} Commodity"
                if commodity.get('country'):
                    description += f" ({commodity['country'].title()})"
                
                commodity_info = {
                    'parite': f"{symbol}/{currency}",
                    'aktif': 1,
                    'borsa': 'COMMODITY',
                    'tip': 'COMMODITY',
                    'ulke': commodity.get('country', 'Global').title(),
                    'aciklama': description
                }
                commodity_list.append(commodity_info)
                
            except Exception as e:
                print(f"Emtia işleme hatası ({commodity.get('name', 'Bilinmeyen')}): {str(e)}")
                continue
        
        eklenen, guncellenen, silinen = sync_pariteler_to_db(commodity_list)
        print(f"{eklenen} yeni, {guncellenen} güncellenen, {silinen} silinen")
        
        
        return commodity_list
        
    except Exception as e:
        print(f"Emtia verisi alınamadı: {str(e)}")
        return []

def get_all_pariteler():
    """
    Tüm pariteleri (Binance, Forex, Endeks, Emtia ve Hisse) getirir
    """
    try:
        all_pariteler = []
        eklenen_pariteler = set()
        
        # Binance paritelerini ekle
        binance_pariteler = get_binance_pariteler()
        for parite in binance_pariteler:
            parite_key = (parite['parite'], parite['borsa'], parite['tip'])
            if parite_key not in eklenen_pariteler:
                all_pariteler.append(parite)
                eklenen_pariteler.add(parite_key)
        
        # Forex paritelerini ekle
        forex_pariteler = get_forex_pariteler()
        for parite in forex_pariteler:
            parite_key = (parite['parite'], parite['borsa'], parite['tip'])
            if parite_key not in eklenen_pariteler:
                all_pariteler.append(parite)
                eklenen_pariteler.add(parite_key)
        
        # Endeksleri ekle
        indices = get_indices()
        for parite in indices:
            parite_key = (parite['parite'], parite['borsa'], parite['tip'])
            if parite_key not in eklenen_pariteler:
                all_pariteler.append(parite)
                eklenen_pariteler.add(parite_key)
        
        # Emtiaları ekle
        commodities = get_commodities()
        for parite in commodities:
            parite_key = (parite['parite'], parite['borsa'], parite['tip'])
            if parite_key not in eklenen_pariteler:
                all_pariteler.append(parite)
                eklenen_pariteler.add(parite_key)
        
        # Hisse senetlerini ekle
        stock_pariteler = get_stocks()
        for parite in stock_pariteler:
            parite_key = (parite['parite'], parite['borsa'], parite['tip'])
            if parite_key not in eklenen_pariteler:
                all_pariteler.append(parite)
                eklenen_pariteler.add(parite_key)
        
        return all_pariteler
        
    except Exception as e:
        print(f"Parite veri alma hatası: {str(e)}")
        return []

def sync_pariteler_to_db(yeni_pariteler):
    """
    Pariteleri veritabanı ile senkronize eder ve değişiklikleri döndürür
    """
    if not yeni_pariteler:
        return 0, 0, 0  # eklenen, güncellenen, silinen
        
    db = None
    conn = None
    try:
        db = Database()
        conn = db.connect()
        if not conn:
            return 0, 0, 0
            
        cursor = conn.cursor()
        
        # Sadece ilgili borsanın mevcut paritelerini al
        cursor.execute("""
            SELECT parite, borsa, tip, aktif, ulke 
            FROM pariteler 
            WHERE borsa = ?
        """, yeni_pariteler[0]['borsa'])
        
        # Mevcut pariteleri set olarak tut
        mevcut_pariteler = {
            (row[0], row[1], row[2], row[3], row[4])  # parite, borsa, tip, aktif, ulke
            for row in cursor.fetchall()
        }
        
        eklenen = 0
        silinen = 0
        
        # Yeni pariteleri işle
        yeni_keys = set()
        for parite in yeni_pariteler:
            try:
                key = (parite['parite'], parite['borsa'], parite['tip'], parite['aktif'], parite['ulke'])
                yeni_keys.add(key)
                
                if key not in mevcut_pariteler:
                    # Yeni parite ekle
                    cursor.execute("""
                        IF NOT EXISTS (
                            SELECT 1 FROM pariteler 
                            WHERE parite = ? AND borsa = ? AND tip = ? AND aktif = ? AND ulke = ?
                        )
                        BEGIN
                            INSERT INTO pariteler (parite, aktif, borsa, tip, ulke, aciklama)
                            VALUES (?, ?, ?, ?, ?, ?)
                        END
                    """, 
                    # Kontrol için
                    parite['parite'], parite['borsa'], parite['tip'], parite['aktif'], parite['ulke'],
                    # Insert için
                    parite['parite'], parite['aktif'], parite['borsa'], 
                    parite['tip'], parite['ulke'], parite['aciklama'])
                    eklenen += 1
                
            except Exception as e:
                print(f"Hata: {parite['parite']} - {str(e)}")
                continue
        
        # Sadece aynı borsadaki kullanılmayan pariteleri sil
        for key in mevcut_pariteler:
            if key not in yeni_keys:
                cursor.execute("""
                    DELETE FROM pariteler 
                    WHERE parite = ? AND borsa = ? AND tip = ? AND aktif = ? AND ulke = ?
                """, key[0], key[1], key[2], key[3], key[4])
                silinen += 1
        
        # Son commit
        conn.commit()
        return eklenen, 0, silinen  # güncelleme yok
        
    except Exception as e:
        if conn:
            conn.rollback()
        return 0, 0, 0
        
    finally:
        if db:
            db.close()

def run_continuous():
    """Sürekli çalışan ana döngü"""
    if not check_sql_driver() or not check_db_config():
        return
    
    print("Parite izleme başladı...")
    
    while True:
        try:
            # 1. Binance pariteleri
            binance_pariteler = get_binance_pariteler()
            if binance_pariteler:
                print(f"Binance: {len(binance_pariteler)} parite bulundu", end=" -> ")
                eklenen, guncellenen, silinen = sync_pariteler_to_db(binance_pariteler)
                print(f"{eklenen} yeni parite, {guncellenen} güncellenen parite, {silinen} silinen parite")
            
            # 2. Forex pariteleri
            forex_pariteler = get_forex_pariteler()
            if forex_pariteler:
                print(f"Forex: {len(forex_pariteler)} parite bulundu", end=" -> ")
                eklenen, guncellenen, silinen = sync_pariteler_to_db(forex_pariteler)
                print(f"{eklenen} yeni parite, {guncellenen} güncellenen parite, {silinen} silinen parite")
            
            # 3. Emtialar (Forex'ten hemen sonra)
            get_commodities()  # Direkt işlem yapacak
            
            # 4. Endeksler
            get_indices()  # Direkt işlem yapacak
            
            # 5. Hisse senetleri
            get_stocks()  # Direkt işlem yapacak
                        
        except KeyboardInterrupt:
            print("\nProgram kullanıcı tarafından durduruldu")
            break
        except Exception as e:
            print(f"İşlem hatası: {str(e)}")

def fetch_currency_list():
    """ISO 4217 para birimleri listesini Wikipedia'dan çeker."""
    url = 'https://en.wikipedia.org/wiki/ISO_4217'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'lxml')
    currencies = []
    
    # Ana para birimleri tablosunu bul (ilk büyük tablo)
    tables = soup.find_all('table', {'class': 'wikitable'})
    if not tables:
        print("Para birimi tablosu bulunamadı.")
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

if __name__ == "__main__":
    run_continuous() 