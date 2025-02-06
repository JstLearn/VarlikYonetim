"""
ccxt kütüphanesi kullanarak Binance borsasından aktif pariteleri veritabanına kaydeder
"""

import ccxt
from utils.database import Database
import sys
import pyodbc
import os
from pathlib import Path
from dotenv import load_dotenv
from utils.config import DB_CONFIG
from datetime import datetime
import time
import investpy
import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag
import yfinance as yf
import logging
import signal

# yfinance ve ilgili logger'ları kapat
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

# Diğer logger'ları da kapat
for name in logging.root.manager.loggerDict:
    if 'yfinance' in name or 'urllib3' in name:
        logging.getLogger(name).setLevel(logging.CRITICAL)
        logging.getLogger(name).propagate = False

# .env dosyasını yükle
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Global değişken
should_exit = False

def signal_handler(signum, frame):
    """Sinyal yakalayıcı"""
    global should_exit
    should_exit = True
    log("\nProgram durduruluyor, lütfen bekleyin...")

# Signal handler'ı kaydet
signal.signal(signal.SIGINT, signal_handler)

def log(message):
    """Zaman damgalı log mesajı yazdırır"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {message}")

def check_sql_driver():
    """SQL Server sürücülerini kontrol eder"""
    try:
        drivers = [x for x in pyodbc.drivers() if x.startswith('SQL Server')]
        if not drivers:
            log("SQL Server sürücüsü bulunamadı!")
            return False
        return True
    except Exception as e:
        log(f"Sürücü kontrolü sırasında hata: {str(e)}")
        return False

def check_db_config():
    """Veritabanı konfigürasyonunu kontrol eder"""
    required_keys = ['server', 'database', 'user', 'password']
    missing_keys = [key for key in required_keys if not DB_CONFIG.get(key)]
    
    if missing_keys:
        log(f"Eksik veritabanı konfigürasyonu: {missing_keys}")
        return False
        
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
        log(f"Binance veri alma hatası: {str(e)}")
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
        log(f"Para birimi alınamadı ({country_name}): {str(e)}")
        return 'USD'  # Hata durumunda USD kullan

def get_stocks():
    """
    Investpy üzerinden hisse senetlerini getirir ve direkt veritabanına ekler
    """
    try:
        countries = investpy.get_stock_countries()
        
        for country in countries:
            try:
                stocks = investpy.get_stocks(country=country)
                if len(stocks) == 0:
                    continue
                
                currency = get_country_currency(country)
                eklenen_sayisi = 0
                
                # Ülke adını standartlaştır
                country_name = country.upper()
                
                for _, stock in stocks.iterrows():
                    try:
                        yf_symbol = stock['symbol'].strip().upper()
                        if not yf_symbol:
                            continue
                            
                        # Önce veritabanında bu sembolü kontrol et
                        db = Database()
                        conn = db.connect()
                        if conn:
                            cursor = conn.cursor()
                            cursor.execute("""
                                SELECT borsa FROM pariteler 
                                WHERE parite LIKE ? AND tip = 'STOCK'
                            """, (f"{yf_symbol}/%",))
                            
                            existing_exchange = cursor.fetchone()
                            db.disconnect()
                            
                            if existing_exchange:
                                exchange = existing_exchange[0]
                            else:
                                # Önce Yahoo Finance'den borsa adını almaya çalış
                                try:
                                    stock_ticker = yf.Ticker(yf_symbol)
                                    info = stock_ticker.info
                                    if info and 'exchange' in info:
                                        exchange = info['exchange'].upper()
                                    else:
                                        exchange = f"{country_name}_STOCK"
                                except:
                                    exchange = f"{country_name}_STOCK"
                        else:
                            exchange = f"{country_name}_STOCK"
                            
                        stock_info = [{
                            'parite': f"{yf_symbol}/{currency}",
                            'aktif': 1,
                            'borsa': exchange,
                            'tip': 'STOCK',
                            'ulke': country_name,
                            'aciklama': f"{stock['name']} - {country_name} Stock"
                        }]
                        
                        # Her hisseyi tek tek kaydet
                        eklenen, guncellenen, silinen = sync_pariteler_to_db(stock_info)
                        eklenen_sayisi += eklenen
                        
                    except Exception as e:
                        continue
                
                log(f"{country} Stocks: {len(stocks)} parite bulundu -> {eklenen_sayisi} yeni, 0 güncellenen, 0 silinen")
                
            except Exception as e:
                log(f"{country} hatası: {str(e)}")
                continue
        
        return []
        
    except Exception as e:
        log(f"Hata: {str(e)}")
        return []

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
                        # Ülke adını büyük harfe çevir
                        country_name = country.get('name', {}).get('common', '').upper()
                        if country_name:
                            countries.add(country_name)
            except:
                continue
        
        toplam_eklenen = 0
        
        for country in countries:
            try:
                indices = investpy.get_indices(country=country.lower())
                if len(indices) == 0:
                    continue
                
                currency = get_country_currency(country.lower())
                
                for _, index in indices.iterrows():
                    try:
                        yf_symbol = index['symbol'].strip().upper()
                        if not yf_symbol:
                            continue
                            
                        # Önce veritabanında bu sembolü kontrol et
                        db = Database()
                        conn = db.connect()
                        if conn:
                            cursor = conn.cursor()
                            cursor.execute("""
                                SELECT borsa FROM pariteler 
                                WHERE parite LIKE ? AND tip = 'INDEX'
                            """, (f"{yf_symbol}/%",))
                            
                            existing_exchange = cursor.fetchone()
                            db.disconnect()
                            
                            if existing_exchange:
                                exchange = existing_exchange[0]
                            else:
                                # Önce Yahoo Finance'den borsa adını almaya çalış
                                try:
                                    index_ticker = yf.Ticker(yf_symbol)
                                    info = index_ticker.info
                                    if info and 'exchange' in info:
                                        exchange = info['exchange'].upper()
                                    else:
                                        exchange = f"{country}_INDEX"
                                except:
                                    exchange = f"{country}_INDEX"
                        else:
                            exchange = f"{country}_INDEX"
                        
                        index_info = [{
                            'parite': f"{yf_symbol}/{currency}",
                            'aktif': 1,
                            'borsa': exchange,
                            'tip': 'INDEX',
                            'ulke': country,
                            'aciklama': f"{index['name']} - {country} Index"
                        }]
                        
                        # Her bir endeks için hemen veritabanına kaydet
                        eklenen, guncellenen, silinen = sync_pariteler_to_db(index_info)
                        toplam_eklenen += eklenen
                        
                    except:
                        continue
                
                log(f"{country} endeksleri: {len(indices)} endeks bulundu ({currency}) -> {toplam_eklenen} yeni eklendi")
                toplam_eklenen = 0  # Ülke bazlı sayacı sıfırla
                
            except Exception as e:
                error_msg = str(e)
                if "ERR#0034: country" in error_msg and "not found" in error_msg:
                    continue
                else:
                    log(f"{country} endeks hatası: {error_msg}")
                continue
        
        return []
        
    except Exception as e:
        log(f"Endeks hatası: {str(e)}")
        return []

def get_forex_pariteler():
    """
    Temel forex paritelerini ISO 4217 listesinden dinamik olarak tüm çiftleri döndürür.
    """
    try:
        currency_list = fetch_currency_list()
        fetched_codes = list({code for _, code in currency_list})
    except Exception as e:
        log(f"Para birimi listesi alınamadı: {str(e)}")
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

def get_commodities():
    """
    Investpy üzerinden emtia verilerini getirir ve direkt veritabanına ekler
    """
    try:
        commodities = investpy.get_commodities()
        if len(commodities) == 0:
            log("Emtia verisi bulunamadı")
            return []
            
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
                log(f"Emtia işleme hatası ({commodity.get('name', 'Bilinmeyen')}): {str(e)}")
                continue
        
        eklenen, guncellenen, silinen = sync_pariteler_to_db(commodity_list)
        log(f"Emtia: {len(commodities)} emtia bulundu -> {eklenen} yeni, {guncellenen} güncellenen, {silinen} silinen")
        
        
        return commodity_list
        
    except Exception as e:
        log(f"Emtia verisi alınamadı: {str(e)}")
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

        # Hisse senetlerini ekle
        stock_pariteler = get_stocks()
        for parite in stock_pariteler:
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
        

        
        return all_pariteler
        
    except Exception as e:
        log(f"Parite veri alma hatası: {str(e)}")
        return []

def sync_pariteler_to_db(yeni_pariteler):
    """
    Pariteleri veritabanı ile senkronize eder ve değişiklikleri döndürür.
    Her bir pariteyi tek tek işler ve commit eder.
    """
    global should_exit
    if not yeni_pariteler or should_exit:
        return (0, 0, 0)  # eklenen, güncellenen, silinen
        
    db = None
    conn = None
    try:
        db = Database()
        conn = db.connect()
        if not conn:
            log("Veritabanı bağlantısı kurulamadı")
            return (0, 0, 0)
            
        cursor = conn.cursor()
        eklenen = 0
        
        # Her bir pariteyi tek tek işle
        for parite in yeni_pariteler:
            if should_exit:
                log("Program durduruluyor...")
                return (eklenen, 0, 0)
                
            try:
                # Parite zaten var mı kontrol et
                cursor.execute("""
                    SELECT 1 FROM pariteler 
                    WHERE parite = ? AND borsa = ? AND tip = ? AND aktif = ? AND ulke = ?
                """, 
                parite['parite'], parite['borsa'], parite['tip'], 
                parite['aktif'], parite['ulke'])
                
                exists = cursor.fetchone() is not None
                
                if not exists:
                    cursor.execute("""
                        INSERT INTO pariteler (parite, aktif, borsa, tip, ulke, aciklama)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, 
                    parite['parite'], parite['aktif'], parite['borsa'], 
                    parite['tip'], parite['ulke'], parite['aciklama'])
                    
                    # Her ekleme sonrası hemen commit et
                    conn.commit()
                    eklenen += 1
                    
                    # Her 100 işlemde bir log
                    if eklenen % 100 == 0 and not should_exit:
                        log(f"İşlenen: {eklenen} parite")
                
            except Exception as e:
                if not should_exit:
                    log(f"Parite ekleme hatası ({parite['parite']}): {str(e)}")
                continue
            
        return (eklenen, 0, 0)  # güncelleme ve silme yok
            
    except Exception as e:
        if not should_exit:
            log(f"Veritabanı işlem hatası: {str(e)}")
        return (0, 0, 0)
        
    finally:
        if db:
            db.disconnect()

def update_exchange_names():
    """
    Borsa adında _STOCK geçen kayıtları Yahoo Finance'den tekrar kontrol eder
    ve borsa adını güncelleyebilirse günceller.
    """
    global should_exit
    try:
        db = Database()
        conn = db.connect()
        if not conn:
            log("Veritabanı bağlantısı kurulamadı")
            return
            
        cursor = conn.cursor()
        
        # _STOCK içeren kayıtları al
        cursor.execute("""
            SELECT parite, borsa, ulke 
            FROM pariteler 
            WHERE borsa LIKE '%_STOCK' AND tip = 'STOCK'
            ORDER BY kayit_tarihi ASC  -- En eski kayıtlardan başla
        """)
        
        records = cursor.fetchall()
        guncellenen = 0
        
        for record in records:
            if should_exit:
                log("Program durduruluyor...")
                return
                
            try:
                parite, current_exchange, ulke = record
                symbol = parite.split('/')[0]  # Sembolü al
                
                # Yahoo Finance'den borsa adını almaya çalış
                try:
                    stock_ticker = yf.Ticker(symbol)
                    info = stock_ticker.info
                    if info and 'exchange' in info:
                        new_exchange = info['exchange'].upper()
                        if new_exchange != current_exchange:
                            # Her kayıt için yeni bir bağlantı ve cursor oluştur
                            update_db = Database()
                            update_conn = update_db.connect()
                            if update_conn:
                                update_cursor = update_conn.cursor()
                                
                                # Borsa adını ve kayıt tarihini güncelle
                                update_cursor.execute("""
                                    UPDATE pariteler 
                                    SET borsa = ?, kayit_tarihi = GETDATE()
                                    WHERE parite = ? AND borsa = ? AND tip = 'STOCK'
                                """, new_exchange, parite, current_exchange)
                                
                                # Hemen commit et
                                update_conn.commit()
                                update_db.disconnect()
                                
                                guncellenen += 1
                                log(f"Borsa güncellendi: {parite} -> {new_exchange}")
                except Exception as e:
                    if not should_exit:
                        log(f"Yahoo Finance hatası ({parite}): {str(e)}")
                    continue
                    
            except Exception as e:
                if not should_exit:
                    log(f"Borsa güncelleme hatası ({parite}): {str(e)}")
                continue
        
        if not should_exit:
            log(f"Borsa güncelleme tamamlandı: {guncellenen} kayıt güncellendi")
            
    except Exception as e:
        if not should_exit:
            log(f"Borsa güncelleme işlemi hatası: {str(e)}")
    finally:
        if db:
            db.disconnect()

def run_continuous():
    """Sürekli çalışan ana döngü"""
    global should_exit
    
    if not check_sql_driver() or not check_db_config():
        return
    
    log("Parite izleme başladı...")
    
    while not should_exit:
        try:
            # 0. Borsa isimlerini güncelleme kontrolü
            if should_exit: break
            log("Borsa isimleri kontrol ediliyor...")
            # update_exchange_names()
            
            # 1. Binance pariteleri
            if should_exit: break
            log("Binance pariteleri alınıyor...")
            binance_pariteler = get_binance_pariteler()
            if binance_pariteler:
                eklenen, guncellenen, silinen = sync_pariteler_to_db(binance_pariteler)
                if not should_exit:
                    log(f"Binance: {len(binance_pariteler)} parite bulundu -> {eklenen} yeni, {guncellenen} güncellenen, {silinen} silinen")
            
            # 2. Forex pariteleri
            if should_exit: break
            log("Forex pariteleri alınıyor...")
            forex_pariteler = get_forex_pariteler()
            if forex_pariteler:
                eklenen, guncellenen, silinen = sync_pariteler_to_db(forex_pariteler)
                if not should_exit:
                    log(f"Forex: {len(forex_pariteler)} parite bulundu -> {eklenen} yeni, {guncellenen} güncellenen, {silinen} silinen")

            # 3. Hisse senetleri
            if should_exit: break
            log("Hisse senetleri alınıyor...")
            get_stocks()  # Direkt işlem yapacak
            
            # 4. Endeksler
            if should_exit: break
            log("Endeksler alınıyor...")
            get_indices()  # Direkt işlem yapacak
            
            # 5. Emtialar
            if should_exit: break
            log("Emtialar alınıyor...")
            get_commodities()  # Direkt işlem yapacak
            
            if should_exit: 
                log("Program durduruluyor...")
                break
            
            # İşlem tamamlandı, 1 saat bekle
            log("Tüm işlemler tamamlandı. 1 saat bekleniyor...")
            for i in range(3600):  # 1 saat = 3600 saniye
                if should_exit: 
                    log("Program durduruluyor...")
                    break
                time.sleep(1)
                if i % 60 == 0 and not should_exit:  # Her dakikada bir log
                    log(f"Beklemede: {60 - (i // 60)} dakika kaldı")
                    
        except Exception as e:
            if not should_exit:
                log(f"İşlem hatası: {str(e)}")
                time.sleep(5)  # Hata durumunda 5 saniye bekle
    
    log("Program sonlandırıldı")

def fetch_currency_list():
    """ISO 4217 para birimleri listesini Wikipedia'dan çeker."""
    url = 'https://en.wikipedia.org/wiki/ISO_4217'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'lxml')
    currencies = []
    
    # Ana para birimleri tablosunu bul (ilk büyük tablo)
    tables = soup.find_all('table', {'class': 'wikitable'})
    if not tables:
        log("Para birimi tablosu bulunamadı.")
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