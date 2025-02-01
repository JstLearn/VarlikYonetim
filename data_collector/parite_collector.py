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
        
        if toplam_eklenen > 0:
            print(f"\nToplam {toplam_eklenen} yeni hisse eklendi")
        
        return []
        
    except Exception as e:
        print(f"Hata: {str(e)}")
        return []

def get_forex_pariteler():
    """
    Temel forex paritelerini döndürür
    """
    # Temel forex pariteleri
    forex_pairs = [
        # Major pairs
        ('EUR', 'USD'), ('GBP', 'USD'), ('USD', 'JPY'), ('USD', 'CHF'),
        ('AUD', 'USD'), ('USD', 'CAD'), ('NZD', 'USD'),
        
        # Minor pairs (Crosses)
        ('EUR', 'GBP'), ('EUR', 'JPY'), ('EUR', 'CHF'), ('EUR', 'AUD'),
        ('GBP', 'JPY'), ('GBP', 'CHF'), ('GBP', 'AUD'),
        ('AUD', 'JPY'), ('AUD', 'CHF'), ('AUD', 'CAD'),
        ('NZD', 'JPY'), ('NZD', 'CHF'),
        ('CAD', 'JPY'), ('CAD', 'CHF'),
        
        # Exotic pairs
        ('USD', 'TRY'), ('EUR', 'TRY'), ('GBP', 'TRY'),
        ('USD', 'SGD'), ('USD', 'HKD'), ('USD', 'ZAR'),
        ('EUR', 'NOK'), ('EUR', 'SEK'), ('EUR', 'DKK'),
        ('USD', 'MXN'), ('USD', 'PLN'), ('USD', 'HUF')
    ]
    
    forex_pariteler = []
    for base, quote in forex_pairs:
        parite = f"{base}/{quote}"
        parite_info = {
            'parite': parite,
            'aktif': 1,
            'borsa': 'FOREX',
            'tip': 'SPOT',
            'ulke': 'Global',
            'aciklama': f"{base}/{quote} Forex Pair"
        }
        forex_pariteler.append(parite_info)
    
    return forex_pariteler

def get_all_pariteler():
    """
    Tüm pariteleri (Binance, Forex ve Hisse) getirir
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
            
            
            # 3. Hisse senetleri
            get_stocks()  # Direkt işlem yapacak
                        
        except KeyboardInterrupt:
            print("\nProgram kullanıcı tarafından durduruldu")
            break
        except Exception as e:
            print(f"İşlem hatası: {str(e)}")

if __name__ == "__main__":
    run_continuous() 