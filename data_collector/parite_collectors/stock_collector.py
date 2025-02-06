"""
Hisse senetlerini toplayan sınıf
"""

import investpy
import yfinance as yf
import requests
import pandas as pd
from utils.database import Database

class StockCollector:
    def __init__(self):
        self.major_exchanges = {
            'NYSE': {'country': 'USA', 'currency': 'USD'},
            'NASDAQ': {'country': 'USA', 'currency': 'USD'},
            'LSE': {'country': 'UK', 'currency': 'GBP'},
            'TSE': {'country': 'Japan', 'currency': 'JPY'},
            'SSE': {'country': 'China', 'currency': 'CNY'},
            'HKEX': {'country': 'Hong Kong', 'currency': 'HKD'},
            'BIST': {'country': 'Turkey', 'currency': 'TRY'},
            'BSE': {'country': 'India', 'currency': 'INR'},
            'XETRA': {'country': 'Germany', 'currency': 'EUR'},
            'EURONEXT': {'country': 'France', 'currency': 'EUR'}
        }

    def get_country_currency(self, country_name):
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

    def sync_pariteler_to_db(self, yeni_pariteler):
        """Pariteleri veritabanına kaydeder"""
        if not yeni_pariteler:
            return (0, 0, 0)
            
        db = None
        try:
            db = Database()
            if not db.connect():
                print("Veritabanı bağlantısı kurulamadı")
                return (0, 0, 0)
                
            cursor = db.cursor()
            if not cursor:
                print("Veritabanı cursor'ı oluşturulamadı")
                return (0, 0, 0)
                
            eklenen = 0
            
            for parite in yeni_pariteler:
                try:
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
                        
                        db.commit()
                        eklenen += 1
                        
                except Exception as e:
                    print(f"Parite ekleme hatası ({parite['parite']}): {str(e)}")
                    continue
                    
            return (eklenen, 0, 0)
            
        except Exception as e:
            print(f"Veritabanı işlem hatası: {str(e)}")
            return (0, 0, 0)
            
        finally:
            if db:
                db.disconnect()

    def collect_pariteler(self):
        """
        Investpy üzerinden hisse senetlerini getirir ve veritabanına ekler
        """
        try:
            countries = investpy.get_stock_countries()
            
            for country in countries:
                try:
                    stocks = investpy.get_stocks(country=country)
                    if len(stocks) == 0:
                        continue
                    
                    currency = self.get_country_currency(country)
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
                            eklenen, guncellenen, silinen = self.sync_pariteler_to_db(stock_info)
                            eklenen_sayisi += eklenen
                            
                        except Exception as e:
                            continue
                    
                    print(f"{country} Stocks: {len(stocks)} parite bulundu -> {eklenen_sayisi} yeni eklendi")
                    
                except Exception as e:
                    print(f"{country} hatası: {str(e)}")
                    continue
            
        except Exception as e:
            print(f"Hisse senedi verisi alınamadı: {str(e)}")

if __name__ == "__main__":
    collector = StockCollector()
    collector.collect_pariteler() 