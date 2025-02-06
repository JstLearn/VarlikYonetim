"""
Hisse senetlerini toplayan sınıf
"""

import investpy
import yfinance as yf
import requests
import pandas as pd
from utils.database import Database
from bs4 import BeautifulSoup, Tag
import logging

# yfinance ve ilgili logger'ları kapat
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

# Diğer logger'ları da kapat
for name in logging.root.manager.loggerDict:
    if 'yfinance' in name or 'urllib3' in name:
        logging.getLogger(name).setLevel(logging.CRITICAL)
        logging.getLogger(name).propagate = False

class StockCollector:
    def __init__(self):
        pass

    def get_exchange_info(self, symbol, country_name):
        """
        Borsa bilgisini önce yfinance sonra investpy'dan almaya çalışır
        """
        try:
            # Önce yfinance'den dene
            stock_ticker = yf.Ticker(symbol)
            info = stock_ticker.info
            if info and 'exchange' in info:
                return info['exchange'].upper()
                
            # Yfinance'den bulunamazsa investpy'dan dene
            stocks = investpy.get_stocks(country=country_name)
            stock_info = stocks[stocks['symbol'].str.upper() == symbol.upper()]
            if not stock_info.empty:
                exchange = stock_info.iloc[0].get('exchange', '')
                if exchange:
                    return exchange.upper()
                    
            return f"{country_name.upper()}_STOCK"
            
        except:
            return f"{country_name.upper()}_STOCK"

    def fetch_currency_list(self):
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
                    if not currency:  # Para birimi bulunamadıysa bu ülkeyi atla
                        print(f"{country} için para birimi bulunamadı, atlanıyor...")
                        continue
                        
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
                                
                                if existing_exchange:
                                    exchange = existing_exchange[0]
                                    # Eğer mevcut borsa _STOCK içeriyorsa yeniden kontrol et
                                    if '_STOCK' in exchange:
                                        try:
                                            new_exchange = self.get_exchange_info(yf_symbol, country)
                                            if new_exchange != exchange and '_STOCK' not in new_exchange:
                                                # Borsa adını ve kayıt tarihini güncelle
                                                cursor.execute("""
                                                    UPDATE pariteler 
                                                    SET borsa = ?, kayit_tarihi = GETDATE()
                                                    WHERE parite LIKE ? AND tip = 'STOCK'
                                                """, (new_exchange, f"{yf_symbol}/%"))
                                                db.commit()
                                                print(f"Borsa güncellendi: {yf_symbol} -> {new_exchange}")
                                                exchange = new_exchange
                                        except:
                                            pass  # Güncelleme başarısız olursa mevcut borsa adını kullan
                                    
                                    db.disconnect()
                                    continue  # Sembol zaten var, sonraki sembole geç
                                
                                db.disconnect()
                                
                                # Yeni kayıt için borsa adını al
                                exchange = self.get_exchange_info(yf_symbol, country)
                            else:
                                exchange = f"{country_name.upper()}_STOCK"
                                
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
                            print(f"Hisse işleme hatası ({yf_symbol}): {str(e)}")
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