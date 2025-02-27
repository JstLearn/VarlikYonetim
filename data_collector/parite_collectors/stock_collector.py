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
import warnings
import urllib3

# Requests uyarılarını bastır
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)
requests.packages.urllib3.disable_warnings()

# yfinance ve ilgili logger'ları kapat
logging.getLogger('yfinance').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)
logging.getLogger('requests').setLevel(logging.CRITICAL)

# Diğer logger'ları da kapat
for name in logging.root.manager.loggerDict:
    if 'yfinance' in name or 'urllib3' in name or 'requests' in name:
        logging.getLogger(name).setLevel(logging.CRITICAL)
        logging.getLogger(name).propagate = False

class StockCollector:
    def __init__(self):
        pass

    def get_exchange_info(self, symbol, country_name):
        """
        Borsa bilgisini önce yfinance sonra investpy'dan almaya çalışır
        Birden fazla borsa varsa virgülle ayırarak birleştirir
        """
        exchanges = set()  # Tekrar eden borsaları önlemek için set kullan
        
        try:
            # Önce yfinance'den dene
            try:
                # Standart çıktıyı geçici olarak yönlendir, hata mesajlarını gösterme
                import io
                import sys
                original_stderr = sys.stderr
                sys.stderr = io.StringIO()
                
                stock_ticker = yf.Ticker(symbol)
                info = stock_ticker.info
                
                # Standart hata çıktısını geri yükle
                sys.stderr = original_stderr
                
                if info:
                    # Ana borsa
                    if 'exchange' in info:
                        exchanges.add(info['exchange'].upper())
                    # İkincil borsalar
                    if 'otherExchanges' in info:
                        other = info['otherExchanges']
                        if isinstance(other, str):
                            for ex in other.split(','):
                                ex = ex.strip().upper()
                                if ex:  # Boş string kontrolü
                                    exchanges.add(ex)
            except Exception:
                # Sessizce devam et, hata mesajını gösterme
                pass
            
            # Investpy'dan dene
            try:
                stocks = investpy.get_stocks(country=country_name)
                stock_info = stocks[stocks['symbol'].str.upper() == symbol.upper()]
                if not stock_info.empty:
                    for _, row in stock_info.iterrows():
                        # Exchange sütunundan borsa bilgisi
                        exchange = row.get('exchange', '')
                        if exchange:
                            exchanges.add(exchange.upper())
                        # Market sütunundan borsa bilgisi
                        market = row.get('market', '')
                        if market and market != 'GLOBAL_INDICES':
                            exchanges.add(market.upper())
                        # Name sütunundan borsa bilgisi (genelde parantez içinde olur)
                        name = row.get('name', '')
                        if name and '(' in name and ')' in name:
                            exchange_part = name[name.find('(')+1:name.find(')')].strip()
                            if exchange_part and exchange_part != 'GLOBAL_INDICES':
                                exchanges.add(exchange_part.upper())
            except:
                pass
            
            # Hala borsa bulunamadıysa veritabanından o ülkenin endeks borsasını kontrol et
            if not exchanges:
                db = Database()
                conn = db.connect()
                if conn:
                    try:
                        cursor = db.cursor()
                        if cursor:
                            cursor.execute("""
                                SELECT top 1 borsa 
                                FROM [VARLIK_YONETIM].[dbo].[pariteler] WITH (NOLOCK)
                                WHERE tip = 'INDEX' 
                                AND ulke = ? 
                                AND borsa not like '%_INDICES%' 
                                AND borsa not like '%_STOCK%'
                            """, (country_name.upper(),))
                            
                            result = cursor.fetchone()
                            if result and result[0]:
                                return result[0].upper()  # Direkt olarak veritabanından gelen borsa adını döndür
                    except:
                        pass
                    finally:
                        db.disconnect()
            
            # Eğer hiç borsa bulunamadıysa ülke_STOCK kullan
            if not exchanges:
                return f"{country_name.upper()}_STOCK"
                
            # Bulunan tüm borsaları virgülle birleştir
            return ','.join(sorted(exchanges))
            
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
        Ülkenin para birimini investpy'dan alır
        """
        try:
            # Önce ülke adını küçük harfe çevir
            country_name = country_name.lower()
            
            # Investpy'dan hisse senetlerini al
            stocks = investpy.get_stocks(country=country_name)
            if len(stocks) > 0 and 'currency' in stocks.columns:
                # İlk hissenin para birimini al
                currency = stocks.iloc[0]['currency'].upper()
                if currency and currency != 'USD':
                    return currency
            
            # Bulunamazsa endeksleri dene
            indices = investpy.get_indices(country=country_name)
            if len(indices) > 0 and 'currency' in indices.columns:
                # İlk endeksin para birimini al
                currency = indices.iloc[0]['currency'].upper()
                if currency and currency != 'USD':
                    return currency
            
            # Bulunamazsa Wikipedia'dan dene
            currency_list = self.fetch_currency_list()
            for currency_name, currency_code in currency_list:
                currency_name = currency_name.lower()
                if (country_name in currency_name or 
                    country_name.replace(' ', '') in currency_name.replace(' ', '')):
                    return currency_code
            
            # Yine bulunamazsa USD döndür
            return 'USD'
            
        except Exception as e:
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
                        SELECT 1 FROM pariteler WITH (NOLOCK)
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
                        
                        # Yeni eklenen hisse senedi için log mesajı göster
                        # "_STOCK" ile biten borsa adları için mesaj gösterme
                        if parite['tip'] == 'STOCK' and not parite['borsa'].endswith('_STOCK'):
                            sembol = parite['parite'].split('/')[0] if '/' in parite['parite'] else parite['parite']
                            print(f"{sembol} Hissesi {parite['borsa']} Borsasında listelendi")
                        
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
            
            genel_toplam = 0
            genel_eklenen = 0
            
            for country in countries:
                try:
                    stocks = investpy.get_stocks(country=country)
                    if len(stocks) == 0:
                        continue
                    
                    currency = self.get_country_currency(country)
                    if not currency:  # Para birimi bulunamadıysa bu ülkeyi atla
                        print(f"{country} için para birimi bulunamadı, atlanıyor...")
                        continue
                        
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
                                    SELECT borsa FROM pariteler WITH (NOLOCK)
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
                                                # Borsa adını sadece ilk kısmı alacak şekilde ayır
                                                display_exchange = new_exchange.split('/')[0] if '/' in new_exchange else new_exchange
                                                
                                                # Borsa adını ve kayıt tarihini güncelle
                                                cursor.execute("""
                                                    UPDATE p
                                                    SET p.borsa = ?, p.kayit_tarihi = GETDATE()
                                                    FROM [VARLIK_YONETIM].[dbo].[pariteler] p WITH (NOLOCK)
                                                    WHERE p.parite LIKE ? AND p.tip = 'STOCK'
                                                """, (display_exchange, f"{yf_symbol}/%"))
                                                db.commit()
                                                
                                                print(f"{yf_symbol} Hissesi {display_exchange} Borsasında listelendi")
                                                exchange = display_exchange
                                        except:
                                            pass  # Güncelleme başarısız olursa mevcut borsa adını kullan
                                    
                                    db.disconnect()
                                    continue  # Sembol zaten var, sonraki sembole geç
                                
                                db.disconnect()
                                
                                # Yeni kayıt için borsa adını al
                                exchange = self.get_exchange_info(yf_symbol, country)
                            else:
                                exchange = f"{country_name.upper()}_STOCK"
                                
                            # Borsa adını sadece ilk kısmı alacak şekilde ayır
                            display_exchange = exchange.split('/')[0] if '/' in exchange else exchange
                                
                            stock_info = [{
                                'parite': f"{yf_symbol}/{currency}",
                                'aktif': 1,
                                'borsa': display_exchange,
                                'tip': 'STOCK',
                                'ulke': country_name,
                                'aciklama': f"{stock['name']} - {country_name} Stock"
                            }]
                            
                            # Her hisseyi tek tek kaydet
                            eklenen, guncellenen, silinen = self.sync_pariteler_to_db(stock_info)
                            genel_eklenen += eklenen
                            
                        except Exception as e:
                            print(f"Hisse işleme hatası ({yf_symbol}): {str(e)}")
                            continue
                    
                    genel_toplam += len(stocks)
                    
                except Exception as e:
                    print(f"{country} hatası: {str(e)}")
                    continue
            
            print(f"STOCK: {genel_toplam} parite bulundu -> {genel_eklenen} yeni eklendi")
            
        except Exception as e:
            print(f"Hisse senedi verisi alınamadı: {str(e)}")

if __name__ == "__main__":
    collector = StockCollector()
    collector.collect_pariteler() 