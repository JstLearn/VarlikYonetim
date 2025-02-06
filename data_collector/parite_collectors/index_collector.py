"""
Borsa endekslerini toplayan sınıf
"""

import investpy
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from utils.database import Database

class IndexCollector:
    def __init__(self):
        pass

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

    def collect_pariteler(self):
        """
        Investpy üzerinden endeksleri getirir ve veritabanına ekler
        """
        try:
            # Forex için kullanılan para birimi listesini al
            currency_list = self.fetch_currency_list()
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
                    
                    currency = self.get_country_currency(country.lower())
                    
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
                            eklenen, guncellenen, silinen = self.sync_pariteler_to_db(index_info)
                            toplam_eklenen += eklenen
                            
                        except:
                            continue
                    
                    print(f"{country} endeksleri: {len(indices)} endeks bulundu ({currency}) -> {toplam_eklenen} yeni eklendi")
                    toplam_eklenen = 0  # Ülke bazlı sayacı sıfırla
                    
                except Exception as e:
                    error_msg = str(e)
                    if "ERR#0034: country" in error_msg and "not found" in error_msg:
                        continue
                    else:
                        print(f"{country} endeks hatası: {error_msg}")
                    continue
            
        except Exception as e:
            print(f"Endeks verisi alınamadı: {str(e)}")

if __name__ == "__main__":
    collector = IndexCollector()
    collector.collect_pariteler()