"""
Forex paritelerini toplayan sınıf
"""

import investpy
import requests
from bs4 import BeautifulSoup, Tag
from utils.database import Database

class ForexCollector:
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
        """Forex paritelerini getirir ve veritabanına ekler"""
        try:
            # Para birimi listesini al
            currency_list = self.fetch_currency_list()
            fetched_codes = list({code for _, code in currency_list})
            
            if not fetched_codes:
                print("Para birimi listesi alınamadı")
                return
                
            parite_list = []
            # Tüm olası para birimi çiftlerini oluştur
            for i in range(len(fetched_codes)):
                for j in range(len(fetched_codes)):
                    if i != j:
                        base = fetched_codes[i]
                        quote = fetched_codes[j]
                        parite_info = {
                            'parite': f"{base}/{quote}",
                            'aktif': 1,
                            'borsa': 'FOREX',
                            'tip': 'SPOT',
                            'ulke': 'Global',
                            'aciklama': f"{base}/{quote} Forex Pair"
                        }
                        parite_list.append(parite_info)
            
            # Veritabanına kaydet
            if parite_list:
                eklenen, guncellenen, silinen = self.sync_pariteler_to_db(parite_list)
                print(f"Forex: {len(parite_list)} parite bulundu -> {eklenen} yeni eklendi")
            
        except Exception as e:
            print(f"Forex verisi alınamadı: {str(e)}")

if __name__ == "__main__":
    collector = ForexCollector()
    collector.collect_pariteler() 