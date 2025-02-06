"""
Emtia paritelerini toplayan sınıf
"""

import investpy
import signal
import time
from utils.database import Database

class CommodityCollector:
    def __init__(self):
        self.should_exit = False
        signal.signal(signal.SIGINT, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Sinyal yakalayıcı"""
        self.should_exit = True
        print("Program durduruluyor, lütfen bekleyin...")

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
                if self.should_exit:
                    break
                    
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
        """Investpy üzerinden emtia verilerini getirir ve veritabanına ekler"""
        try:
            commodities = investpy.get_commodities()
            if len(commodities) == 0:
                print("Emtia verisi bulunamadı")
                return
                
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
                    if not self.should_exit:
                        print(f"Emtia işleme hatası ({commodity.get('name', 'Bilinmeyen')}): {str(e)}")
                    continue
            
            # Veritabanına kaydet
            if commodity_list:
                eklenen, guncellenen, silinen = self.sync_pariteler_to_db(commodity_list)
                if not self.should_exit:
                    print(f"Emtia: {len(commodities)} emtia bulundu -> {eklenen} yeni eklendi")
            
        except Exception as e:
            if not self.should_exit:
                print(f"Emtia verisi alınamadı: {str(e)}")

    def run_continuous(self, interval=3600):
        """Sürekli çalışan ana döngü"""
        print("Parite izleme başladı...")
        
        while not self.should_exit:
            try:
                self.collect_pariteler()
                
                if self.should_exit:
                    print("Program durduruluyor...")
                    break
                    
                print(f"Tüm işlemler tamamlandı. {interval//60} dakika bekleniyor...")
                for i in range(interval):
                    if self.should_exit:
                        print("Program durduruluyor...")
                        break
                    time.sleep(1)
                    
            except Exception as e:
                if not self.should_exit:
                    print(f"İşlem hatası: {str(e)}")
                    time.sleep(5)
        
        print("Program sonlandırıldı")

if __name__ == "__main__":
    collector = CommodityCollector()
    collector.run_continuous() 