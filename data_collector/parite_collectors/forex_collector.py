"""
Forex paritelerini toplayan sınıf
"""

import investpy
from utils.database import Database

class ForexCollector:
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

    def collect_pariteler(self):
        """Forex paritelerini getirir ve veritabanına ekler"""
        try:
            # Tüm para birimlerini al
            currencies = investpy.get_currency_crosses_list()
            if not currencies:
                print("Forex verisi bulunamadı")
                return
                
            parite_list = []
            for currency in currencies:
                try:
                    # Para birimi çiftini ayır
                    base, quote = currency.split('/')
                    
                    # Parite bilgilerini oluştur
                    parite_info = {
                        'parite': currency,
                        'aktif': 1,
                        'borsa': 'FOREX',
                        'tip': 'FOREX',
                        'ulke': 'Global',
                        'aciklama': f"Forex - {base}/{quote}"
                    }
                    parite_list.append(parite_info)
                    
                except Exception as e:
                    print(f"Parite işleme hatası ({currency}): {str(e)}")
                    continue
            
            # Veritabanına kaydet
            if parite_list:
                eklenen, guncellenen, silinen = self.sync_pariteler_to_db(parite_list)
                print(f"Forex: {len(parite_list)} parite bulundu -> {eklenen} yeni eklendi")
            
        except Exception as e:
            print(f"Forex verisi alınamadı: {str(e)}")

if __name__ == "__main__":
    collector = ForexCollector()
    collector.collect_pariteler() 