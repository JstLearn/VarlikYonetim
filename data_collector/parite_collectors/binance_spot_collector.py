"""
Binance Spot paritelerini toplayan sınıf
"""

from binance.client import Client
from utils.database import Database

class BinanceSpotCollector:
    def __init__(self):
        self.client = Client(None, None)

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
        """Binance Spot paritelerini getirir ve veritabanına ekler"""
        try:
            # Tüm parite bilgilerini al
            exchange_info = self.client.get_exchange_info()
            if not exchange_info or 'symbols' not in exchange_info:
                print("Parite bilgisi alınamadı")
                return
                
            parite_list = []
            for symbol in exchange_info['symbols']:
                try:
                    # Sadece SPOT ve aktif olan pariteleri al
                    if symbol['status'] != 'TRADING':
                        continue
                        
                    # Parite bilgilerini oluştur
                    parite_info = {
                        'parite': symbol['symbol'],
                        'aktif': 1,
                        'borsa': 'BINANCE',
                        'tip': 'SPOT',
                        'ulke': 'Global',
                        'aciklama': f"Binance Spot - {symbol['baseAsset']}/{symbol['quoteAsset']}"
                    }
                    parite_list.append(parite_info)
                    
                except Exception as e:
                    print(f"Parite işleme hatası ({symbol.get('symbol', 'Bilinmeyen')}): {str(e)}")
                    continue
            
            # Veritabanına kaydet
            if parite_list:
                eklenen, guncellenen, silinen = self.sync_pariteler_to_db(parite_list)
                print(f"Binance Spot: {len(parite_list)} parite bulundu -> {eklenen} yeni eklendi")
            
        except Exception as e:
            print(f"Binance Spot verisi alınamadı: {str(e)}")

if __name__ == "__main__":
    collector = BinanceSpotCollector()
    collector.collect_pariteler() 