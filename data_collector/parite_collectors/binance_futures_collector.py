"""
Binance Futures paritelerini toplayan sınıf
"""

from binance.client import Client
from utils.database import Database

class BinanceFuturesCollector:
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
        """Binance Futures paritelerini getirir ve veritabanına ekler"""
        try:
            # Tüm parite bilgilerini al
            futures_info = self.client.futures_exchange_info()
            if not futures_info or 'symbols' not in futures_info:
                print("Parite bilgisi alınamadı")
                return
                
            parite_list = []
            for symbol in futures_info['symbols']:
                try:
                    # Sadece aktif olan pariteleri al
                    if symbol['status'] != 'TRADING':
                        continue
                        
                    # Parite bilgilerini oluştur
                    parite_info = {
                        'parite': symbol['symbol'],
                        'aktif': 1,
                        'borsa': 'BINANCE',
                        'tip': 'FUTURES',
                        'ulke': 'Global',
                        'aciklama': f"Binance Futures - {symbol['baseAsset']}/{symbol['quoteAsset']}"
                    }
                    parite_list.append(parite_info)
                    
                except Exception as e:
                    print(f"Parite işleme hatası ({symbol.get('symbol', 'Bilinmeyen')}): {str(e)}")
                    continue
            
            # Veritabanına kaydet
            if parite_list:
                eklenen, guncellenen, silinen = self.sync_pariteler_to_db(parite_list)
                print(f"BINANCE FUTURES: {len(parite_list)} parite bulundu -> {eklenen} yeni eklendi")
            
        except Exception as e:
            print(f"Binance Futures verisi alınamadı: {str(e)}")

if __name__ == "__main__":
    collector = BinanceFuturesCollector()
    collector.collect_pariteler() 