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

# .env dosyasını yükle
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

def check_sql_driver():
    """SQL Server sürücülerini kontrol eder"""
    try:
        drivers = [x for x in pyodbc.drivers() if x.startswith('SQL Server')]
        if not drivers:
            print("SQL Server sürücüsü bulunamadı!")
            print("Kullanılabilir sürücüler:", pyodbc.drivers())
            return False
        print(f"Kullanılabilir SQL Server sürücüleri: {drivers}")
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
        
        for symbol, market in markets.items():
            if market['active']:  # Sadece aktif olanları al
                base = market['base']
                quote = market['quote']
                parite = f"{base}/{quote}"
                tip = 'SPOT' if market['spot'] else 'FUTURES'
                
                # Parite+tip kombinasyonunu kontrol et
                parite_key = (parite, tip)
                if parite_key not in eklenen_pariteler:
                    parite_info = {
                        'parite': parite,
                        'aktif': 1,
                        'borsa': 'BINANCE',
                        'tip': tip,
                        'ulke': 'Global',
                        'aciklama': f"{base}/{quote} {market['type'].upper()} Pair"
                    }
                    all_pariteler.append(parite_info)
                    eklenen_pariteler.add(parite_key)
        
        return all_pariteler
        
    except Exception as e:
        print(f"Binance veri alma hatası: {str(e)}")
        return []

def sync_pariteler_to_db(yeni_pariteler):
    """
    Pariteleri veritabanı ile senkronize eder
    """
    db = None
    conn = None
    try:
        db = Database()
        conn = db.connect()
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        # Mevcut pariteleri al
        cursor.execute("""
            SELECT parite, borsa, tip, aktif
            FROM pariteler 
            WHERE borsa = 'BINANCE'
        """)
        mevcut_pariteler = {(row[0], row[1], row[2], row[3]): True for row in cursor.fetchall()}
        
        # Yeni pariteleri ekle
        eklenen_sayisi = 0
        for parite in yeni_pariteler:
            try:
                # Aynı anahtar kombinasyonunu kullan
                parite_key = (parite['parite'], parite['borsa'], parite['tip'], parite['aktif'])
                
                # Eğer bu kombinasyon yoksa ekle
                if parite_key not in mevcut_pariteler:
                    cursor.execute("""
                        IF NOT EXISTS (
                            SELECT 1 FROM pariteler 
                            WHERE parite = ? AND borsa = ? AND tip = ? AND aktif = ?
                        )
                        BEGIN
                            INSERT INTO pariteler (parite, aktif, borsa, tip, ulke, aciklama)
                            VALUES (?, ?, ?, ?, ?, ?)
                        END
                    """, 
                    parite['parite'], parite['borsa'], parite['tip'], parite['aktif'],  # Kontrol için
                    parite['parite'], parite['aktif'], parite['borsa'], 
                    parite['tip'], parite['ulke'], parite['aciklama'])  # Insert için
                    eklenen_sayisi += 1
                
            except Exception as e:
                print(f"Parite işleme hatası ({parite['parite']} {parite['tip']}): {str(e)}")
                continue
        
        # Artık kullanılmayan pariteleri sil
        silinen_sayisi = 0
        for mevcut_key in mevcut_pariteler:
            if mevcut_key not in {(p['parite'], p['borsa'], p['tip'], p['aktif']) for p in yeni_pariteler}:
                cursor.execute("""
                    DELETE FROM pariteler 
                    WHERE parite = ? AND borsa = ? AND tip = ? AND aktif = ?
                """, mevcut_key[0], mevcut_key[1], mevcut_key[2], mevcut_key[3])
                silinen_sayisi += 1
        
        conn.commit()
        
        # Sadece değişiklik varsa bilgi ver
        if eklenen_sayisi > 0 or silinen_sayisi > 0:
            print(f"Toplam {len(yeni_pariteler)} parite içinden {eklenen_sayisi} eklendi, {silinen_sayisi} silindi")
            
        return True
        
    except Exception as e:
        print(f"Veritabanı işlem hatası: {str(e)}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return False
    finally:
        if db:
            try:
                db.close()
            except:
                pass

def run_continuous():
    """Sürekli çalışan ana döngü"""
    if not check_sql_driver() or not check_db_config():
        return
    
    print("Parite izleme başladı...")
    
    while True:
        try:
            pariteler = get_binance_pariteler()
            if pariteler:
                sync_pariteler_to_db(pariteler)
            
        except KeyboardInterrupt:
            print("\nProgram kullanıcı tarafından durduruldu")
            break
        except Exception as e:
            print(f"İşlem hatası: {str(e)}")
            time.sleep(1)

if __name__ == "__main__":
    run_continuous() 