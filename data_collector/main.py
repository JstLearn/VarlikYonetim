"""
Veri toplama uygulaması ana modülü
"""

import time
from datetime import datetime, timedelta
import schedule
from database import Database
from collectors import collect_all_data
from config import UPDATE_INTERVAL

def update_data():
    """Verileri günceller"""
    try:
        db = Database()
        collect_all_data(db)
    except Exception as e:
        print(f"Veri güncelleme hatası: {str(e)}")

def main():
    """Ana program döngüsü"""
    try:
        print("Veri toplama uygulaması başlatıldı")
        print(f"Güncelleme aralığı: {UPDATE_INTERVAL} saniye")
                
        # İlk çalıştırmada verileri topla
        update_data()
                    
        # Belirli aralıklarla güncelleme yap
        schedule.every(UPDATE_INTERVAL).seconds.do(update_data)
                    
        while True:
            schedule.run_pending()
            time.sleep(1)
                
    except KeyboardInterrupt:
        print("Program kapatılıyor...")
    except Exception as e:
        print(f"Program hatası: {str(e)}")

if __name__ == "__main__":
    main() 