"""
Veri toplama uygulaması ana modülü
"""

import logging
import time
from datetime import datetime, timedelta
import schedule
from database import Database
from collectors import collect_all_data
from config import UPDATE_INTERVAL

# Loglama ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def update_data():
    """Verileri günceller"""
    try:
        db = Database()
        collect_all_data(db)
    except Exception as e:
        logging.error(f"Veri güncelleme hatası: {str(e)}")

def main():
    """Ana program döngüsü"""
    try:
        logging.info("Veri toplama uygulaması başlatıldı")
        logging.info(f"Güncelleme aralığı: {UPDATE_INTERVAL} saniye")
        
        # İlk çalıştırmada verileri topla
        update_data()
        
        # Belirli aralıklarla güncelleme yap
        schedule.every(UPDATE_INTERVAL).seconds.do(update_data)
        
        while True:
            schedule.run_pending()
            time.sleep(1)
            
    except KeyboardInterrupt:
        logging.info("Program kapatılıyor...")
    except Exception as e:
        logging.error(f"Program hatası: {str(e)}")

if __name__ == "__main__":
    main() 