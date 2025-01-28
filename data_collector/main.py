"""
Veri toplama uygulamasının ana dosyası
"""

import time
import signal
import schedule # type: ignore
from datetime import datetime, timezone
from database import create_connection, insert_currency_data, check_currency_exists
from collectors import collect_all_data
from config import UPDATE_INTERVAL

# Global değişken olarak çalışma durumunu tut
running = True

def signal_handler(signum, frame):
    """Ctrl+C sinyalini yakala"""
    global running
    print(f"\n[{datetime.now()}] Uygulama kapatılıyor...")
    running = False

def process_data(data, conn):
    """Toplanan verileri veritabanına kaydeder"""
    try:
        # UTC+0 tarihini al
        utc_now = datetime.now(timezone.utc)
        
        for currency_data in data:
            # Aynı parite ve tarih için veri var mı kontrol et
            if not check_currency_exists(conn, currency_data[0], currency_data[1], utc_now):
                # Veriyi eklerken UTC+0 tarihini kullan
                data_with_utc = currency_data[:-1] + (utc_now,)
                insert_currency_data(conn, data_with_utc)
        
        print(f"[{datetime.now()}] Veriler başarıyla kaydedildi")
    except Exception as e:
        print(f"[{datetime.now()}] Veri işleme hatası: {str(e)}")

def update_data():
    """Verileri günceller"""
    try:
        # Veritabanı bağlantısı
        conn = create_connection()
        
        # Veri toplama
        print(f"[{datetime.now()}] Veri toplama işlemi başlatılıyor...")
        data = collect_all_data()
        
        # Verileri işleme
        if data:
            process_data(data, conn)
        else:
            print(f"[{datetime.now()}] Toplanacak veri bulunamadı")
        
        # Bağlantıyı kapat
        conn.close()
        
    except Exception as e:
        print(f"[{datetime.now()}] Güncelleme hatası: {str(e)}")

def main():
    """Ana uygulama döngüsü"""
    try:
        # Ctrl+C sinyalini yakala
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        print(f"[{datetime.now()}] Veri toplama uygulaması başlatıldı")
        print(f"Güncelleme aralığı: {UPDATE_INTERVAL} saniye")
        print("Uygulamayı durdurmak için Ctrl+C tuşlarına basın")
        
        # İlk veri toplama işlemi
        update_data()
        
        # Zamanlanmış görevleri ayarla
        schedule.every(UPDATE_INTERVAL).seconds.do(update_data)
        
        # Ana döngü
        while running:
            schedule.run_pending()
            time.sleep(1)
            
        print(f"[{datetime.now()}] Uygulama başarıyla kapatıldı")
            
    except Exception as e:
        print(f"[{datetime.now()}] Uygulama hatası: {str(e)}")
        
if __name__ == "__main__":
    main() 