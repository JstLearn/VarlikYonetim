"""
Veri toplama servisinin ana giriş noktası
"""

import sys
import time
import argparse
from utils.config import UPDATE_INTERVAL

# Parite toplayıcıları
from parite_collectors.binance_spot_collector import BinanceSpotCollector as PariteBinanceSpotCollector
from parite_collectors.binance_futures_collector import BinanceFuturesCollector as PariteBinanceFuturesCollector
from parite_collectors.forex_collector import ForexCollector as PariteForexCollector
from parite_collectors.index_collector import IndexCollector as PariteIndexCollector
from parite_collectors.stock_collector import StockCollector as PariteStockCollector
from parite_collectors.commodity_collector import CommodityCollector as PariteCommodityCollector

# Mum toplayıcıları
from candle_collectors.binance_spot_collector import BinanceSpotCollector as CandleBinanceSpotCollector
from candle_collectors.binance_futures_collector import BinanceFuturesCollector as CandleBinanceFuturesCollector
from candle_collectors.forex_collector import ForexCollector as CandleForexCollector
from candle_collectors.index_collector import IndexCollector as CandleIndexCollector
from candle_collectors.stock_collector import StockCollector as CandleStockCollector
from candle_collectors.commodity_collector import CommodityCollector as CandleCommodityCollector

# Global değişkenler
collectors = []

def collect_data(args):
    """Veri toplama işlemini gerçekleştiren fonksiyon"""
    global collectors
    collectors = []  # Her çalıştırmada collectors listesini temizle
    
    # Parite toplayıcıları
    if args.type in ['all', 'parite']:
        if args.source in ['all', 'commodity']:
            collectors.append(PariteCommodityCollector())
        if args.source in ['all', 'binance_futures']:
            collectors.append(PariteBinanceFuturesCollector())
        if args.source in ['all', 'binance_spot']:
            collectors.append(PariteBinanceSpotCollector())
        if args.source in ['all', 'index']:
            collectors.append(PariteIndexCollector())                
        if args.source in ['all', 'forex']:
            collectors.append(PariteForexCollector())
        if args.source in ['all', 'stock']:
            collectors.append(PariteStockCollector())

            
    # Mum toplayıcıları
    if args.type in ['all', 'candle']:
        if args.source in ['all', 'commodity']:
            collectors.append(CandleCommodityCollector())
        if args.source in ['all', 'binance_futures']:
            collectors.append(CandleBinanceFuturesCollector())
        if args.source in ['all', 'binance_spot']:
            collectors.append(CandleBinanceSpotCollector())
        if args.source in ['all', 'index']:
            collectors.append(CandleIndexCollector())
        if args.source in ['all', 'forex']:
            collectors.append(CandleForexCollector())
        if args.source in ['all', 'stock']:
            collectors.append(CandleStockCollector())
            
    # Her collector için çalıştır
    for collector in collectors:
        try:
            if hasattr(collector, 'collect_pariteler'):
                collector.collect_pariteler()
            elif hasattr(collector, 'run'):
                collector.run()
        except KeyboardInterrupt:
            print("\nKullanıcı tarafından durduruldu.")
            raise
        except Exception as e:
            print(f"Toplayıcı hatası: {str(e)}")
            continue

def main():
    """Ana fonksiyon"""
    # Argüman ayrıştırıcıyı ayarla
    parser = argparse.ArgumentParser(description='Veri toplama servisi')
    parser.add_argument('--source', type=str, choices=['all', 'binance_futures', 'binance_spot', 'forex', 'index', 'commodity', 'stock'],
                      default='all', help='Veri kaynağı')
    parser.add_argument('--type', type=str, choices=['all', 'parite', 'candle'],
                      default='all', help='Veri tipi')
    args = parser.parse_args()
    
    # Sonsuz döngüde veri toplama işlemine devam et
    while True:
        try:
            print(f"\n[{time.strftime('%H:%M:%S')}] Veri toplama işlemi başlatılıyor...")
            collect_data(args)
            
            # UPDATE_INTERVAL değeri config dosyasından alınıyor (varsayılan 1 saat = 3600 saniye)
            wait_time = UPDATE_INTERVAL
            print(f"\n[{time.strftime('%H:%M:%S')}] Veri toplama tamamlandı. {wait_time} saniye beklenecek...")
            
            # Beklerken kullanıcının Ctrl+C ile sonlandırabilmesi için her saniye kontrol ediyoruz
            for i in range(wait_time):
                time.sleep(1)
                # Her dakikada bir kalan süreyi göster
                if i > 0 and i % 60 == 0:
                    remaining = wait_time - i
                    remaining_min = remaining // 60
                    remaining_sec = remaining % 60
                    print(f"[{time.strftime('%H:%M:%S')}] Kalan süre: {remaining_min} dakika {remaining_sec} saniye")
                
        except KeyboardInterrupt:
            print(f"\n[{time.strftime('%H:%M:%S')}] Kullanıcı tarafından durduruldu.")
            sys.exit(0)
        except Exception as e:
            print(f"\n[{time.strftime('%H:%M:%S')}] Beklenmeyen hata: {str(e)}")
            print("30 saniye sonra yeniden denenecek...")
            time.sleep(30)  # Hata durumunda 30 saniye bekleyip tekrar dene

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nKullanıcı tarafından durduruldu.")
        sys.exit(0)
    
