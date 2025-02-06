"""
Veri toplama servisinin ana giriş noktası
"""

import signal
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
should_exit = False

def signal_handler(sig, frame):
    """Ctrl+C ile programı sonlandır"""
    global should_exit
    should_exit = True
    print("Program durduruluyor, lütfen bekleyin...")

def main():
    """Ana fonksiyon"""
    # Sinyal işleyicisini ayarla
    signal.signal(signal.SIGINT, signal_handler)
    
    # Argüman ayrıştırıcıyı ayarla
    parser = argparse.ArgumentParser(description='Veri toplama servisi')
    parser.add_argument('--source', type=str, choices=['all', 'binance_futures', 'binance_spot', 'forex', 'index', 'commodity', 'stock'],
                      default='all', help='Veri kaynağı')
    parser.add_argument('--type', type=str, choices=['all', 'parite', 'candle'],
                      default='all', help='Veri tipi')
    args = parser.parse_args()
    
    try:
        global collectors
        
        # Parite toplayıcıları
        if args.type in ['all', 'parite']:
            if args.source in ['all', 'binance_futures']:
                collectors.append(PariteBinanceFuturesCollector())
            if args.source in ['all', 'binance_spot']:
                collectors.append(PariteBinanceSpotCollector())
            if args.source in ['all', 'forex']:
                collectors.append(PariteForexCollector())
            if args.source in ['all', 'stock']:
                collectors.append(PariteStockCollector())
            if args.source in ['all', 'index']:
                collectors.append(PariteIndexCollector())
            if args.source in ['all', 'commodity']:
                collectors.append(PariteCommodityCollector())
                
        # Mum toplayıcıları
        if args.type in ['all', 'candle']:
            if args.source in ['all', 'binance_futures']:
                collectors.append(CandleBinanceFuturesCollector())
            if args.source in ['all', 'binance_spot']:
                collectors.append(CandleBinanceSpotCollector())
            if args.source in ['all', 'forex']:
                collectors.append(CandleForexCollector())
            if args.source in ['all', 'stock']:
                collectors.append(CandleStockCollector())
            if args.source in ['all', 'index']:
                collectors.append(CandleIndexCollector())
            if args.source in ['all', 'commodity']:
                collectors.append(CandleCommodityCollector())
                
        # Sonsuz döngü
        while not should_exit:
            for collector in collectors:
                if should_exit:
                    break
                    
                try:
                    # Veri topla
                    collector.run()
                except Exception as e:
                    print(f"Toplayıcı hatası: {str(e)}")
                    continue
                    
            # Güncelleme aralığı kadar bekle
            if not should_exit:
                time.sleep(UPDATE_INTERVAL)
                
    except KeyboardInterrupt:
        print("\nProgram durduruluyor...")
        sys.exit(0)
    except Exception as e:
        print(f"Beklenmeyen hata: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
    
