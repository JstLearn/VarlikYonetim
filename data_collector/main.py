"""
Veri toplama servisinin ana giriş noktası
"""

import sys
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

def main():
    """Ana fonksiyon"""
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
                
        # Her collector için çalıştır
        for collector in collectors:
            try:
                collector.collect_pariteler()
            except KeyboardInterrupt:
                print("\nKullanıcı tarafından durduruldu.")
                sys.exit(0)
            except Exception as e:
                print(f"Toplayıcı hatası: {str(e)}")
                continue
                    
    except KeyboardInterrupt:
        print("\nKullanıcı tarafından durduruldu.")
        sys.exit(0)
    except Exception as e:
        print(f"Beklenmeyen hata: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nKullanıcı tarafından durduruldu.")
        sys.exit(0)
    
