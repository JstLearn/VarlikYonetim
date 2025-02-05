"""
Veri toplama servisi ana modülü
"""

import signal
import sys
import argparse
from binance_futures_collector import BinanceFuturesCollector
from binance_spot_collector import BinanceSpotCollector
from forex_collector import ForexCollector
from index_collector import IndexCollector
from commodity_collector import CommodityCollector
from stock_collector import StockCollector

def signal_handler(sig, frame):
    """Ctrl+C ile programı sonlandır"""
    print('\nProgram sonlandırılıyor...')
    sys.exit(0)

def main():
    """Ana fonksiyon"""
    # Sinyal işleyicisini ayarla
    signal.signal(signal.SIGINT, signal_handler)
    
    # Argüman ayrıştırıcıyı ayarla
    parser = argparse.ArgumentParser(description='Veri toplama servisi')
    parser.add_argument('--source', type=str, choices=['all', 'binance_futures', 'binance_spot', 'forex', 'index', 'commodity', 'stock'],
                      default='all', help='Veri kaynağı')
    args = parser.parse_args()
    
    try:
        if args.source in ['all', 'binance_futures']:
            collector = BinanceFuturesCollector()
            collector.run()
            
        if args.source in ['all', 'binance_spot']:
            collector = BinanceSpotCollector()
            collector.run()
            
        if args.source in ['all', 'forex']:
            collector = ForexCollector()
            collector.run()

        if args.source in ['all', 'stock']:
            collector = StockCollector()
            collector.run()      

        if args.source in ['all', 'index']:
            collector = IndexCollector()
            collector.run()
            
        if args.source in ['all', 'commodity']:
            collector = CommodityCollector()
            collector.run()   
            
    except Exception as e:
        print(f"Hata: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 