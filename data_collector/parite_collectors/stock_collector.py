"""
Hisse senetlerini toplayan sınıf
"""

import investpy
import yfinance as yf
import requests
import pandas as pd
import signal
import time
from utils.database import Database

class StockCollector():
    def __init__(self):
        self.should_exit = False
        signal.signal(signal.SIGINT, self._signal_handler)
        self.major_exchanges = {
            'NYSE': {'country': 'USA', 'currency': 'USD'},
            'NASDAQ': {'country': 'USA', 'currency': 'USD'},
            'LSE': {'country': 'UK', 'currency': 'GBP'},
            'TSE': {'country': 'Japan', 'currency': 'JPY'},
            'SSE': {'country': 'China', 'currency': 'CNY'},
            'HKEX': {'country': 'Hong Kong', 'currency': 'HKD'},
            'BIST': {'country': 'Turkey', 'currency': 'TRY'},
            'BSE': {'country': 'India', 'currency': 'INR'},
            'XETRA': {'country': 'Germany', 'currency': 'EUR'},
            'EURONEXT': {'country': 'France', 'currency': 'EUR'}
        }
        
    def _signal_handler(self, signum, frame):
        """Sinyal yakalayıcı"""
        self.should_exit = True
        print("Program durduruluyor, lütfen bekleyin...")

    def get_country_currency(self, country_name):
        """
        Ülkenin para birimini API'den alır
        """
        try:
            # REST Countries API'den ülke bilgilerini al
            url = f"https://restcountries.com/v3.1/name/{country_name}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    # İlk eşleşen ülkenin para birimini al
                    currencies = data[0].get('currencies', {})
                    if currencies:
                        # İlk para biriminin kodunu al
                        currency_code = list(currencies.keys())[0]
                        return currency_code
            
            # API'den alınamazsa varsayılan değerleri kullan
            defaults = {
                'turkey': 'TRY',
                'united states': 'USD',
                'japan': 'JPY',
                'china': 'CNY',
                'united kingdom': 'GBP',
                'european union': 'EUR',
                'brazil': 'BRL',
                'australia': 'AUD',
                'canada': 'CAD',
                'switzerland': 'CHF'
            }
            return defaults.get(country_name.lower(), 'USD')
            
        except Exception as e:
            if not self.should_exit:
                print(f"Para birimi alınamadı ({country_name}): {str(e)}")
            return 'USD'  # Hata durumunda USD kullan

    def collect_pariteler(self):
        """
        Investpy üzerinden hisse senetlerini getirir ve veritabanına ekler
        """
        try:
            countries = investpy.get_stock_countries()
            
            for country in countries:
                try:
                    stocks = investpy.get_stocks(country=country)
                    if len(stocks) == 0:
                        continue
                    
                    currency = self.get_country_currency(country)
                    eklenen_sayisi = 0
                    
                    # Ülke adını standartlaştır
                    country_name = country.upper()
                    
                    for _, stock in stocks.iterrows():
                        try:
                            yf_symbol = stock['symbol'].strip().upper()
                            if not yf_symbol:
                                continue
                                
                            # Önce veritabanında bu sembolü kontrol et
                            db = Database()
                            conn = db.connect()
                            if conn:
                                cursor = conn.cursor()
                                cursor.execute("""
                                    SELECT borsa FROM pariteler 
                                    WHERE parite LIKE ? AND tip = 'STOCK'
                                """, (f"{yf_symbol}/%",))
                                
                                existing_exchange = cursor.fetchone()
                                db.disconnect()
                                
                                if existing_exchange:
                                    exchange = existing_exchange[0]
                                else:
                                    # Önce Yahoo Finance'den borsa adını almaya çalış
                                    try:
                                        stock_ticker = yf.Ticker(yf_symbol)
                                        info = stock_ticker.info
                                        if info and 'exchange' in info:
                                            exchange = info['exchange'].upper()
                                        else:
                                            exchange = f"{country_name}_STOCK"
                                    except:
                                        exchange = f"{country_name}_STOCK"
                            else:
                                exchange = f"{country_name}_STOCK"
                                
                            stock_info = [{
                                'parite': f"{yf_symbol}/{currency}",
                                'aktif': 1,
                                'borsa': exchange,
                                'tip': 'STOCK',
                                'ulke': country_name,
                                'aciklama': f"{stock['name']} - {country_name} Stock"
                            }]
                            
                            # Her hisseyi tek tek kaydet
                            eklenen, guncellenen, silinen = self.sync_pariteler_to_db(stock_info)
                            eklenen_sayisi += eklenen
                            
                        except Exception as e:
                            continue
                    
                    print(f"{country} Stocks: {len(stocks)} parite bulundu -> {eklenen_sayisi} yeni eklendi")
                    
                except Exception as e:
                    print(f"{country} hatası: {str(e)}")
                    continue
            
        except Exception as e:
            if not self.should_exit:
                print(f"Hisse senedi verisi alınamadı: {str(e)}")

    def collect(self):
        """
        Yahoo Finance üzerinden hisse senetlerini getirir ve direkt veritabanına ekler
        """
        try:
            stock_list = []
            for exchange, info in self.major_exchanges.items():
                if self.should_exit:
                    break
                    
                try:
                    # Borsadaki hisseleri al
                    stocks = self._get_exchange_stocks(exchange)
                    if not stocks:
                        continue
                        
                    for symbol in stocks:
                        if self.should_exit:
                            break
                            
                        try:
                            # Yahoo Finance'dan hisse bilgilerini al
                            stock = yf.Ticker(symbol)
                            stock_info = stock.info
                            
                            # Temel bilgileri kontrol et
                            if not stock_info or 'symbol' not in stock_info:
                                continue
                                
                            # Para birimini al
                            currency = stock_info.get('currency', info['currency'])
                            if not currency:
                                currency = info['currency']
                                
                            # Şirket adını al
                            company_name = stock_info.get('longName', stock_info.get('shortName', symbol))
                            
                            # Sektör bilgisini al
                            sector = stock_info.get('sector', 'Unknown Sector')
                            industry = stock_info.get('industry', 'Unknown Industry')
                            
                            # Açıklama oluştur
                            description = f"{company_name} - {sector}"
                            if industry and industry != 'Unknown Industry':
                                description += f" ({industry})"
                                
                            stock_info = {
                                'parite': f"{symbol}/{currency}",
                                'aktif': 1,
                                'borsa': exchange,
                                'tip': 'STOCK',
                                'ulke': info['country'],
                                'aciklama': description
                            }
                            stock_list.append(stock_info)
                            
                        except Exception as e:
                            if not self.should_exit:
                                print(f"Hisse işleme hatası ({symbol}): {str(e)}")
                            continue
                            
                    # Her borsa için istatistik
                    if not self.should_exit:
                        print(f"{exchange}: {len(stocks)} hisse bulundu")
                        
                except Exception as e:
                    if not self.should_exit:
                        print(f"Borsa işleme hatası ({exchange}): {str(e)}")
                    continue
                    
            # Veritabanına kaydet
            if stock_list:
                eklenen, guncellenen, silinen = self.sync_pariteler_to_db(stock_list)
                if not self.should_exit:
                    print(f"Hisse: {len(stock_list)} hisse bulundu -> {eklenen} yeni, {guncellenen} güncellenen, {silinen} silinen")
                    
            return stock_list
            
        except Exception as e:
            if not self.should_exit:
                print(f"Hisse verisi alınamadı: {str(e)}")
            return []
            
    def _get_exchange_stocks(self, exchange):
        """
        Belirli bir borsadaki hisse senetlerini getirir
        """
        try:
            # Yahoo Finance'dan borsa verilerini al
            if exchange == 'BIST':
                # BIST için özel işlem
                stocks = pd.read_html('https://en.wikipedia.org/wiki/BIST_100')[4]
                return [f"{code.strip()}.IS" for code in stocks['Code'].tolist() if isinstance(code, str)]
            else:
                # Diğer borsalar için Yahoo Finance API kullan
                tickers = yf.Tickers(f"^{exchange}")
                if hasattr(tickers, 'tickers'):
                    return [t.info['symbol'] for t in tickers.tickers if hasattr(t, 'info')]
                    
            return []
            
        except Exception as e:
            if not self.should_exit:
                print(f"Borsa hisseleri alınamadı ({exchange}): {str(e)}")
            return [] 

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
                if self.should_exit:
                    break
                    
                try:
                    cursor.execute("""
                        SELECT 1 FROM pariteler 
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

    def run_continuous(self, interval=3600):
        """Sürekli çalışan ana döngü"""
        print("Parite izleme başladı...")
        
        while not self.should_exit:
            try:
                self.collect_pariteler()
                
                if self.should_exit:
                    print("Program durduruluyor...")
                    break
                    
                print(f"Tüm işlemler tamamlandı. {interval//60} dakika bekleniyor...")
                for i in range(interval):
                    if self.should_exit:
                        print("Program durduruluyor...")
                        break
                    time.sleep(1)
                    
            except Exception as e:
                if not self.should_exit:
                    print(f"İşlem hatası: {str(e)}")
                    time.sleep(5)
        
        print("Program sonlandırıldı")

if __name__ == "__main__":
    collector = StockCollector()
    collector.run_continuous() 