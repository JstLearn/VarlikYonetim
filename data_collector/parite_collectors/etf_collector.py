"""
TradingView üzerinden ETF'leri toplayan sınıf
"""

import requests
from bs4 import BeautifulSoup
import sys
import os
import dotenv
from pathlib import Path

# Çevre değişkenlerini yükle
env_path = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
dotenv.load_dotenv(env_path / '.env')

# Modül yolunu düzelt
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(parent_dir)

from data_collector.utils.database import Database
import logging
import time
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

# Logger ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('etf_collector')

# Diğer gereksiz loggerları kapat
for name in logging.root.manager.loggerDict:
    if name != 'etf_collector':
        logging.getLogger(name).setLevel(logging.CRITICAL)
        logging.getLogger(name).propagate = False

class ETFCollector:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Referer': 'https://www.tradingview.com/'
        }
        self.tradingview_url = "https://www.tradingview.com/etf-screener/"
        self.tradingview_login_url = "https://www.tradingview.com/accounts/signin/"
        self.tradingview_username = os.environ.get('TRADINGVIEW_USERNAME')
        self.tradingview_password = os.environ.get('TRADINGVIEW_PASSWORD')
        self.session = requests.Session()
        
        # ETF takibi için eklenen değişkenler
        self.all_etfs = []  # TradingView'dan çekilen tüm ETF'ler
        self.added_etfs = []  # Veritabanına başarıyla eklenen ETF'ler
        self.skipped_etfs = []  # Çeşitli nedenlerle atlanmış ETF'ler
        self.skipped_reasons = {}  # Atlama nedenleri
        
        # Veritabanı bağlantısı
        try:
            self.db = Database()
            self.db.connect()  # Bağlantıyı başlat
            logger.info("Veritabanı bağlantısı başarıyla kuruldu")
        except Exception as e:
            logger.error(f"Veritabanı bağlantı hatası: {str(e)}")
            raise

    def take_screenshot(self, driver, name="screenshot"):
        """Ekran görüntüsü alır ve kayıt yolunu döndürür"""
        try:
            screenshot_path = f"screenshots/{name}_{int(time.time())}.png"
            # Klasör yoksa oluştur
            os.makedirs(os.path.dirname(screenshot_path), exist_ok=True)
            driver.save_screenshot(screenshot_path)
            logger.info(f"Ekran görüntüsü kaydedildi: {screenshot_path}")
            return screenshot_path
        except Exception as e:
            logger.error(f"Ekran görüntüsü alma hatası: {str(e)}")
            return None

    def analyze_screenshot_for_market_button(self, driver, screenshot_path):
        """Ekran görüntüsünü analiz ederek Market butonunun koordinatlarını bulur"""
        try:
            # Ekran görüntüsünü al ve analiz et
            logger.info(f"Ekran görüntüsü analiz ediliyor: {screenshot_path}")
            
            # Sayfadaki tüm tıklanabilir öğeleri bul
            clickable_elements = driver.find_elements(By.XPATH, "//*[self::button or self::a or self::div[@role='button']]")
            
            market_button_info = None
            for element in clickable_elements:
                try:
                    if not element.is_displayed():
                        continue
                        
                    # Element bilgilerini al
                    text = element.text.strip().lower()
                    location = element.location
                    size = element.size
                    
                    # Market butonu olabilecek anahtar kelimeler
                    keywords = ["market", "piyasa", "usa", "united states", "country", "ülke"]
                    
                    # Metin kontrolü
                    if any(keyword in text for keyword in keywords):
                        # Koordinatları hesapla
                        x = location['x'] + size['width'] // 2
                        y = location['y'] + size['height'] // 2
                        
                        market_button_info = {
                            'element': element,
                            'text': text,
                            'x': x,
                            'y': y,
                            'width': size['width'],
                            'height': size['height']
                        }
                        logger.info(f"Market butonu bulundu: '{text}' - Koordinatlar: ({x}, {y})")
                        break
                        
                except Exception as e:
                    continue
            
            return market_button_info
            
        except Exception as e:
            logger.error(f"Ekran görüntüsü analiz hatası: {str(e)}")
            return None

    def click_with_coordinates(self, driver, x, y, element=None, description=""):
        """Belirtilen koordinatlara tıklar"""
        try:
            # Önce elementi görünür yap
            if element:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                time.sleep(1)
            
            # Koordinat bazlı tıklama dene
            driver.execute_script(f"""
                var element = document.elementFromPoint({x}, {y});
                if(element) {{
                    var clickEvent = new MouseEvent('click', {{
                        'view': window,
                        'bubbles': true,
                        'cancelable': true,
                        'clientX': {x},
                        'clientY': {y}
                    }});
                    element.dispatchEvent(clickEvent);
                }}
            """)
            logger.info(f"{description} koordinat bazlı tıklama yapıldı: ({x}, {y})")
            return True
            
        except Exception as e:
            logger.error(f"Koordinat bazlı tıklama hatası: {str(e)}")
            return False

    def setup_webdriver(self):
        """Selenium WebDriver'ı headless modda ayarlar"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
            chrome_options.add_argument("--window-size=1920,1080")
            
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)
            
            return driver
        except Exception as e:
            logger.error(f"WebDriver başlatma hatası: {str(e)}")
            return None

    def fetch_tradingview_etfs(self):
        """TradingView'dan ETF'leri çeker"""
        logger.info("TradingView'dan ETF'ler çekiliyor...")
        
        driver = None
        try:
            driver = self.setup_webdriver()
            if not driver:
                logger.error("WebDriver başlatılamadı!")
                return []
            
            driver.get(self.tradingview_url)
            logger.info("TradingView ETF Screener sayfası açıldı")
            
            time.sleep(5)  # Sayfa yüklensin
            
            # Market butonunu bulmak için ekran görüntüsü al
            screenshot_path = self.take_screenshot(driver, "market_button_search")
            if screenshot_path:
                # Ekran görüntüsünü analiz et
                market_button_info = self.analyze_screenshot_for_market_button(driver, screenshot_path)
                
                if market_button_info:
                    # Koordinat bazlı tıklama yap
                    click_success = self.click_with_coordinates(
                        driver,
                        market_button_info['x'],
                        market_button_info['y'],
                        market_button_info['element'],
                        "Market butonu"
                    )
                    
                    if click_success:
                        time.sleep(2)  # Menünün açılmasını bekle
                        
                        # Menü açıldıktan sonra ekran görüntüsü al
                        menu_screenshot = self.take_screenshot(driver, "market_menu")
                        
                        # Entire World seçeneğini bul
                        world_menu_info = self.analyze_screenshot_for_market_button(driver, menu_screenshot)
                        
                        if world_menu_info:
                            # Entire World seçeneğine tıkla
                            world_click_success = self.click_with_coordinates(
                                driver,
                                world_menu_info['x'],
                                world_menu_info['y'],
                                world_menu_info['element'],
                                "Entire World seçeneği"
                            )
                            
                            if world_click_success:
                                time.sleep(2)
                                logger.info("Entire World seçeneği başarıyla seçildi")
                        else:
                            logger.warning("Entire World seçeneği bulunamadı")
                    else:
                        logger.error("Market butonuna tıklama başarısız")
                else:
                    logger.error("Market butonu koordinatları bulunamadı")
            
            # ETF verilerini topla
            etfs = []
            # ... ETF verilerini toplama kodu buraya gelecek ...
            
            return etfs
            
        except Exception as e:
            logger.error(f"TradingView ETF çekme hatası: {str(e)}")
            return []
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    def collect_pariteler(self):
        """ETF'leri toplar ve veritabanına kaydeder"""
        try:
            logger.info("ETF toplama işlemi başlatılıyor...")
            
            etfs = self.fetch_tradingview_etfs()
            total = len(etfs)
            added = len(self.added_etfs)
            failed = len(self.skipped_etfs)
            
            logger.info(f"ETF toplama tamamlandı. Toplam: {total} | Eklenen: {added} | Hatalı: {failed}")
            return total, added, failed
            
        except Exception as e:
            logger.error(f"ETF toplama hatası: {str(e)}")
            return 0, 0, 0

if __name__ == "__main__":
    try:
        collector = ETFCollector()
        total, added, failed = collector.collect_pariteler()
        logger.info(f"İşlem tamamlandı. Toplam: {total} | Eklenen: {added} | Hatalı: {failed}")
    except KeyboardInterrupt:
        logger.info("Kullanıcı tarafından durduruldu.")
    except Exception as e:
        logger.error(f"Program hatası: {str(e)}") 