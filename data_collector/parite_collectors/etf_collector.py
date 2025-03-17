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

    def find_market_filter_button(self, driver):
        """Sol taraftaki Markets filtresini bulur"""
        try:
            # Sol taraftaki filtreleme bölümüne odaklanarak daha spesifik arama yapalım
            # Filtre bölümünü bulalım
            filter_patterns = [
                "//div[contains(@class, 'tv-screener__content-pane')]",
                "//div[contains(@class, 'tv-screener__field-filter')]",
                "//div[contains(@class, 'tv-screener-sidebar')]",
                "//div[contains(@class, 'left-sidebar')]",
                "//div[contains(@class, 'filter-pane')]"
            ]
            
            filter_container = None
            for pattern in filter_patterns:
                try:
                    elements = driver.find_elements(By.XPATH, pattern)
                    for element in elements:
                        if element.is_displayed():
                            filter_container = element
                            logger.info(f"Filtreleme bölümü bulundu")
                            break
                    if filter_container:
                        break
                except:
                    continue
            
            # Filtreleme bölümü içinde veya doğrudan sayfada Markets butonunu ara
            market_button_patterns = [
                ".//div[contains(@class, 'filter-item') and contains(text(), 'Market')]",
                ".//span[contains(text(), 'Market') and not(contains(text(), 'ETF'))]",
                "//div[contains(@class, 'dropdown') and contains(text(), 'Market')]",
                "//span[contains(text(), 'Market') and not(contains(text(), 'ETF')) and not(contains(text(), 'iShares')) and not(contains(text(), 'Vanguard'))]",
                "//div[contains(@class, 'filter-item')]//span[text()='Market']",
                "//div[contains(@class, 'filter')]//span[text()='Market']",
                "//div[contains(@class, 'filter-field')]//span[text()='Market']"
            ]
            
            # İlk önce filtreleme bölümü içinde arayalım
            market_button = None
            if filter_container:
                for pattern in market_button_patterns:
                    try:
                        elements = filter_container.find_elements(By.XPATH, pattern)
                        for element in elements:
                            if element.is_displayed():
                                text = element.text.strip()
                                # ETF isimleri içermediğinden emin olalım
                                if "ETF" not in text and "iShares" not in text and "Vanguard" not in text:
                                    logger.info(f"Markets filtresi bulundu: '{text}'")
                                    market_button = element
                                    break
                        if market_button:
                            break
                    except:
                        continue
            
            # Filtreleme bölümünde bulamazsak tüm sayfada arayalım
            if not market_button:
                for pattern in market_button_patterns:
                    try:
                        elements = driver.find_elements(By.XPATH, pattern)
                        for element in elements:
                            if element.is_displayed():
                                text = element.text.strip()
                                # ETF isimleri içermediğinden emin olalım
                                if "ETF" not in text and "iShares" not in text and "Vanguard" not in text:
                                    logger.info(f"Markets filtresi bulundu: '{text}'")
                                    market_button = element
                                    break
                        if market_button:
                            break
                    except:
                        continue
            
            # Hala bulamazsak daha spesifik desenleri deneyelim
            if not market_button:
                # Sol tarafta Market filtresi (tam kelime eşleşmesi)
                more_specific_patterns = [
                    "//span[text()='Market' and not(contains(ancestor::div[contains(@class, 'table-cell')], 'ETF'))]",
                    "//span[text()='Markets' and not(contains(ancestor::div[contains(@class, 'table-cell')], 'ETF'))]",
                    "//div[text()='Market' and not(contains(ancestor::div[contains(@class, 'table-cell')], 'ETF'))]",
                    "//div[text()='Markets' and not(contains(ancestor::div[contains(@class, 'table-cell')], 'ETF'))]"
                ]
                
                for pattern in more_specific_patterns:
                    try:
                        elements = driver.find_elements(By.XPATH, pattern)
                        for element in elements:
                            if element.is_displayed():
                                text = element.text.strip()
                                logger.info(f"Kesin Markets filtresi bulundu: '{text}'")
                                market_button = element
                                
                                # Element tıklanabilir değilse, parent elementini almayı deneyelim
                                try:
                                    parent = element.find_element(By.XPATH, "./..")
                                    market_button = parent
                                    logger.info("Markets filtresinin parent elementi kullanılacak")
                                except:
                                    pass
                                
                                break
                        if market_button:
                            break
                    except:
                        continue
            
            if not market_button:
                # Ekran görüntüsü alalım sorunu görelim
                self.take_screenshot(driver, "market_filter_not_found")
                logger.error("Markets filtresi bulunamadı!")
                
                # Sayfa kaynağını yazdıralım
                page_source = driver.page_source
                with open("page_source.html", "w", encoding="utf-8") as f:
                    f.write(page_source)
                logger.info("Sayfa kaynağı kaydedildi: page_source.html")
                
                return None
            
            return market_button
            
        except Exception as e:
            logger.error(f"Markets filtresi arama hatası: {str(e)}")
            return None
            
    def find_world_option(self, driver):
        """Açılan menüde 'Entire World' seçeneğini bulur"""
        try:
            # Açılan menüyü bulmak için biraz bekleyelim
            time.sleep(1)
            
            # Açılan menüyü bulalım
            menu_patterns = [
                "//div[contains(@class, 'menu') and contains(@class, 'dropdown')]",
                "//div[contains(@class, 'menuWrap')]",
                "//div[contains(@class, 'menu') and @role='menu']",
                "//div[contains(@class, 'dropdown-content')]",
                "//div[contains(@class, 'menu-container')]"
            ]
            
            menu = None
            for pattern in menu_patterns:
                elements = driver.find_elements(By.XPATH, pattern)
                for element in elements:
                    if element.is_displayed():
                        menu = element
                        logger.info("Açılan menü bulundu")
                        break
                if menu:
                    break
            
            # Menü elemanlarını bulmak için XPath desenleri
            option_patterns = [
                ".//div[contains(@class, 'item')]",
                ".//div[contains(@class, 'option')]",
                ".//div[@role='option']",
                ".//div[contains(@class, 'dropdown-item')]"
            ]
            
            # İlk olarak "Entire World" içeren seçeneği arayalım
            world_option = None
            world_keywords = ["entire world", "world", "tüm dünya", "global", "worldwide"]
            
            if menu:
                # Menü içinde ara
                all_options = []
                
                for pattern in option_patterns:
                    try:
                        options = menu.find_elements(By.XPATH, pattern)
                        for option in options:
                            if option.is_displayed():
                                all_options.append(option)
                    except:
                        continue
                
                logger.info(f"Menüde {len(all_options)} seçenek bulundu")
                
                # İlk 10 seçeneği logla
                for i, option in enumerate(all_options[:10]):
                    try:
                        logger.info(f"  Seçenek {i+1}: '{option.text}'")
                    except:
                        pass
                
                # Entire World içeren seçeneği bul
                for option in all_options:
                    try:
                        text = option.text.lower().strip()
                        if any(keyword in text for keyword in world_keywords):
                            logger.info(f"Entire World seçeneği bulundu: '{option.text}'")
                            world_option = option
                            break
                    except:
                        continue
                
                # Eğer bulamazsak, ilk seçeneği kullan
                if not world_option and all_options:
                    world_option = all_options[0]
                    try:
                        logger.info(f"İlk seçenek kullanılacak: '{world_option.text}'")
                    except:
                        logger.info("İlk seçenek kullanılacak (metni okunamadı)")
            
            # Menüde bulamazsak doğrudan sayfada arayalım
            if not world_option:
                # Sadece şu anda görünür olan ve açılan menüde olabilecek eleman türleri
                visible_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'item') and @role='option']")
                if visible_elements:
                    # İlk elemanı seçelim
                    world_option = visible_elements[0]
                    try:
                        logger.info(f"İlk görünür menü seçeneği kullanılacak: '{world_option.text}'")
                    except:
                        logger.info("İlk görünür menü seçeneği kullanılacak (metni okunamadı)")
            
            if not world_option:
                self.take_screenshot(driver, "world_option_not_found")
                logger.warning("Entire World seçeneği bulunamadı!")
                return None
            
            return world_option
            
        except Exception as e:
            logger.error(f"Entire World seçeneği arama hatası: {str(e)}")
            return None

    def setup_webdriver(self):
        """Selenium WebDriver'ı ayarlar"""
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

    def try_click_element(self, driver, element, description="element"):
        """Farklı tıklama yöntemlerini dener"""
        try:
            # Normal tıklama dene
            element.click()
            logger.info(f"{description} başarıyla tıklandı")
            return True
        except Exception as e:
            logger.warning(f"Normal tıklama hatası: {str(e)}")
            
            try:
                # JavaScript ile tıklama dene
                driver.execute_script("arguments[0].click();", element)
                logger.info(f"{description} JavaScript ile tıklandı")
                return True
            except Exception as js_err:
                logger.warning(f"JavaScript tıklama hatası: {str(js_err)}")
                
                try:
                    # ActionChains ile tıklama dene
                    actions = ActionChains(driver)
                    actions.move_to_element(element).pause(0.5).click().perform()
                    logger.info(f"{description} ActionChains ile tıklandı")
                    return True
                except Exception as action_err:
                    logger.warning(f"ActionChains tıklama hatası: {str(action_err)}")
                    
                    try:
                        # ActionChains ile farklı bir yaklaşım dene
                        actions = ActionChains(driver)
                        # Element üzerine gel, bekle
                        actions.move_to_element(element).pause(1)
                        # Sayfayı bir miktar kaydır (bazen sayfayı kaydırmak elementleri görünür yapar)
                        driver.execute_script("window.scrollBy(0, 10);")
                        time.sleep(0.5)
                        # Yeniden element üzerine gel ve tıkla
                        actions.move_to_element(element).pause(0.5).click().perform()
                        logger.info(f"{description} gelişmiş ActionChains ile tıklandı")
                        return True
                    except Exception as advanced_err:
                        logger.error(f"Gelişmiş ActionChains tıklama hatası: {str(advanced_err)}")
                        
                        try:
                            # Son çare: koordinat bazlı tıklama
                            location = element.location
                            size = element.size
                            x = location['x'] + size['width'] // 2
                            y = location['y'] + size['height'] // 2
                            
                            # Sayfayı elementin olduğu yere kaydır
                            driver.execute_script(f"window.scrollTo({x}, {y - 100});")
                            time.sleep(0.5)
                            
                            # Koordinat bazlı tıklama
                            actions = ActionChains(driver)
                            actions.move_by_offset(x, y).click().perform()
                            logger.info(f"{description} koordinat bazlı ActionChains ile tıklandı: x={x}, y={y}")
                            return True
                        except Exception as coord_err:
                            logger.error(f"Koordinat bazlı tıklama hatası: {str(coord_err)}")
                            return False

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
            
            # Sayfanın ekran görüntüsünü alalım
            self.take_screenshot(driver, "etf_screener_page")
            
            # Sol taraftaki Market filtresini bul
            market_filter = self.find_market_filter_button(driver)
            
            if market_filter:
                # Market filtresine tıkla - try_click_element metodu kullanılıyor
                if self.try_click_element(driver, market_filter, "Market filtresi"):
                    # Tıklamadan sonra bekle
                    time.sleep(2)
                    
                    # Tıklamadan sonra ekran görüntüsü al
                    self.take_screenshot(driver, "after_market_filter_click")
                    
                    # Entire World seçeneğini bul
                    world_option = self.find_world_option(driver)
                    
                    if world_option:
                        # World seçeneğine tıkla - try_click_element metodu kullanılıyor
                        if self.try_click_element(driver, world_option, "World seçeneği"):
                            # Tıklamadan sonra bekle
                            time.sleep(2)
                            
                            # Tıklamadan sonra ekran görüntüsü al
                            self.take_screenshot(driver, "after_world_option_click")
                            
                            # Apply/OK/Done butonunu bul ve tıkla
                            apply_button_patterns = [
                                "//button[contains(., 'Apply')]",
                                "//button[contains(., 'OK')]",
                                "//button[contains(., 'Done')]",
                                "//button[contains(., 'Uygula')]",
                                "//button[contains(., 'Tamam')]",
                                "//div[@role='button' and contains(., 'Apply')]",
                                "//div[@role='button' and contains(., 'OK')]",
                                "//div[@role='button' and contains(., 'Done')]"
                            ]
                            
                            apply_button = None
                            for pattern in apply_button_patterns:
                                elements = driver.find_elements(By.XPATH, pattern)
                                for element in elements:
                                    if element.is_displayed():
                                        apply_button = element
                                        break
                                if apply_button:
                                    break
                            
                            if apply_button:
                                # Apply butonuna tıkla - try_click_element metodu kullanılıyor
                                self.try_click_element(driver, apply_button, "Uygula butonu")
                            else:
                                logger.warning("Uygula butonu bulunamadı")
                            
                            # ETF verilerinin yüklenmesi için bekle
                            time.sleep(3)
                        else:
                            logger.error("World seçeneğine tıklama başarısız oldu")
                    else:
                        logger.error("World seçeneği bulunamadı!")
                else:
                    logger.error("Market filtresine tıklama başarısız oldu")
            else:
                logger.error("Market filtresi bulunamadı!")
                
                # Son çare: Sayfada görünür tüm elementleri logla
                logger.info("Sayfadaki tüm görünür elementler aranıyor...")
                
                # Sayfa yüklenirken farklı bir yöntem deneyelim
                try:
                    WebDriverWait(driver, 10).until(
                        EC.visibility_of_element_located((By.XPATH, "//div[contains(@class, 'etf-screener')]"))
                    )
                    logger.info("ETF Screener sayfası yüklendi")
                except:
                    logger.warning("ETF Screener sayfası beklenirken zaman aşımı")
                
                # Sayfadaki tüm metin içeren elementleri tara
                all_elements = driver.find_elements(By.XPATH, "//*[text()]")
                visible_elements = []
                
                for elem in all_elements:
                    try:
                        if elem.is_displayed() and elem.text.strip():
                            visible_elements.append((elem, elem.text.strip()))
                    except:
                        continue
                
                logger.info(f"Sayfada {len(visible_elements)} görünür element bulundu:")
                for i, (_, text) in enumerate(visible_elements[:20]):  # İlk 20 elementi göster
                    logger.info(f"  {i+1}: '{text}'")
                
                # Market ile ilgili elementleri filtrele
                market_elements = [elem for elem, text in visible_elements if 'market' in text.lower() or 'piyasa' in text.lower()]
                if market_elements:
                    logger.info(f"{len(market_elements)} adet 'Market' içeren element bulundu")
                    # Bu elementleri deneyelim
                    for i, elem in enumerate(market_elements[:5]):  # İlk 5 elementi dene
                        logger.info(f"  {i+1}. Market elementi deneniyor: '{elem.text}'")
                        if self.try_click_element(driver, elem, f"Alternatif Market {i+1}"):
                            logger.info(f"Alternatif Market {i+1} başarıyla tıklandı")
                            time.sleep(2)
                            self.take_screenshot(driver, f"after_alt_market_{i+1}_click")
                            break
            
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