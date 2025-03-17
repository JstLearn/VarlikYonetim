"""
TradingView üzerinden ETF'leri toplayan sınıf
"""

import requests
from bs4 import BeautifulSoup
import sys
import os

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
        
        # Logger mesajı run metoduna taşınıyor, burada çalıştırılmamalı
        # Böylece kolektör başlatıldığında değil, run() çağrıldığında bildirilecek
        # logger.info("ETFCollector başlatıldı")
        
    def normalize_exchange_name(self, exchange, country):
        """
        Borsa adını normalize eder
        """
        # Borsa adı boşsa veya bilinmiyorsa
        if not exchange or exchange == "UNKNOWN":
            # Ülke kodunu kullan
            if country and country != "UNKNOWN":
                return f"{country}_ETF"
            return "GLOBAL_ETF"
        
        # Borsa adını temizle ve normalize et
        exchange = exchange.strip().upper()
        
        # Özel durumlar
        exchange_map = {
            "NYSE": "NYSE",
            "NASDAQ": "NASDAQ",
            "AMEX": "AMEX",
            "LSE": "LSE",
            "TSE": "TSE",
            "SSE": "SSE",
            "SZSE": "SZSE",
            "HKEX": "HKEX",
            "ASX": "ASX",
            "BSE": "BSE",
            "NSE": "NSE",
            "BIST": "BIST",
            "MOEX": "MOEX",
            "JSE": "JSE",
            "BM&F": "BMFBOVESPA",
            "BMV": "BMV",
            "TSX": "TSX"
        }
        
        # Eşleşen borsa adı varsa onu kullan
        for key, value in exchange_map.items():
            if key in exchange:
                return value
        
        # Eşleşme yoksa orijinal adı kullan (20 karakterle sınırla)
        return exchange[:20]

    def sync_pariteler_to_db(self, pariteler):
        """
        ETF'leri veritabanına senkronize eder
        
        Args:
            pariteler: Eklenecek ETF listesi
            
        Returns:
            tuple: (eklenen_sayısı, atlanan_sayısı)
        """
        try:
            eklenen = 0
            atlanan = 0
            
            # Her bir ETF için
            for parite in pariteler:
                try:
                    # Parite zaten var mı kontrol et
                    query = """
                    SELECT 1 FROM pariteler WITH (NOLOCK)
                    WHERE parite = ? AND borsa = ? AND tip = 'ETF'
                    """
                    result = self.db.fetch_one(query, (parite['parite'], parite['borsa']))
                    
                    if result:
                        # Parite zaten var, güncelle
                        update_query = """
                        UPDATE pariteler 
                        SET aktif = ?, 
                            ulke = ?,
                            aciklama = ?,
                            veri_var = 0,
                            veriler_guncel = 0
                        WHERE parite = ? AND borsa = ? AND tip = 'ETF'
                        """
                        update_params = (
                            parite['aktif'],
                            parite['ulke'],
                            parite['aciklama'],
                            parite['parite'],
                            parite['borsa']
                        )
                        if self.db.execute_non_query(update_query, update_params):
                            atlanan += 1
                            self.skipped_etfs.append(parite)
                            self.skipped_reasons[parite['parite']] = "Veritabanında zaten mevcut"
                    else:
                        # Yeni parite ekle
                        insert_query = """
                        INSERT INTO pariteler (
                            parite, aktif, borsa, tip, ulke, 
                            aciklama, veri_var, veriler_guncel, kayit_tarihi
                        ) VALUES (
                            ?, ?, ?, 'ETF', ?, ?, 0, 0, GETDATE()
                        )
                        """
                        insert_params = (
                            parite['parite'],
                            parite['aktif'],
                            parite['borsa'],
                            parite['ulke'],
                            parite['aciklama']
                        )
                        if self.db.execute_non_query(insert_query, insert_params):
                            eklenen += 1
                            self.added_etfs.append(parite)
                        
                except Exception as e:
                    logger.error(f"ETF veritabanı işlem hatası ({parite['parite']}): {str(e)}")
                    self.skipped_etfs.append(parite)
                    self.skipped_reasons[parite['parite']] = f"Veritabanı hatası: {str(e)}"
                    continue
            
            return eklenen, atlanan
            
        except Exception as e:
            logger.error(f"Veritabanı senkronizasyon hatası: {str(e)}")
            return 0, 0
        
    def setup_webdriver(self):
        """
        Selenium WebDriver'ı headless modda ayarlar
        """
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument(f"user-agent={self.headers['User-Agent']}")
            
            # Sayfa kaydırma için window size ayarı
            chrome_options.add_argument("--window-size=1920,1080")
            
            # WebDriver'ı başlat
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)  # Sayfa yükleme zaman aşımı
            
            return driver
        except Exception as e:
            logger.error(f"WebDriver başlatma hatası: {str(e)}")
            return None
        
    def fetch_tradingview_etfs(self):
        """
        TradingView'dan tüm dünya ETF'lerini çeker (Selenium WebDriver kullanarak)
        """
        logger.info("TradingView'dan ETF'ler çekiliyor (Selenium WebDriver ile)...")
        
        driver = None
        try:
            # WebDriver ayarla
            driver = self.setup_webdriver()
            if not driver:
                logger.error("WebDriver başlatılamadı!")
                return []
            
            # TradingView ETF Screener sayfasını aç
            driver.get(self.tradingview_url)
            logger.info("TradingView ETF Screener sayfası açıldı")
            
            # Sayfanın yüklenmesini bekle
            wait = WebDriverWait(driver, 15)
            
            # Sayfa tamamen yüklensin
            time.sleep(5)
            
            # Market filtresini seçmeye çalış, ancak hataları yok say
            # Filtresiz de devam edebiliriz, yeterince ETF toplayabiliriz
            try:
                # TradingView sayfası tamamen yüklenene kadar bekle
                logger.info("TradingView sayfası tamamen yükleniyor...")
                try:
                    # Sayfa yüklendiğine dair bir element bekleyelim
                    wait.until(
                        EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'table-')]"))
                    )
                    time.sleep(2)  # Ekstra bekleme
                except TimeoutException:
                    logger.warning("Sayfa yükleme zaman aşımı, devam ediliyor...")
            except Exception as e:
                logger.warning(f"Sayfa yükleme işleminde genel hata: {str(e)}")
                logger.info("Devam ediliyor...")
                
            # Market filtresini açma işlemi
            logger.info("Market filtresini aramaya başlıyor...")
            
            # JavaScript ile market filtre düğmelerini bulmayı dene
            try:
                # JavaScript ile tüm filtreleri bul ve market içerenlere tıkla
                js_filters_script = """
                const getTextContent = (element) => {
                    return element.textContent || element.innerText || '';
                };
                
                // Tüm butonları bul
                const allButtons = Array.from(document.querySelectorAll('button, [role="button"]'));
                
                // İçeriği "Market" veya "Piyasa" olan butonu bul
                const marketButton = allButtons.find(btn => {
                    const text = getTextContent(btn).toLowerCase();
                    return text.includes('market') || text.includes('piyasa');
                });
                
                if (marketButton) {
                    marketButton.scrollIntoView({block: 'center'});
                    return true;
                }
                
                // Filtre butonlarını bul (TradingView'da genelde filterButton class'ı içerir)
                const filterButtons = Array.from(document.querySelectorAll('.filterButton, [data-name="filter-button"]'));
                if (filterButtons.length > 0) {
                    filterButtons[0].scrollIntoView({block: 'center'});
                    return true;
                }
                
                return false;
                """
                
                found_filter = driver.execute_script(js_filters_script)
                if found_filter:
                    logger.info("JavaScript ile bir filtre butonu bulundu ve görünür hale getirildi")
                    time.sleep(2)
            except Exception as e:
                logger.warning(f"JavaScript filtre arama hatası: {str(e)}")
            
            # Farklı market filtre seçicileri dene
            filter_selectors = [
                "//button[contains(@class, 'filterButton')]",
                "//button[contains(text(), 'Market')]",
                "//button[contains(text(), 'Piyasa')]",
                "//span[contains(text(), 'Market')]/parent::button",
                "//span[contains(text(), 'Piyasa')]/parent::button",
                "//div[contains(@class, 'button') and contains(text(), 'Market')]",
                "//div[contains(@class, 'button') and contains(text(), 'Piyasa')]",
                "//div[contains(@class, 'filter') and contains(@class, 'button')]",
                "//div[contains(@class, 'dropdown')]",
                "//div[contains(@data-name, 'market-filter')]",
                "//div[contains(@data-name, 'filter')]"
            ]
            
            market_button = None
            
            # Her bir seçiciyi dene
            for selector in filter_selectors:
                buttons = driver.find_elements(By.XPATH, selector)
                if buttons:
                    for button in buttons:
                        try:
                            # Butonun görünür ve tıklanabilir olduğunu kontrol et
                            if button.is_displayed() and button.is_enabled():
                                button_text = button.text.strip()
                                logger.info(f"Potansiyel filtre butonu bulundu: '{button_text}' ({selector})")
                                market_button = button
                                break
                        except Exception as e:
                            logger.debug(f"Buton kontrolünde hata: {str(e)}")
                
                if market_button:
                    break
            
            # Market butonu bulunamadıysa, sayfadaki tüm butonları kontrol et ve logla
            if not market_button:
                logger.info("Standart seçicilerle filtre bulunamadı, tüm butonları kontrol ediyorum...")
                all_buttons = driver.find_elements(By.TAG_NAME, "button")
                
                for i, button in enumerate(all_buttons):
                    try:
                        if button.is_displayed() and button.is_enabled():
                            button_text = button.text.strip()
                            logger.info(f"Buton {i+1}: '{button_text}'")
                            
                            # Market veya filtre içeren herhangi bir buton varsa seç
                            if button_text and ("Market" in button_text or "Piyasa" in button_text or 
                                              "market" in button_text.lower() or "filter" in button_text.lower() or 
                                              "filtre" in button_text.lower()):
                                market_button = button
                                logger.info(f"Aday market filtre butonu bulundu: '{button_text}'")
                                break
                                
                            # İlk birkaç görünür butonu potansiyel filtre butonu olarak kabul et
                            if i < 5 and button_text:
                                market_button = button
                                logger.info(f"İlk görünür butonlardan biri seçildi: '{button_text}'")
                                break
                    except Exception as e:
                        logger.debug(f"Buton {i+1} kontrolünde hata: {str(e)}")
            
            # Market butonuna tıkla
            if market_button:
                try:
                    # Butonu görünür hale getir
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", market_button)
                    time.sleep(1)
                    
                    # Klasik tıklama dene
                    try:
                        market_button.click()
                        logger.info("Market filtre düğmesine tıklandı (normal tıklama)")
                    except Exception as click_error:
                        logger.warning(f"Normal tıklama başarısız: {str(click_error)}")
                        
                        # JavaScript ile tıklamayı dene
                        try:
                            driver.execute_script("arguments[0].click();", market_button)
                            logger.info("Market filtre düğmesine tıklandı (JavaScript tıklama)")
                        except Exception as js_click_error:
                            logger.warning(f"JavaScript tıklama başarısız: {str(js_click_error)}")
                            
                            # Son çare olarak Actions zinciri ile tıklamayı dene
                            try:
                                from selenium.webdriver.common.action_chains import ActionChains
                                actions = ActionChains(driver)
                                actions.move_to_element(market_button).pause(1).click().perform()
                                logger.info("Market filtre düğmesine tıklandı (Actions zinciri)")
                            except Exception as action_error:
                                logger.warning(f"Actions tıklama başarısız: {str(action_error)}")
                                raise Exception("Tüm tıklama yöntemleri başarısız oldu")
                    
                    time.sleep(3)  # Filtrenin açılması için bekle
                    
                    # Filtre açıldıktan sonra JavaScript ile dünya listesini bulmayı dene
                    try:
                        js_world_script = """
                        const getTextContent = (element) => {
                            return element.textContent || element.innerText || '';
                        };
                        
                        // Açılır menü öğelerini bul
                        const findMenuItems = () => {
                            // TradingView'da kullanılan sınıflar ve öznitelikler
                            const menuSelectors = [
                                '.menu-DhK_9GD4 .menuItem-DhK_9GD4',
                                '.dialog-DhK_9GD4 .menuItem-DhK_9GD4',
                                '[data-name="menu-item"]',
                                '.item',
                                '.menuItem',
                                '.menu .item',
                                '.dropdown-item'
                            ];
                            
                            for (const selector of menuSelectors) {
                                const items = document.querySelectorAll(selector);
                                if (items.length > 0) return Array.from(items);
                            }
                            
                            return [];
                        };
                        
                        const menuItems = findMenuItems();
                        
                        // Dünya veya global seçeneğini bul
                        const worldKeywords = ['entire world', 'all', 'world', 'global', 'tümü'];
                        const worldItem = menuItems.find(item => {
                            const text = getTextContent(item).toLowerCase();
                            return worldKeywords.some(keyword => text.includes(keyword));
                        });
                        
                        if (worldItem) {
                            worldItem.scrollIntoView({block: 'center'});
                            return true;
                        } else if (menuItems.length > 0) {
                            // İlk öğeyi görünür yap
                            menuItems[0].scrollIntoView({block: 'center'});
                            return true;
                        }
                        
                        return false;
                        """
                        
                        found_world = driver.execute_script(js_world_script)
                        if found_world:
                            logger.info("JavaScript ile dünya seçeneği veya ilk menü öğesi bulundu")
                            time.sleep(1)
                    except Exception as e:
                        logger.warning(f"JavaScript dünya öğesi arama hatası: {str(e)}")
                    
                    # "Entire world" veya benzeri seçeneği ara
                    logger.info("Dünya ETF seçeneği aranıyor...")
                    world_option_found = False
                    
                    # Farklı XPath'lerle seçenekleri ara - daha kapsamlı liste
                    world_option_xpaths = [
                        "//div[contains(text(), 'Entire world')]",
                        "//div[contains(text(), 'All') and not(contains(text(), 'Allow'))]", 
                        "//div[contains(text(), 'Tümü') and not(contains(text(), 'Tüm'))]",
                        "//span[contains(text(), 'Entire world')]",
                        "//span[contains(text(), 'All') and not(contains(text(), 'Allow'))]",
                        "//span[contains(text(), 'World')]",
                        "//span[contains(text(), 'Global')]",
                        "//div[contains(@class, 'menuItem') and contains(text(), 'World')]",
                        "//div[contains(@class, 'menuItem') and contains(text(), 'Global')]",
                        "//div[contains(@class, 'menuItem') and contains(text(), 'All')]",
                        "//div[contains(@class, 'menuItem') and contains(text(), 'Entire')]",
                        "//div[@role='menuitem' and contains(text(), 'World')]",
                        "//div[@role='menuitem' and contains(text(), 'All')]",
                        "//div[@role='menuitem' and contains(text(), 'Global')]",
                        "//div[contains(@class, 'dropdown-item') and contains(text(), 'All')]",
                        "//div[contains(@class, 'item') and contains(text(), 'All')]",
                        "//div[contains(@class, 'item') and contains(text(), 'World')]"
                    ]
                    
                    # XPath ifadelerini sırayla dene
                    for xpath in world_option_xpaths:
                        world_options = driver.find_elements(By.XPATH, xpath)
                        if world_options:
                            for option in world_options:
                                try:
                                    if option.is_displayed():
                                        logger.info(f"Dünya seçeneği bulundu: '{option.text}' ({xpath})")
                                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", option)
                                        time.sleep(1)
                                        # Farklı tıklama stratejileri dene
                                        try:
                                            option.click()
                                            logger.info("Dünya ETF seçeneğine tıklandı (normal tıklama)")
                                        except Exception as click_error:
                                            logger.error(click_error)
                                            world_option_found = True
                                except Exception as e:
                                    logger.error(e)
                                    continue
                                break
                    
                    # Dünya seçeneği bulunamadıysa tüm açık menü öğelerini listele ve ilkini seç
                    if not world_option_found:
                        menu_items = []
                        
                        # CSS seçicilerle tüm açılır menü öğelerini bul
                        menu_selectors = [
                            "div.menu div.menuItem", 
                            "div.menu div.item", 
                            "div.dropdown-menu div.dropdown-item",
                            "div[role='menu'] div[role='menuitem']",
                            "div.dropdown div.item",
                            ".menu .menuItem",
                            ".dialog .menuItem"
                        ]
                        
                        # CSS seçicileri dene
                        for selector in menu_selectors:
                            items = driver.find_elements(By.CSS_SELECTOR, selector)
                            if items:
                                menu_items = items
                                logger.info(f"{len(items)} adet menü öğesi bulundu ({selector})")
                                break
                        
                        # CSS seçicilerle bulunamazsa XPath ile dene
                        if not menu_items:
                            menu_items = driver.find_elements(By.XPATH, "//div[contains(@class, 'menu')]/div[contains(@class, 'item') or contains(@class, 'menuItem')]")
                        
                        # Menü öğelerini kontrol et
                        if not menu_items:
                            logger.warning("Açılır menüde hiç öğe bulunamadı")
                        else:
                            logger.warning(f"Dünya ETF seçeneği bulunamadı, açılan menüde {len(menu_items)} öğe var")
                            
                            # Öğeleri logla ve ilk görünür öğeyi seç
                            for i, item in enumerate(menu_items):
                                try:
                                    if item.is_displayed():
                                        item_text = item.text.strip()
                                        logger.info(f"Menü öğesi {i+1}: '{item_text}'")
                                        
                                        # İlk görünür öğeyi seç
                                        if not world_option_found:
                                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", item)
                                            time.sleep(1)
                                            
                                            try:
                                                item.click()
                                                logger.info(f"İlk menü öğesi seçildi: '{item_text}'")
                                            except Exception as click_error:
                                                logger.warning(f"Normal tıklama başarısız: {str(click_error)}")
                                                driver.execute_script("arguments[0].click();", item)
                                                logger.info(f"İlk menü öğesi seçildi (JavaScript): '{item_text}'")
                                            
                                            time.sleep(2)
                                            world_option_found = True
                                            break
                                except Exception as e:
                                    logger.debug(f"Menü öğesi {i+1} kontrolünde hata: {str(e)}")
                                    continue
                    
                    # Market butonuna tıklama ve filtre işlemleri
                    if world_option_found:
                        try:
                            js_apply_script = """
                            const getTextContent = (element) => {
                                return element.textContent || element.innerText || '';
                            };
                            const applyKeywords = ['apply', 'ok', 'tamam', 'uygula'];
                            const buttons = Array.from(document.querySelectorAll('button'));
                            const applyButton = buttons.find(btn => {
                                const text = getTextContent(btn).toLowerCase();
                                return applyKeywords.some(keyword => text.includes(keyword));
                            });
                            if (applyButton) {
                                applyButton.scrollIntoView({block: 'center'});
                                return true;
                            }
                            return false;
                            """
                            found_apply = driver.execute_script(js_apply_script)
                            if found_apply:
                                logger.info("JavaScript ile uygulama düğmesi bulundu")
                                time.sleep(1)
                        except Exception as e:
                            logger.warning(f"JavaScript uygulama düğmesi arama hatası: {str(e)}")
                except Exception as e:
                    logger.warning(f"Market butonuna tıklama hatası: {str(e)}")
                    logger.warning("Dünya ETF seçeneği seçilemedi, işlem iptal ediliyor")
                    try:
                        # Market butonuna tıkla
                        driver.find_element(By.TAG_NAME, "body").click()
                        time.sleep(1)
                    except Exception as e:
                        logger.warning(f"Market filtre etkileşim hatası: {str(e)}")
            
            # ETF tablosunu bekleme
            try:
                table = wait.until(
                    EC.presence_of_element_located((By.XPATH, "//table[contains(@class, 'table-')]"))
                )
                logger.info("ETF tablosu bulundu")
            except TimeoutException:
                logger.error("TradingView'da ETF tablosu bulunamadı - zaman aşımı!")
                return []
            
            # TradingView'ın gösterdiği maksimum ETF sayısı (gözlemlenen değer)
            tradingview_max_limit = 400
            
            # Toplam ETF sayısını bulmaya çalış
            try:
                total_etfs_element = driver.find_element(By.XPATH, "//div[contains(@class, 'title') and contains(., 'ETF') and contains(., 'found')]")
                total_etfs_text = total_etfs_element.text
                
                # "xxxx ETFs found" formatından sayıyı çıkar
                import re
                match = re.search(r'(\d+,?\d*)\s+ETF', total_etfs_text)
                if match:
                    total_etfs_text = match.group(1).replace(',', '')
                    total_etfs = int(total_etfs_text)
                    logger.info(f"Toplam {total_etfs} ETF bulunduğu tespit edildi")
                else:
                    # TradingView'ın maksimum gösterimi bir sınır olarak kullan
                    total_etfs = tradingview_max_limit
                    logger.warning(f"Toplam ETF sayısı belirlenemedi, maksimum limit: {total_etfs}")
            except:
                # TradingView'ın maksimum gösterimi bir sınır olarak kullan
                total_etfs = tradingview_max_limit
                logger.warning(f"Toplam ETF sayısı bulunamadı, maksimum limit: {total_etfs}")
            
            # Toplanacak ETF sayısını hesapla (gerçekçi bir limit belirle)
            target_etfs = min(total_etfs, tradingview_max_limit)
            logger.info(f"Hedeflenen ETF sayısı: {target_etfs}")
            
            # Sayfayı aşağı kaydırarak daha fazla ETF yükle
            current_etfs = 0
            last_etfs = 0
            scroll_pause_time = 1
            no_progress_count = 0
            max_no_progress = 5  # Ardışık 5 kez ilerleme olmazsa dur
            
            logger.info("Sayfa kaydırma başlatılıyor...")
            
            while current_etfs < target_etfs and no_progress_count < max_no_progress:
                # Tablonun en altına kaydır
                driver.execute_script("arguments[0].scrollIntoView(false);", table)
                time.sleep(scroll_pause_time)
                
                # Alternatif olarak sayfanın sonuna kaydır
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(scroll_pause_time)
                
                # ETF satırlarını kontrol et
                rows = driver.find_elements(By.XPATH, "//table[contains(@class, 'table-')]//tbody/tr")
                current_etfs = len(rows)
                
                # İlerlemeyi logla
                logger.info(f"Sayfa kaydırıldı - Şu ana kadar {current_etfs} ETF yüklendi")
                
                # Eğer yeni ETF yüklenmediyse, ilerleme yok sayacını artır
                if current_etfs == last_etfs:
                    no_progress_count += 1
                    logger.info(f"İlerleme yok ({no_progress_count}/{max_no_progress})")
                    
                    # İlerleme yoksa farklı kaydırma stratejileri dene
                    if no_progress_count % 2 == 0:
                        # 500 piksel yukarı, sonra aşağı kaydır (bazen çalışır)
                        driver.execute_script("window.scrollBy(0, -500);")
                        time.sleep(0.5)
                        driver.execute_script("window.scrollBy(0, 700);")
                    else:
                        # Rastgele bir ETF'e tıkla, sonra geri gel (sayfayı yenileme etkisi)
                        try:
                            if len(rows) > 10:
                                random_index = random.randint(5, min(20, len(rows)-1))
                                cell = rows[random_index].find_element(By.TAG_NAME, "td")
                                driver.execute_script("arguments[0].scrollIntoView();", cell)
                                cell.click()
                                time.sleep(0.5)
                                driver.back()
                                logger.info("Sayfayı canlandırmak için etkileşim yapıldı")
                        except:
                            pass
                else:
                    # Yeni ETF'ler yüklendiyse sayacı sıfırla
                    no_progress_count = 0
                    last_etfs = current_etfs
                
                # Her 500 ETF'de bir daha uzun bekle (sayfa tamamen yüklensin)
                if current_etfs % 500 < 50 and current_etfs > 0:
                    time.sleep(2)
            
            # Maksimum ilerleme sayısına ulaşıldığında veya toplanan maksimum ETF sayısına eriştiğimizde sonlandır
            if no_progress_count >= max_no_progress:
                logger.info(f"Maksimum ilerleme sayısına ulaşıldı, ETF toplama işlemi sonlandırılıyor. Toplanan ETF sayısı: {current_etfs}")
            
            if current_etfs >= tradingview_max_limit:
                logger.info(f"TradingView'ın maksimum gösterdiği ETF sayısına ulaşıldı: {current_etfs}")
            
            # Tablodaki tüm satırları son bir kez kontrol et
            rows = driver.find_elements(By.XPATH, "//table[contains(@class, 'table-')]//tbody/tr")
            
            if not rows:
                # Alternatif olarak div tabanlı satırları dene
                logger.info("Standart tablo satırları bulunamadı, alternatif arama yapılıyor...")
                rows = driver.find_elements(By.XPATH, "//div[contains(@class, 'listContainer')]//div[contains(@class, 'row')]")
            
            if not rows:
                logger.error("TradingView'da ETF satırları bulunamadı!")
                return []
            
            # İlerleme bilgisi için
            total_rows = len(rows)
            logger.info(f"Toplam {total_rows} ETF satırı bulundu, işleniyor...")
            
            etfs = []
            
            # ETF'leri işle
            max_rows = min(target_etfs, total_rows)
            
            # Hızlı işleme için ilerleme loglarını azalt
            log_interval = 100  # Her 100 ETF'de bir log göster
            
            etfs = []
            batch_size = 100  # Her seferde 100 ETF'yi veritabanına ekle
            current_batch = []
            
            for i in range(max_rows):
                try:
                    # İlerleme mesajını azalt
                    if i % log_interval == 0:
                        logger.info(f"ETF satırları işleniyor: {i}/{max_rows}")
                    
                    try:
                        # Satırı tekrar al (stale element hatası olmaması için)
                        row = driver.find_elements(By.XPATH, "//table[contains(@class, 'table-')]//tbody/tr")[i]
                    except IndexError:
                        rows_count = len(driver.find_elements(By.XPATH, "//table//tbody/tr"))
                        if i >= rows_count:
                            logger.warning(f"ETF satırı indeksi {i} aşıldı. Toplam satır sayısı: {rows_count}. İşleme sona eriyor.")
                            break
                        continue
                        
                    # ETF verilerini çıkar ve batch'e ekle
                    etf_info = self._extract_etf_info(row)
                    if etf_info:
                        current_batch.append(etf_info)
                        etfs.append(etf_info)
                        
                        # Batch dolduğunda veritabanına ekle
                        if len(current_batch) >= batch_size:
                            eklenen, atlanan = self.sync_pariteler_to_db(current_batch)
                            logger.info(f"Batch işlemi: {eklenen} eklendi, {atlanan} atlandı")
                            current_batch = []
                    
                except Exception as e:
                    logger.warning(f"ETF satırı işleme hatası (satır {i}): {str(e)}")
                    continue
            
            # Kalan batch'i işle
            if current_batch:
                eklenen, atlanan = self.sync_pariteler_to_db(current_batch)
                logger.info(f"Son batch işlemi: {eklenen} eklendi, {atlanan} atlandı")
            
            logger.info(f"{len(etfs)} ETF TradingView'dan başarıyla çekildi")
            return etfs
            
        except Exception as e:
            logger.error(f"TradingView ETF çekme genel hatası: {str(e)}")
            return []
        finally:
            # WebDriver'ı kapat
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            
    def _extract_etf_info(self, row):
        """Bir satırdan ETF bilgilerini çıkarır"""
        try:
            # Sembol hücresi
            symbol_cell = row.find_elements(By.TAG_NAME, "td")[0]
            symbol_element = symbol_cell.find_element(By.TAG_NAME, "a")
            symbol = symbol_element.text.strip()
            
            # Borsa bilgisi
            exchange = "UNKNOWN"
            try:
                sup_element = symbol_cell.find_element(By.TAG_NAME, "sup")
                exchange = sup_element.text.strip()
            except NoSuchElementException:
                pass
            
            # İsim
            name = ""
            try:
                name_cell = row.find_elements(By.TAG_NAME, "td")[1]
                name = name_cell.text.strip()
            except (IndexError, NoSuchElementException):
                name = symbol
            
            # Ülke bilgisi
            country = "UNKNOWN"
            try:
                flag_element = symbol_cell.find_element(By.XPATH, ".//i[contains(@class, 'flag-')]")
                flag_class = flag_element.get_attribute("class")
                
                for class_name in flag_class.split():
                    if class_name.startswith("flag-") and not "flag-CyFdKRxR" in class_name:
                        country = class_name.replace("flag-", "").upper()
                        break
            except NoSuchElementException:
                pass
            
            # Fiyat ve para birimi
            currency = "USD"  # Varsayılan para birimi
            try:
                price_cell = row.find_elements(By.TAG_NAME, "td")[2]
                price_text = price_cell.text.strip()
                
                # Para birimi tespiti
                currency_symbols = {
                    "$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY", 
                    "₩": "KRW", "₺": "TRY", "₽": "RUB", "₹": "INR",
                    "A$": "AUD", "C$": "CAD", "HK$": "HKD", "S$": "SGD",
                    "CHF": "CHF", "SEK": "SEK", "NOK": "NOK", "DKK": "DKK",
                    "PLN": "PLN", "CZK": "CZK", "HUF": "HUF", "MXN": "MXN",
                    "BRL": "BRL", "ZAR": "ZAR", "CNY": "CNY", "CNH": "CNH"
                }
                
                for symbol_code, currency_code in currency_symbols.items():
                    if symbol_code in price_text:
                        currency = currency_code
                        break
                
                if '(' in price_text and ')' in price_text:
                    parts = price_text.split('(')
                    if len(parts) > 1:
                        possible_currency = parts[1].split(')')[0].strip()
                        if possible_currency in currency_symbols.values():
                            currency = possible_currency
            except (IndexError, NoSuchElementException):
                pass
            
            # Borsa adını normalize et
            exchange = self.normalize_exchange_name(exchange, country)
            
            # Açıklama oluştur
            description = f"{name} - {exchange} ETF"
            
            # Veritabanı kaydı için ETF bilgisi
            return {
                'parite': f"{symbol}/{currency}",
                'aktif': 1,
                'borsa': exchange,
                'tip': 'ETF',
                'ulke': country,
                'aciklama': description
            }
            
        except Exception as e:
            logger.error(f"ETF bilgisi çıkarma hatası: {str(e)}")
            return None

    def collect_pariteler(self):
        """
        TradingView'dan ETF'leri toplar ve veritabanına kaydeder
        """
        try:
            print("\n" + "="*50)
            print("TRADINGVIEW ETF COLLECTOR BAŞLATILIYOR...")
            print("="*50 + "\n")
            
            etfs = self.fetch_tradingview_etfs()
            total = len(etfs)
            added = len(self.added_etfs)
            failed = len(self.skipped_etfs)
            
            print("\n" + "="*50)
            print(f"TRADINGVIEW ETF COLLECTOR TAMAMLANDI.")
            print(f"Toplam: {total} | Eklenen: {added} | Hatalı: {failed}")
            print("="*50 + "\n")
            
            return total, added, failed
            
        except KeyboardInterrupt:
            print("\nKullanıcı tarafından durduruldu.")
            raise
        except Exception as e:
            print(f"\nProgram hatası: {str(e)}")
            return 0, 0, 0

if __name__ == "__main__":
    try:
        print("\n" + "="*50)
        print("TRADINGVIEW ETF COLLECTOR BAŞLATILIYOR...")
        print("="*50 + "\n")
        
        collector = ETFCollector()
        total, added, failed = collector.collect_pariteler()
        
        print("\n" + "="*50)
        print(f"TRADINGVIEW ETF COLLECTOR TAMAMLANDI.")
        print(f"Toplam: {total} | Eklenen: {added} | Hatalı: {failed}")
        print("="*50 + "\n")
    except KeyboardInterrupt:
        print("\nKullanıcı tarafından durduruldu.")
    except Exception as e:
        print(f"\nProgram hatası: {str(e)}")
