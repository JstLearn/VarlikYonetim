"""
Veritabanı işlemleri için yardımcı fonksiyonlar
"""

import pyodbc
from config import DB_CONFIG
from datetime import datetime
import pandas as pd

class Database:
    _instance = None
    _connection = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
        
    def __init__(self):
        if self._initialized:
            return
            
        # Kullanılabilir SQL Server sürücülerini bul
        drivers = [x for x in pyodbc.drivers() if x.startswith('SQL Server')]
        if not drivers:
            raise Exception("SQL Server sürücüsü bulunamadı!")
            
        # En uygun sürücüyü seç
        driver = next((d for d in drivers if 'Native Client' in d), drivers[0])
        
        self.connection_string = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['user']};"
            f"PWD={DB_CONFIG['password']}"
        )
        self._initialized = True
                
    def connect(self):
        """Veritabanına bağlanır ve bağlantıyı döndürür"""
        try:
            if self._connection is None or not self.is_connection_alive():
                self._connection = pyodbc.connect(self.connection_string)
            return self._connection
        except Exception as e:
            print(f"Veritabanı bağlantı hatası: {str(e)}")
            return None

    def is_connection_alive(self):
        """Bağlantının aktif olup olmadığını kontrol eder"""
        if self._connection is None:
            return False
        try:
            self._connection.execute("SELECT 1")
            return True
        except:
            return False

    def close(self):
        """Veritabanı bağlantısını kapatır"""
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception as e:
                print(f"Veritabanı kapatma hatası: {str(e)}")
            finally:
                self._connection = None

    def get_last_candle(self, parite):
        """Son mum verisini getirir"""
        conn = self.connect()
        if not conn:
            return None
            
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 1 tarih
                FROM kurlar
                WHERE parite = ?
                ORDER BY tarih DESC
            """, parite)
            
            row = cursor.fetchone()
            return row[0] if row else None
            
        except Exception as e:
            print(f"Son mum verisi alınamadı ({parite}): {str(e)}")
            return None
            
    def save_candles(self, parite, veriler_df):
        """DataFrame'deki verileri kurlar tablosuna kaydeder"""
        if veriler_df.empty:
            return True

        conn = self.connect()
        if not conn:
            return False
            
        try:
            cursor = conn.cursor()
            kayit_sayisi = 0
            
            # DataFrame'i datetime index'e çevir
            if not isinstance(veriler_df.index, pd.DatetimeIndex):
                if 'Datetime' in veriler_df.columns:
                    veriler_df.set_index('Datetime', inplace=True)
                elif 'Date' in veriler_df.columns:
                    veriler_df.set_index('Date', inplace=True)
                else:
                    veriler_df.index = pd.to_datetime(veriler_df.index)
            
            # Günlük verilere dönüştür
            daily_df = veriler_df.resample('D').agg({
                'close': 'last'  # Gün sonu kapanış
            }).dropna()
            
            for tarih, row in daily_df.iterrows():
                try:
                    # Kapanış fiyatını al
                    kapanis = float(row['close'])
                    
                    # USD bazlı fiyat hesapla
                    base_currency = parite.split('/')[0]
                    if base_currency != 'USD':
                        usd_price = 1 / kapanis if kapanis != 0 else None
                    else:
                        usd_price = kapanis
                    
                    # Veriyi kaydet
                    cursor.execute("""
                        IF NOT EXISTS (
                                SELECT 1 FROM kurlar 
                                WHERE parite = ? AND [interval] = ? AND tarih = ?
                        )
                            INSERT INTO kurlar (
                                parite, [interval], tip, ulke, fiyat, dolar_karsiligi, tarih
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                        parite, '1d', tarih,
                        parite, '1d', 'FOREX',
                        base_currency, usd_price, 1.0 if base_currency == 'USD' else usd_price,
                        tarih)
                    
                    kayit_sayisi += 1
                    
                except Exception as e:
                    print(f"Kayıt hatası ({parite}, {tarih}): {str(e)}")
                    continue

            conn.commit()
            
            if kayit_sayisi > 0:
                print(f"{parite} için {kayit_sayisi} yeni kayıt eklendi ({daily_df.index[0].strftime('%Y-%m-%d')} - {daily_df.index[-1].strftime('%Y-%m-%d')})")
                
            return True
            
        except Exception as e:
            print(f"Veri kaydetme hatası ({parite}): {str(e)}")
            return False

    def __del__(self):
        """Yıkıcı - bağlantıyı kapat"""
        self.close()

def create_connection():
    """Veritabanı bağlantısı oluşturur"""
    conn_str = (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['uid']};"
        f"PWD={DB_CONFIG['pwd']};"
        "TrustServerCertificate=yes;"  # SSL sertifika doğrulamasını atla
        "Encrypt=no;"  # Şifrelemeyi devre dışı bırak
    )
    print("Bağlantı dizesi:", conn_str)  # Bağlantı dizesini logla
    return pyodbc.connect(conn_str)

def create_tables(conn):
    """Gerekli tabloları oluşturur"""
    cursor = conn.cursor()
    
    # Hisse senedi tablosu
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Hisseler' AND xtype='U')
        CREATE TABLE Hisseler (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            Sembol NVARCHAR(20),
            Tarih DATETIME,
            Acilis FLOAT,
            Yuksek FLOAT,
            Dusuk FLOAT,
            Kapanis FLOAT,
            Hacim BIGINT
        )
    """)
    
    # Kripto para tablosu
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Kriptolar' AND xtype='U')
        CREATE TABLE Kriptolar (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            Sembol NVARCHAR(20),
            Tarih DATETIME,
            Acilis FLOAT,
            Yuksek FLOAT,
            Dusuk FLOAT,
            Kapanis FLOAT,
            Hacim FLOAT
        )
    """)
    
    # Döviz tablosu
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Dovizler' AND xtype='U')
        CREATE TABLE Dovizler (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            Sembol NVARCHAR(20),
            Tarih DATETIME,
            Acilis FLOAT,
            Yuksek FLOAT,
            Dusuk FLOAT,
            Kapanis FLOAT
        )
    """)
    
    # Altın tablosu
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Altin' AND xtype='U')
        CREATE TABLE Altin (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            Tur NVARCHAR(50),
            Tarih DATETIME,
            Alis FLOAT,
            Satis FLOAT
        )
    """)
    
    conn.commit()

def insert_stock_data(conn, data):
    """Hisse senedi verilerini veritabanına ekler"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Hisseler (Sembol, Tarih, Acilis, Yuksek, Dusuk, Kapanis, Hacim)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, data)
    conn.commit()

def insert_crypto_data(conn, data):
    """Kripto para verilerini veritabanına ekler"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Kriptolar (Sembol, Tarih, Acilis, Yuksek, Dusuk, Kapanis, Hacim)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, data)
    conn.commit()

def insert_forex_data(conn, data):
    """Döviz verilerini veritabanına ekler"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Dovizler (Sembol, Tarih, Acilis, Yuksek, Dusuk, Kapanis)
        VALUES (?, ?, ?, ?, ?, ?)
    """, data)
    conn.commit()

def insert_gold_data(conn, data):
    """Altın verilerini veritabanına ekler"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO Altin (Tur, Tarih, Alis, Satis)
        VALUES (?, ?, ?, ?)
    """, data)
    conn.commit()

def insert_currency_data(conn, data):
    """Kur verilerini veritabanına ekler"""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO kurlar (parite, [interval], tip, ulke, fiyat, dolar_karsiligi, tarih, kayit_tarihi)
        VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
    """, data)
    conn.commit()

def get_latest_currency_data(conn, parite, interval):
    """Belirli bir parite için en son kur verisini getirir"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TOP 1 fiyat, dolar_karsiligi, tarih
        FROM kurlar
        WHERE parite = ? AND [interval] = ?
        ORDER BY kayit_tarihi DESC
    """, (parite, interval))
    return cursor.fetchone()

def check_currency_exists(conn, parite, interval, tarih):
    """Belirli bir parite ve tarih için veri olup olmadığını kontrol eder"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM kurlar
        WHERE parite = ? AND [interval] = ? AND CONVERT(date, tarih) = CONVERT(date, ?)
    """, (parite, interval, tarih))
    count = cursor.fetchone()[0]
    return count > 0 