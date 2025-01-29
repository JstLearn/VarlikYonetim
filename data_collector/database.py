"""
Veritabanı işlemleri için yardımcı fonksiyonlar
"""

import pyodbc
from config import DB_CONFIG
from datetime import datetime
import pandas as pd
import logging

class Database:
    def __init__(self):
        self.connection_string = (
            f"DRIVER={{{DB_CONFIG['driver']}}};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['user']};"
            f"PWD={DB_CONFIG['password']};"
            "TrustServerCertificate=yes;"
            "Encrypt=no;"
        )
        
    def connect(self):
        try:
            conn = pyodbc.connect(self.connection_string)
            return conn
        except Exception as e:
            logging.error(f"Hata: {str(e)}")
            return None

    def get_last_candle(self, parite):
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
            return None
        finally:
            conn.close()

    def save_candles(self, parite, veriler_df):
        """
        DataFrame'deki verileri kurlar tablosuna kaydeder
        
        Args:
            parite (str): Para birimi çifti (örn: EUR/USD)
            veriler_df (pd.DataFrame): Kur verileri DataFrame'i
        """
        if veriler_df.empty:
            return True

        conn = self.connect()
        if not conn:
            return False

        try:
            cursor = conn.cursor()
            
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
                # Kapanış fiyatını al
                kapanis = float(row['close'])
                
                # USD bazlı fiyat hesapla
                base_currency = parite.split('/')[0]  # İlk para birimi (TRY/USD -> TRY)
                if base_currency != 'USD':
                    # Eğer parite USD ile değilse, USD karşılığını hesapla (1/fiyat)
                    usd_price = 1 / kapanis if kapanis != 0 else None
                else:
                    usd_price = kapanis
                
                data_dict = {
                    'parite': parite,
                    'interval': '1d',  # Günlük veri
                    'tip': 'FOREX',
                    'ulke': base_currency,  # İlk para birimi ülke kodu olarak kullanılır
                    'fiyat': usd_price,  # USD bazlı fiyat
                    'dolar_karsiligi': 1.0 if base_currency == 'USD' else usd_price,  # USD için 1, diğerleri için hesaplanan değer
                    'tarih': tarih.to_pydatetime()  # datetime objesine çevir
                }

                cursor.execute("""
                    IF NOT EXISTS (
                        SELECT 1 FROM kurlar 
                        WHERE parite = ? AND [interval] = ? AND tarih = ?
                    )
                    INSERT INTO kurlar (
                        parite, [interval], tip, ulke, fiyat, dolar_karsiligi, tarih
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, 
                data_dict['parite'], data_dict['interval'], data_dict['tarih'],
                data_dict['parite'], data_dict['interval'], data_dict['tip'],
                data_dict['ulke'], data_dict['fiyat'], data_dict['dolar_karsiligi'],
                data_dict['tarih'])

            conn.commit()
            return True
            
        except Exception as e:
            return False
        finally:
            conn.close()

    def log_operation(self, para_birimi, baslangic_tarihi, bitis_tarihi, durum, mesaj):
        conn = self.connect()
        if not conn:
            return False
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO VeriCekmeLoglari (
                    ParaBirimi, BaslangicTarihi, BitisTarihi, Durum, Mesaj
                ) VALUES (?, ?, ?, ?, ?)
            """, para_birimi, baslangic_tarihi, bitis_tarihi, durum, mesaj)
            
            conn.commit()
            return True
            
        except Exception as e:
            logging.error(f"Log kaydetme hatası: {str(e)}")
            return False
        finally:
            conn.close()

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