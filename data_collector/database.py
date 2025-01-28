"""
Veritabanı işlemleri için yardımcı fonksiyonlar
"""

import pyodbc
from config import DB_CONFIG

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