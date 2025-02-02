"""
Veritabanı bağlantı sınıfı
"""

import pyodbc
from config import DB_CONFIG

class Database:
    _instance = None
    _connection = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
        return cls._instance
        
    def __init__(self):
        # Windows Authentication veya SQL Server Authentication
        if DB_CONFIG['trusted_connection'].lower() == 'yes':
            self.connection_string = (
                f"DRIVER={DB_CONFIG['driver']};"
                f"SERVER={DB_CONFIG['server']};"
                f"DATABASE={DB_CONFIG['database']};"
                f"Trusted_Connection=yes"
            )
        else:
            self.connection_string = (
                f"DRIVER={DB_CONFIG['driver']};"
                f"SERVER={DB_CONFIG['server']};"
                f"DATABASE={DB_CONFIG['database']};"
                f"UID={DB_CONFIG['user']};"
                f"PWD={DB_CONFIG['password']}"
            )
        
    def connect(self):
        """Veritabanına bağlanır"""
        try:
            if not self._connection or self._connection.closed:
                self._connection = pyodbc.connect(self.connection_string)
            return self._connection
        except Exception as e:
            print(f"Veritabanı bağlantı hatası: {str(e)}")
            return None
            
    def disconnect(self):
        """Veritabanı bağlantısını kapatır"""
        try:
            if self._connection and not self._connection.closed:
                self._connection.close()
        except Exception as e:
            print(f"Bağlantı kapatma hatası: {str(e)}")
            
    def execute_query(self, query, params=None):
        """SQL sorgusunu çalıştır"""
        try:
            conn = self.connect()
            if not conn:
                return None
                
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            return cursor
            
        except Exception as e:
            print(f"Sorgu çalıştırma hatası: {str(e)}")
            return None
            
    def fetch_all(self, query, params=None):
        """Tüm sonuçları getir"""
        cursor = self.execute_query(query, params)
        if cursor:
            try:
                return cursor.fetchall()
            except Exception as e:
                print(f"Veri getirme hatası: {str(e)}")
                
        return None
        
    def fetch_one(self, query, params=None):
        """Tek sonuç getir"""
        cursor = self.execute_query(query, params)
        if cursor:
            try:
                return cursor.fetchone()
            except Exception as e:
                print(f"Veri getirme hatası: {str(e)}")
                
        return None
        
    def execute_non_query(self, query, params=None):
        """INSERT, UPDATE, DELETE gibi sorguları çalıştır"""
        try:
            conn = self.connect()
            if not conn:
                return False
                
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            conn.commit()
            return True
            
        except Exception as e:
            print(f"Sorgu çalıştırma hatası: {str(e)}")
            if conn:
                conn.rollback()
            return False
            
    def __del__(self):
        """Yıkıcı metod"""
        self.disconnect() 