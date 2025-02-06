"""
Veritabanı işlemleri için yardımcı fonksiyonlar
"""

import pyodbc
from .config import DB_CONFIG

class Database:
    def __init__(self):
        self._connection = None
        self._cursor = None
        
    def connect(self):
        """Veritabanına bağlanır"""
        try:
            if not self._connection:
                self._connection = pyodbc.connect(**DB_CONFIG)
                self._cursor = self._connection.cursor()
            return self._connection
        except Exception as e:
            print(f"Veritabanı bağlantı hatası: {str(e)}")
            self._connection = None
            self._cursor = None
            return None
            
    def disconnect(self):
        """Veritabanı bağlantısını kapatır"""
        try:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            if self._connection:
                self._connection.close()
                self._connection = None
        except Exception as e:
            print(f"Bağlantı kapatma hatası: {str(e)}")
        finally:
            self._connection = None
            self._cursor = None
            
    def cursor(self):
        """Veritabanı cursor'ını döndürür"""
        if not self._connection or not self._cursor:
            self.connect()
        return self._cursor

    def commit(self):
        """Değişiklikleri kaydeder"""
        try:
            if self._connection:
                self._connection.commit()
        except Exception as e:
            print(f"Commit hatası: {str(e)}")
            
    def rollback(self):
        """Değişiklikleri geri alır"""
        try:
            if self._connection:
                self._connection.rollback()
        except Exception as e:
            print(f"Rollback hatası: {str(e)}")
            
    def __del__(self):
        """Yıkıcı metod"""
        self.disconnect()

    def execute_query(self, query, params=None):
        """SQL sorgusunu çalıştır"""
        try:
            cursor = self.cursor()
            if not cursor:
                return None
                
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
            cursor = self.cursor()
            if not cursor:
                return False
                
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            self.commit()
            return True
            
        except Exception as e:
            print(f"Sorgu çalıştırma hatası: {str(e)}")
            self.rollback()
            return False 