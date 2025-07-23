"""
Conexões e operações de banco de dados
"""

import mysql.connector
from mysql.connector import Error
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from config.config_db import DB_CONFIG

class DatabaseConnection:
    def __init__(self):
        self.connection = None
    
    def connect(self):
        """Estabelece conexão com MySQL"""
        try:
            self.connection = mysql.connector.connect(**DB_CONFIG)
            if self.connection.is_connected():
                cursor = self.connection.cursor()
                cursor.execute("SET SESSION wait_timeout=28800")
                cursor.execute("SET SESSION interactive_timeout=28800")
                cursor.execute("SET SESSION innodb_lock_wait_timeout=300")
                cursor.execute("SET SESSION sql_mode=''")
                cursor.close()
                return True
        except Error as e:
            print(f"❌ Erro ao conectar: {e}")
            return False
    
    def disconnect(self):
        """Fecha conexão"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
    
    def execute_query(self, query, params=None):
        """Executa query de consulta"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return None
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or ())
            
            # Para DDL statements (CREATE, ALTER, DROP), não há resultado para buscar
            if query.strip().upper().startswith(('CREATE', 'ALTER', 'DROP', 'INSERT', 'UPDATE', 'DELETE')):
                self.connection.commit()
                cursor.close()
                return True
            else:
                # Para SELECT statements
                result = cursor.fetchall()
                cursor.close()
                return result
        except Error as e:
            print(f"❌ Erro na query: {e}")
            return None
    
    def execute_insert(self, query, data_list):
        """Executa inserção em lote"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False
        
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, data_list)
            self.connection.commit()
            cursor.close()
            return True
        except Error as e:
            print(f"❌ Erro na inserção: {e}")
            try:
                self.connection.rollback()
            except:
                pass
            return False
