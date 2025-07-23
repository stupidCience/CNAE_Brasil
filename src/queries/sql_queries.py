"""
Queries SQL para análise de dados CNAE
Para usar com banco de dados MySQL configurado
"""

import sys
import os
import mysql.connector
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from config.config_db import DB_CONFIG

def conectar_mysql():
    """Conecta ao banco MySQL"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        return None

def executar_query(query, params=None):
    """Executa uma query no banco"""
    connection = conectar_mysql()
    if not connection:
        return None
    
    try:
        cursor = connection.cursor()
        cursor.execute(query, params or ())
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"❌ Erro na query: {e}")
        return None
    finally:
        if connection.is_connected():
            connection.close()

# Queries de exemplo
QUERY_EMPRESAS_POR_PORTE = """
SELECT porte_empresa, COUNT(*) as total
FROM empresas_qualificacoes 
GROUP BY porte_empresa 
ORDER BY total DESC
"""

QUERY_TOP_QUALIFICACOES = """
SELECT qualificacao_responsavel, COUNT(*) as total
FROM empresas_qualificacoes 
WHERE qualificacao_responsavel IS NOT NULL
GROUP BY qualificacao_responsavel 
ORDER BY total DESC 
LIMIT 10
"""
