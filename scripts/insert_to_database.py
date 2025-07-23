"""
Script para inserção de dados no banco MySQL
Uso: python scripts/insert_to_database.py
"""

import sys
import os
from pathlib import Path
import duckdb as db
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.database.connection import DatabaseConnection
from config.config_db import DB_CONFIG

def criar_tabelas(db_conn):
    """Cria tabelas no banco de dados"""
    
    # Tabela de empresas
    create_empresas = """
    CREATE TABLE IF NOT EXISTS empresas_qualificacoes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        cnpj_basico VARCHAR(15) NOT NULL,
        razao_social VARCHAR(200),
        natureza_juridica VARCHAR(80),
        qualificacao_responsavel VARCHAR(100),
        capital_social DECIMAL(15,2),
        porte_empresa VARCHAR(50),
        data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_cnpj_basico (cnpj_basico),
        INDEX idx_porte (porte_empresa)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    
    # Tabela de estabelecimentos
    create_estabelecimentos = """
    CREATE TABLE IF NOT EXISTS estabelecimentos (
        id INT AUTO_INCREMENT PRIMARY KEY,
        cnpj_completo VARCHAR(14) NOT NULL,
        cnpj_basico VARCHAR(8) NOT NULL,
        nome_fantasia VARCHAR(200),
        situacao_cadastral VARCHAR(2),
        data_situacao_cadastral DATE,
        cnae_fiscal_principal VARCHAR(7),
        logradouro VARCHAR(200),
        numero VARCHAR(10),
        bairro VARCHAR(100),
        cep VARCHAR(8),
        uf VARCHAR(2),
        municipio VARCHAR(10),
        email VARCHAR(200),
        data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_cnpj_completo (cnpj_completo),
        INDEX idx_cnpj_basico (cnpj_basico),
        INDEX idx_municipio (municipio),
        INDEX idx_uf (uf)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """
    
    queries = [create_empresas, create_estabelecimentos]
    
    for query in queries:
        result = db_conn.execute_query(query)
        if result is not None:
            print("✅ Tabela criada com sucesso")
        else:
            print("❌ Erro ao criar tabela")
            return False
    
    return True

def inserir_dados_empresas(db_conn, chunk_size=50000):
    """Insere dados de empresas no banco"""
    print("📊 Inserindo dados de empresas...")
    
    parquet_file = Path("database/empresas_final.parquet")
    if not parquet_file.exists():
        print("❌ Arquivo empresas_final.parquet não encontrado")
        return False
    
    # Lê dados com DuckDB
    con = db.connect()
    query = f"""
        SELECT 
            cnpj_basico,
            razao_social,
            natureza_juridica,
            qualificacao_responsavel,
            CAST(REPLACE(capital_social, ',', '.') AS DOUBLE) as capital_social,
            porte_empresa
        FROM '{parquet_file}'
        LIMIT {chunk_size}
    """
    
    df = con.execute(query).fetchdf()
    con.close()
    
    if df.empty:
        print("❌ Nenhum dado encontrado")
        return False
    
    # Prepara dados para inserção
    insert_query = """
        INSERT INTO empresas_qualificacoes 
        (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    dados = []
    for _, row in df.iterrows():
        dados.append((
            row['cnpj_basico'],
            row['razao_social'],
            row['natureza_juridica'],
            row['qualificacao_responsavel'],
            row['capital_social'] if pd.notna(row['capital_social']) else None,
            row['porte_empresa']
        ))
    
    success = db_conn.execute_insert(insert_query, dados)
    if success:
        print(f"✅ {len(dados):,} registros de empresas inseridos")
    else:
        print("❌ Erro ao inserir dados de empresas")
    
    return success

def main():
    """Função principal"""
    print("🚀 Iniciando inserção de dados no banco MySQL...")
    
    db_conn = DatabaseConnection()
    
    if not db_conn.connect():
        print("❌ Não foi possível conectar ao banco")
        return
    
    try:
        print("📋 Criando tabelas...")
        if not criar_tabelas(db_conn):
            return
        
        print("📊 Inserindo dados...")
        inserir_dados_empresas(db_conn)
        
        print("✅ Processo concluído!")
        
    finally:
        db_conn.disconnect()

if __name__ == "__main__":
    main()
