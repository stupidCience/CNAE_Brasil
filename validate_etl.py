"""
Script de validação do processo ETL para bases CNAE
Verifica se os schemas estão sendo aplicados corretamente e se os dados estão íntegros
"""

import pandas as pd
import os
from pathlib import Path

# Importa os schemas
from Schemas.empSchema import EMPRESAS_SCHEMA
from Schemas.estabSchema import ESTABELECIMENTOS_SCHEMA  
from Schemas.sociosSchema import SOCIOS_SCHEMA

def validate_file_schema(file_path, expected_schema, file_type):
    """Valida se um arquivo CSV tem o schema esperado"""
    if not os.path.exists(file_path):
        print(f"❌ {file_type}: Arquivo {file_path} não encontrado")
        return False
    
    try:
        # Lê apenas o cabeçalho
        df_header = pd.read_csv(file_path, sep=';', nrows=0)
        actual_columns = list(df_header.columns)
        
        print(f"\n📋 {file_type}:")
        print(f"   Esperado ({len(expected_schema)} colunas): {expected_schema[:3]}...{expected_schema[-2:]}")
        print(f"   Atual    ({len(actual_columns)} colunas): {actual_columns[:3]}...{actual_columns[-2:]}")
        
        if set(actual_columns) == set(expected_schema):
            print(f"   ✅ Schema correto!")
            return True
        else:
            missing = set(expected_schema) - set(actual_columns)
            extra = set(actual_columns) - set(expected_schema)
            
            if missing:
                print(f"   ❌ Colunas faltando: {missing}")
            if extra:
                print(f"   ❌ Colunas extras: {extra}")
            return False
            
    except Exception as e:
        print(f"❌ {file_type}: Erro ao ler arquivo - {e}")
        return False

def validate_data_integrity(file_path, file_type):
    """Valida a integridade dos dados"""
    try:
        # Lê uma amostra dos dados
        sample = pd.read_csv(file_path, sep=';', nrows=1000)
        
        print(f"\n🔍 {file_type} - Integridade dos dados:")
        print(f"   Total de linhas na amostra: {len(sample)}")
        print(f"   Valores nulos por coluna: {sample.isnull().sum().sum()}")
        
        # Validações específicas por tipo
        if file_type == "Empresas":
            if 'cnpj_basico' in sample.columns:
                invalid_cnpj = sample[sample['cnpj_basico'] == '00000000']
                print(f"   CNPJs inválidos (00000000): {len(invalid_cnpj)}")
                
        elif file_type == "Estabelecimentos":
            if 'CNPJ' in sample.columns:
                print(f"   Formato CNPJ: {sample['CNPJ'].iloc[0] if len(sample) > 0 else 'N/A'}")
            if 'cnpj_basico' in sample.columns:
                invalid_cnpj = sample[sample['cnpj_basico'] == '00000000']
                print(f"   CNPJs básicos inválidos: {len(invalid_cnpj)}")
                
        print(f"   ✅ Dados parecem íntegros")
        return True
        
    except Exception as e:
        print(f"   ❌ Erro ao validar integridade: {e}")
        return False

def main():
    print("🚀 Iniciando validação do processo ETL CNAE...")
    
    # Caminhos dos arquivos finais
    base_path = Path(__file__).parent
    
    files_to_validate = [
        (base_path / "database" / "empresas_final.csv", EMPRESAS_SCHEMA, "Empresas"),
        (base_path / "database" / "estabelecimentos_final.csv", ESTABELECIMENTOS_SCHEMA, "Estabelecimentos"),
        (base_path / "database" / "socios_final.csv", SOCIOS_SCHEMA, "Sócios")
    ]
    
    results = []
    
    for file_path, schema, file_type in files_to_validate:
        schema_ok = validate_file_schema(str(file_path), schema, file_type)
        integrity_ok = validate_data_integrity(str(file_path), file_type) if schema_ok else False
        results.append((file_type, schema_ok, integrity_ok))
    
    # Resumo final
    print("\n" + "="*50)
    print("📊 RESUMO DA VALIDAÇÃO:")
    print("="*50)
    
    for file_type, schema_ok, integrity_ok in results:
        status = "✅ OK" if (schema_ok and integrity_ok) else "❌ FALHOU"
        print(f"{file_type:15} | Schema: {'✅' if schema_ok else '❌'} | Integridade: {'✅' if integrity_ok else '❌'} | {status}")
    
    all_ok = all(schema_ok and integrity_ok for _, schema_ok, integrity_ok in results)
    
    if all_ok:
        print("\n🎉 Processo ETL está funcionando corretamente!")
    else:
        print("\n⚠️  Processo ETL tem problemas que precisam ser corrigidos.")
    
    return all_ok

if __name__ == "__main__":
    main()
