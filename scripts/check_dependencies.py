"""
Script para verificar dependências necessárias
"""

import sys
import subprocess
import pkg_resources

def verificar_dependencias():
    """Verifica se todas as dependências estão instaladas"""
    
    with open('requirements.txt', 'r') as f:
        required_packages = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    print("🔍 Verificando dependências...")
    
    missing_packages = []
    
    for package in required_packages:
        try:
            pkg_resources.require(package)
            print(f"✅ {package}")
        except pkg_resources.DistributionNotFound:
            print(f"❌ {package} - NÃO INSTALADO")
            missing_packages.append(package)
        except pkg_resources.VersionConflict as e:
            print(f"⚠️ {package} - CONFLITO DE VERSÃO: {e}")
    
    if missing_packages:
        print(f"\n📥 Para instalar os pacotes em falta, execute:")
        print(f"pip install {' '.join(missing_packages)}")
    else:
        print("\n✅ Todas as dependências estão instaladas!")

if __name__ == "__main__":
    verificar_dependencias()
