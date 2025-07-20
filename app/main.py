# main.py corrigido

import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import Services.getEmpresas
import Services.getEstabelecimentos
import Services.getSocios
import process.empresasConstructor
import process.estabelecimentoConstructor
import process.sociosConstructor

def run_all_processes():
    
    Services.getEstabelecimentos.getEstab()
    process.estabelecimentoConstructor.estabelecimentoConstructor()
    
    # 1. Baixar e preparar dados de Empresas
    Services.getEmpresas.getEmp()
    process.empresasConstructor.empresasConstructor()

    # 2. Baixar e preparar dados de Estabelecimentos
   

    # 3. Baixar e preparar dados de Sócios
    Services.getSocios.getSocios()
    process.sociosConstructor.sociosConstructor()

if __name__ == "__main__":
    run_all_processes()