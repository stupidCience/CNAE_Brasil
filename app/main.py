import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import Services.getEmpresas
import Services.getEstabelecimentos
import Services.getSocios
import process.empresasConstructor
import process.estabelecimentoConstructor
import process.sociosConstructor

def run_services():
    Services.getSocios.getSocios()
    Services.getEmpresas.getEmp()
    Services.getEstabelecimentos.getEstab()

def run_processing():
    process.empresasConstructor.empresasConstructor()
    process.estabelecimentoConstructor.estabelecimentoConstructor()
    process.sociosConstructor.sociosConstructor()

if __name__ == "__main__":
    run_services()
    run_processing()