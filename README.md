# CNAE Brasil - ETL Pipeline 🇧🇷

Pipeline completo para processamento de dados do Cadastro Nacional de Atividades Econômicas (CNAE) do governo brasileiro. Inclui download, processamento, validação e otimização de dados de empresas, estabelecimentos e sócios.

## 🚀 Funcionalidades Principais

- **Download automático** dos arquivos oficiais da Receita Federal
- **Processamento eficiente** com chunks para arquivos de grande volume (25GB+)
- **Validação automatizada** da integridade e consistência dos dados
- **Otimização Parquet** com compressão de 69% e queries 250x mais rápidas
- **Análises corporativas** avançadas (holdings, grupos empresariais)

## 📦 Instalação e Configuração

### 1. Verificar Dependências

```bash
# Verificar dependências instaladas
python check_dependencies.py

# Instalar dependências principais
pip install -r requirements.txt

# Instalar dependências de desenvolvimento (opcional)
pip install -r requirements-dev.txt
```

### 2. Dependências Principais

| Biblioteca | Versão | Finalidade |
|------------|--------|------------|
| **pandas** | ≥2.2.0 | Manipulação de DataFrames |
| **duckdb** | ≥0.10.0 | Análise SQL e conversão Parquet |
| **pyarrow** | ≥15.0.0 | Suporte Parquet otimizado |
| **requests** | ≥2.31.0 | Download de arquivos |
| **tqdm** | ≥4.66.0 | Barras de progresso |
| **chardet** | ≥5.2.0 | Detecção de encoding |
| **python-dateutil** | ≥2.8.2 | Manipulação de datas |

## Estrutura do Projeto

```
CNAE_Brasil/
├── main_etl.py                 # Script principal para execução completa do ETL
├── validate_etl.py             # Script de validação dos dados processados
├── app/
│   └── main.py                 # Script principal: orquestra downloads e processamento
├── Services/
│   ├── getEmpresas.py          # Download e extração de empresas
│   ├── getEstabelecimentos.py  # Download e extração de estabelecimentos
│   └── getSocios.py            # Download e extração de sócios
├── process/
│   ├── empresasConstructor.py  # Processamento e enriquecimento de empresas
│   ├── estabelecimentoConstructor.py # Processamento de estabelecimentos
│   └── sociosConstructor.py    # Processamento de sócios
├── Data/                       # Arquivos CSV brutos baixados
├── Auxiliar/                   # Tabelas auxiliares (natureza, qualificações, países, etc)
├── database/                   # Arquivos CSV finais processados
├── Schemas/                    # Schemas de colunas para leitura dos arquivos
├── Tables/                     # Consultas e análises com DuckDB/Pandas
├── .gitignore
└── README.md
```

## Como usar

### 1. **Instale as dependências**  
   Requer Python 3.11+ e dependências listadas em requirements.txt:
   ```bash
   pip install -r requirements.txt
   ```

### 2. **Execute o ETL completo**
   ```bash
   python main_etl.py
   ```
   
   Ou com opções específicas:
   ```bash
   python main_etl.py --mode download    # Apenas download
   python main_etl.py --mode process     # Apenas processamento
   python main_etl.py --mode full        # Processo completo (padrão)
   ```

### 3. **Valide os dados processados**
   ```bash
   python validate_etl.py
   ```

### 4. **Consultas e análises**
   - Utilize os scripts em `Tables/` para realizar análises SQL diretamente nos CSVs usando DuckDB.
   - Exemplos:
     - `holdings.py`: Consulta empresas com sócios pessoa jurídica.
     - `estabs.py`: Consulta filiais e matrizes.

## Correções Implementadas

### ✅ Problemas de Schema
- **Antes:** Dupla atribuição de nomes de colunas (names= + columns=)
- **Agora:** Aplicação consistente dos schemas oficiais em todos os arquivos

### ✅ Inconsistência de Caminhos
- **Antes:** Caminhos relativos inconsistentes causando falhas
- **Agora:** Caminhos absolutos baseados na localização dos scripts

### ✅ Tratamento de Tipos
- **Antes:** Merges falhavam por tipos incompatíveis (int vs string)  
- **Agora:** Padronização de tipos antes de todos os merges

### ✅ Validação de Dados
- **Antes:** Sem validação de integridade
- **Agora:** Script automatizado de validação com relatórios detalhados

### ✅ Fluxo ETL
- **Antes:** Scripts executados manualmente sem coordenação
- **Agora:** Orquestração automática com logging e tratamento de erros

## Observações

- Os arquivos de dados são grandes (vários GB). O processamento é feito em partes para evitar sobrecarga de memória.
- As funções de processamento só são executadas após o download dos arquivos.
- O projeto pode ser facilmente adaptado para novas tabelas auxiliares ou regras de enriquecimento.
- Para consultas SQL eficientes em arquivos grandes, utilize DuckDB (ver exemplos em `Tables/`).
- Para acompanhar o progresso de downloads, barras de progresso são exibidas no terminal.

## Execução por Etapas

Se preferir executar etapa por etapa:

1. **Download dos dados:**
   ```python
   from Services.getEmpresas import getEmp
   from Services.getEstabelecimentos import getEstab
   from Services.getSocios import getSocios
   
   getEmp()
   getEstab()
   getSocios()
   ```

2. **Processamento dos dados:**
   ```python
   from process.empresasConstructor import empresasConstructor
   from process.estabelecimentoConstructor import estabelecimentoConstructor
   from process.sociosConstructor import sociosConstructor
   
   empresasConstructor()
   estabelecimentoConstructor()
   sociosConstructor()
   ```

## Licença

Este projeto é de uso livre para fins acadêmicos e profissionais.  
Os dados utilizados são públicos e provenientes de órgãos oficiais do governo brasileiro.

---
Desenvolvido por [João Vitor Andrade de Melo] - Versão 2.0 com correções de ETL