# CNAE Data Processing

Este projeto automatiza o download, extração, processamento e enriquecimento de grandes bases de dados do Cadastro Nacional de Atividades Econômicas (CNAE), incluindo empresas, estabelecimentos e sócios.

## Funcionalidades

- **Download automático** dos arquivos oficiais do governo (empresas, estabelecimentos, sócios).
- **Extração e limpeza** de arquivos compactados (.zip).
- **Processamento eficiente** de arquivos CSV grandes (suporte a leitura por chunks).
- **Enriquecimento dos dados** com tabelas auxiliares (natureza jurídica, porte, qualificações, países, CNAEs, etc).
- **Geração de arquivos finais** prontos para análise ou integração.

## Estrutura do Projeto

```
CNAE_Brasil/
├── app/
│   └── main.py                  # Script principal: orquestra downloads e processamento
├── Services/
│   ├── getEmpresas.py           # Download e extração de empresas
│   ├── getEstabelecimentos.py   # Download e extração de estabelecimentos
│   └── getSocios.py             # Download e extração de sócios
├── process/
│   ├── empresasConstructor.py   # Processamento e enriquecimento de empresas
│   ├── estabelecimentoConstructor.py # Processamento de estabelecimentos
│   └── sociosConstructor.py     # Processamento de sócios
├── Data/                        # Arquivos CSV brutos baixados
├── Auxiliar/                    # Tabelas auxiliares (natureza, qualificações, países, etc)
├── database/                    # Arquivos CSV finais processados
├── Schemas/                     # Schemas de colunas para leitura dos arquivos
├── Tables/                      # Consultas e análises com DuckDB/Pandas
├── .gitignore
└── README.md
```

## Como usar

1. **Instale as dependências**  
   Requer Python 3.11+ e pandas:
   ```bash
   pip install pandas duckdb tqdm
   ```

2. **Execute o script principal**
   ```bash
   python app/main.py
   ```

   O script irá:
   - Baixar e extrair os arquivos necessários.
   - Processar e enriquecer os dados.
   - Gerar os arquivos finais em `/database`.

3. **Consultas e análises**
   - Utilize os scripts em `Tables/` para realizar análises SQL diretamente nos CSVs usando DuckDB.
   - Exemplos:
     - `holdings.py`: Consulta empresas com sócios pessoa jurídica.
     - `estabs.py`: Consulta filiais e matrizes.

4. **Configuração do .gitignore**  
   O projeto já ignora todos os arquivos `.csv` das pastas de dados e resultados.

## Observações

- Os arquivos de dados são grandes (vários GB). O processamento é feito em partes para evitar sobrecarga de memória.
- As funções de processamento só são executadas após o download dos arquivos.
- O projeto pode ser facilmente adaptado para novas tabelas auxiliares ou regras de enriquecimento.
- Para consultas SQL eficientes em arquivos grandes, utilize DuckDB (ver exemplos em `Tables/`).
- Para acompanhar o progresso de downloads, barras de progresso são exibidas no terminal.

## Licença

Este projeto é de uso livre para fins acadêmicos e profissionais.  
Os dados utilizados são públicos e provenientes de órgãos oficiais do governo brasileiro.

---
Desenvolvido por [João Vitor Andrade de Melo]