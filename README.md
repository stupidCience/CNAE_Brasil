# 🏢 CNAE Brasil - Sistema ETL

Sistema completo de ETL (Extract, Transform, Load) para processamento de dados da Receita Federal do Brasil com otimizações de performance e integração com banco de dados.

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 📊 Performance

- **Compressão**: 69.3% (25.3GB → 7.8GB)
- **Speedup**: 253x mais rápido com Parquet
- **Registros**: 63M+ empresas, 66M+ estabelecimentos, 25M+ sócios

## 📁 Estrutura do Projeto

```
CNAE_Brasil/
├── 📂 src/                      # Código fonte
│   ├── 📂 services/             # Serviços de download
│   │   ├── getEmpresas.py       # Download empresas
│   │   ├── getEstabelecimentos.py # Download estabelecimentos
│   │   └── getSocios.py         # Download sócios
│   ├── 📂 processors/           # Processamento de dados
│   │   ├── empresasConstructor.py
│   │   ├── estabelecimentoConstructor.py
│   │   └── sociosConstructor.py
│   ├── 📂 schemas/              # Esquemas de dados
│   │   ├── empSchema.py
│   │   ├── estabSchema.py
│   │   └── sociosSchema.py
│   └── 📂 database/             # Conexões DB
│       └── connection.py
├── 📂 config/                   # Configurações
│   ├── config_db.py            # Config banco
│   └── config_db_local.py.example # Template local
├── 📂 scripts/                  # Utilitários
│   ├── validate_etl.py          # Validação
│   ├── check_dependencies.py    # Dependências
│   ├── analyze_data.py          # Análise
│   └── insert_to_database.py    # Inserção DB
├── 📂 Auxiliar/                 # Dados auxiliares
├── 📂 database/                 # Dados processados
├── 📂 Data/                     # Dados temporários
├── main_etl.py                  # 🚀 Script principal
└── optimize_data.py             # Otimizações
```

## 🚀 Início Rápido

### 1. Instalação

```bash
git clone https://github.com/seu-usuario/cnae-brasil.git
cd cnae-brasil
pip install -r requirements.txt
```

### 2. Configuração

```bash
# Configurar banco de dados (opcional)
cp config/config_db_local.py.example config/config_db_local.py
# Edite config/config_db_local.py com suas credenciais
```

### 3. Execução

```bash
# ETL completo (download + processamento + otimização)
python main_etl.py

# Apenas download
python main_etl.py --mode download

# Apenas processamento
python main_etl.py --mode process
```

## ⚙️ Configuração do Banco de Dados

Para usar com MySQL, edite `config/config_db_local.py`:

```python
DB_CONFIG_LOCAL = {
    'host': 'localhost',
    'user': 'seu_usuario',
    'password': 'sua_senha',
    'database': 'cnae_brasil',
    'port': 3306
}
```

## 🔧 Scripts Utilitários

```bash
# Validar dados processados
python scripts/validate_etl.py

# Verificar dependências
python scripts/check_dependencies.py

# Analisar dados
python scripts/analyze_data.py

# Inserir no banco
python scripts/insert_to_database.py
```

## 📈 Funcionalidades

- ✅ **Download automático** dos dados oficiais
- ✅ **Processamento por chunks** para grandes volumes
- ✅ **Enriquecimento** com tabelas auxiliares
- ✅ **Otimização Parquet** com compressão
- ✅ **Integração MySQL** com inserção em lotes
- ✅ **Validação de dados** automatizada
- ✅ **Tratamento de encoding** (ISO-8859-1 → UTF-8)

## 🛠️ Tecnologias

- **Python 3.11+**
- **Pandas** - Manipulação de dados
- **DuckDB** - Engine de consultas
- **MySQL Connector** - Conectividade
- **Parquet** - Formato otimizado

## 📊 Dados Processados

| Tipo | Registros | Tamanho Original | Tamanho Parquet |
|------|-----------|------------------|-----------------|
| Empresas | 63M+ | 8.5GB | 2.6GB |
| Estabelecimentos | 66M+ | 12.8GB | 3.9GB |
| Sócios | 25M+ | 4.0GB | 1.3GB |
| **Total** | **154M+** | **25.3GB** | **7.8GB** |

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch (`git checkout -b feature/nova-funcionalidade`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova funcionalidade'`)
4. Push para a branch (`git push origin feature/nova-funcionalidade`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para detalhes.

## 🔗 Links Úteis

- [Dados da Receita Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)
- [Documentação CNAE](https://concla.ibge.gov.br/classificacoes/por-tema/atividades-economicas/cnae)

---

⭐ **Se este projeto foi útil, considere dar uma estrela!** ⭐