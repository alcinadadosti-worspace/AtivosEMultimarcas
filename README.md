# Multimarks Analytics

Aplicacao web profissional para analise de vendas e revendedores do Grupo Boticario. Processa planilhas de vendas, cruza com banco de dados de produtos e gera metricas detalhadas sobre clientes ativos e multimarcas.

## Stack Tecnologica

### Backend
- **FastAPI** - Framework web async, leve e rapido
- **Polars** - Processamento de dados 10-100x mais rapido que Pandas
- **SQLite** - Banco de dados local, sem custo
- **Pydantic** - Validacao de dados

### Frontend
- **HTMX** - Interatividade sem JavaScript complexo
- **Alpine.js** - Reatividade leve para componentes
- **TailwindCSS** - Estilizacao utility-first
- **Jinja2** - Templates server-side
- **Lucide Icons** - Icones SVG minimalistas
- **Chart.js** - Graficos interativos

## Funcionalidades

- **Dashboard** - Visao geral com metricas e graficos
- **Multimarcas** - Clientes que compraram 2+ marcas
- **Produtos Novos** - SKUs nao cadastrados (possiveis lancamentos)
- **Auditoria** - Problemas de matching de SKUs
- **Cliente** - Detalhe individual de compras
- **IAF** - Penetracao de produtos de premiacao

## Instalacao

### Requisitos
- Python 3.11+
- pip

### Setup Local

```bash
# Clonar repositorio
git clone https://github.com/alcinadadosti-worspace/AtivosEMultimarcas.git
cd AtivosEMultimarcas

# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate  # Windows

# Instalar dependencias
pip install -r requirements.txt

# Importar dados para SQLite (opcional)
# Coloque o arquivo estoqueplanilha.xlsx em data/
python import_db.py

# Rodar aplicacao
python main.py
```

Acesse http://localhost:8000

## Estrutura do Projeto

```
multimarks-analytics/
├── main.py                 # FastAPI app entry point
├── requirements.txt        # Dependencias Python
├── render.yaml             # Configuracao Render
├── import_db.py            # Script importacao Excel → SQLite
├── app/
│   ├── config.py           # Constantes e configuracoes
│   ├── database.py         # Conexao SQLite
│   ├── services/           # Logica de negocio
│   │   ├── produto.py      # Busca de produtos
│   │   ├── venda.py        # Processamento de vendas
│   │   ├── metricas.py     # Calculo de metricas
│   │   ├── iaf.py          # Logica IAF
│   │   └── auditoria.py    # Auditoria de SKUs
│   ├── api/
│   │   ├── routes.py       # Endpoints FastAPI
│   │   ├── schemas.py      # Pydantic models
│   │   └── dependencies.py # Dependencias
│   └── utils/
│       ├── normalizers.py  # Normalizacao SKU/marca
│       ├── formatters.py   # Formatacao de valores
│       └── exporters.py    # Exportacao CSV/Excel
├── templates/              # Templates Jinja2
├── static/                 # CSS/JS
├── data/                   # Arquivos de dados
└── tests/                  # Testes
```

## Uso

1. Acesse o Dashboard
2. Faca upload de uma planilha de vendas (CSV ou Excel)
3. A planilha deve conter as colunas:
   - Setor
   - NomeRevendedora
   - CodigoRevendedora
   - CicloFaturamento
   - CodigoProduto
   - NomeProduto
   - Tipo
   - QuantidadeItens
   - ValorPraticado

4. Navegue pelas diferentes paginas para visualizar metricas

## Deploy no Render

1. Conecte seu repositorio ao Render
2. O arquivo `render.yaml` ja contem a configuracao necessaria
3. O deploy sera automatico a cada push

## Testes

```bash
pytest tests/ -v
```

## Licenca

Projeto privado - Uso interno apenas.
