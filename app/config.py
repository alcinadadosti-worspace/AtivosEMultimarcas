"""
Configuration constants for Multimarks Analytics.
"""
import os
from pathlib import Path

# =============================================================================
# PATHS
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATABASE_PATH = os.getenv("DATABASE_PATH", str(DATA_DIR / "produtos.db"))

# =============================================================================
# SALES SPREADSHEET COLUMNS
# =============================================================================
VENDAS_COL_SETOR = "Setor"
VENDAS_COL_NOME_REVENDEDORA = "NomeRevendedora"
VENDAS_COL_CODIGO_REVENDEDORA = "CodigoRevendedora"
VENDAS_COL_CICLO = "CicloFaturamento"
VENDAS_COL_CODIGO_PRODUTO = "CodigoProduto"
VENDAS_COL_NOME_PRODUTO = "NomeProduto"
VENDAS_COL_TIPO = "Tipo"
VENDAS_COL_QTD_ITENS = "QuantidadeItens"
VENDAS_COL_VALOR = "ValorPraticado"

VENDAS_REQUIRED_COLUMNS = [
    VENDAS_COL_SETOR,
    VENDAS_COL_NOME_REVENDEDORA,
    VENDAS_COL_CODIGO_REVENDEDORA,
    VENDAS_COL_CICLO,
    VENDAS_COL_CODIGO_PRODUTO,
    VENDAS_COL_NOME_PRODUTO,
    VENDAS_COL_TIPO,
    VENDAS_COL_QTD_ITENS,
    VENDAS_COL_VALOR,
]

# Optional columns
VENDAS_COL_MEIO_CAPTACAO = "MeioCaptacao"
VENDAS_COL_GERENCIA = "Gerencia"

VENDAS_OPTIONAL_COLUMNS = [VENDAS_COL_MEIO_CAPTACAO, VENDAS_COL_GERENCIA]

# Special values
TIPO_VENDA = "Venda"

# =============================================================================
# SKU MATCHING CONSTANTS
# =============================================================================
MOTIVO_MATCH_EXATO = "MATCH_EXATO"
MOTIVO_MATCH_COM_ZERO = "MATCH_COM_ZERO"
MOTIVO_MATCH_SEM_ZERO = "MATCH_SEM_ZERO"
MOTIVO_NAO_ENCONTRADO = "NAO_ENCONTRADO"

# =============================================================================
# BRAND NORMALIZATION
# =============================================================================
MARCA_ALIASES = {
    # O Boticario (various spellings)
    "OBOTICÁRIO": "oBoticário",
    "OBOTICARIO": "oBoticário",
    "O BOTICÁRIO": "oBoticário",
    "O BOTICARIO": "oBoticário",
    "BOTICÁRIO": "oBoticário",
    "BOTICARIO": "oBoticário",
    "BOT": "oBoticário",

    # Eudora
    "EUD": "Eudora",
    "EUDORA": "Eudora",

    # Quem Disse Berenice
    "QDB": "Quem Disse Berenice",
    "QUEM DISSE BERENICE": "Quem Disse Berenice",
    "QUEM DISSE, BERENICE?": "Quem Disse Berenice",

    # O.U.I
    "OUI": "O.U.I",
    "O.U.I": "O.U.I",
    "O.U.I.": "O.U.I",

    # AuAmigos
    "AUMIGOS": "AuAmigos",
    "AU MIGOS": "AuAmigos",
    "AU AMIGOS": "AuAmigos",
}

MARCAS_GRUPO = ["oBoticário", "Eudora", "Quem Disse Berenice", "O.U.I", "AuAmigos"]

MARCA_DESCONHECIDA = "DESCONHECIDA"

# =============================================================================
# GEOGRAPHIC / CLIENTS SPREADSHEET COLUMNS
# =============================================================================
GEO_COL_NOME = "Nome"
GEO_COL_CPF = "CPF/CNPJ"
GEO_COL_SITUACAO = "Situacao"
GEO_COL_CICLOS_INATIVIDADE = "CiclosInatividade"
GEO_COL_PAPEL = "Papel"
GEO_COL_COD_ESTRUTURA = "CodigoEstruturaComercial"
GEO_COL_ESTRUTURA = "EstruturaComercial"
GEO_COL_COD_ESTRUTURA_PAI = "CodigoEstruturaComercialPai"
GEO_COL_TELEFONE = "TelCelular"
GEO_COL_RUA_RESID = "RuaResidencial"
GEO_COL_CEP_RESID = "CEPResidencial"
GEO_COL_BAIRRO_RESID = "BairroResidencial"
GEO_COL_CIDADE_RESID = "CidadeResidencial"
GEO_COL_RUA_ENTREGA = "RuaEntrega"
GEO_COL_BAIRRO_ENTREGA = "BairroEntrega"
GEO_COL_CIDADE_ENTREGA = "CidadeEntrega"
GEO_COL_ESTADO_ENTREGA = "EstadoEntrega"

GEO_REQUIRED_COLUMNS = [
    GEO_COL_NOME,
    GEO_COL_SITUACAO,
    GEO_COL_BAIRRO_RESID,
    GEO_COL_CIDADE_RESID,
]

GEO_UNIDADE_MATRIZ = "1048"   # Unidade Matriz Penedo
GEO_UNIDADE_FILIAL = "1515"   # Filial Palmeira dos Índios

# Persistent geo data (survives server restarts)
GEO_PARQUET_PATH = str(DATA_DIR / "clientes_geo.parquet")
GEO_STATS_PATH   = str(DATA_DIR / "clientes_geo_stats.json")
