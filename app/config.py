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
