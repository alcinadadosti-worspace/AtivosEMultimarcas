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

# Persistent directory — survives deploys via Render Disk.
# Mount the Render Disk at /opt/render/project/src/persistent (not inside data/
# to avoid hiding git-tracked static files like estoqueplanilha.xlsx).
PERSISTENT_DIR = Path(os.getenv("PERSISTENT_DIR", str(BASE_DIR / "persistent")))
PERSISTENT_DIR.mkdir(parents=True, exist_ok=True)

DATABASE_PATH = os.getenv("DATABASE_PATH", str(PERSISTENT_DIR / "produtos.db"))

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
# SLACK
# =============================================================================
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")

# Maps supervisora name (uppercase) → Slack user ID.
# Set via SLACK_USER_MAP env var as JSON, e.g.:
#   {"KARINE": "U0895CZ8HU7", "LAÍS": "U0123456789"}
# Falls back to SLACK_DEFAULT_USER_ID for any unmapped supervisora.
import json as _json
SLACK_DEFAULT_USER_ID = os.getenv("SLACK_DEFAULT_USER_ID", "U0895CZ8HU7")

# Mapa-padrão embutido para supervisoras que devem funcionar mesmo sem a env
# var (ex.: ambiente local). O SLACK_USER_MAP do ambiente (Render) é mesclado
# POR CIMA e sobrescreve estas entradas.
_DEFAULT_SLACK_USER_MAP = {
    "GESSICA": "U09G04R3CNP",
    "ANALUIZA": "U08ERHMN6F9",   # Platina / Penedo (ciclo 10)
    "RODRIGO": "U0922F5KB7U",    # Bronze 4 / Penedo (ciclo 10)
}

_raw_map = os.getenv("SLACK_USER_MAP", "{}")
try:
    _env_map = {k.upper(): v for k, v in _json.loads(_raw_map).items()}
except Exception:
    _env_map = {}

SLACK_USER_MAP: dict = {
    **{k.upper(): v for k, v in _DEFAULT_SLACK_USER_MAP.items()},
    **_env_map,
}

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

# Persistent geo data (survives server restarts via Render Disk)
GEO_PARQUET_PATH = str(PERSISTENT_DIR / "clientes_geo.parquet")
GEO_STATS_PATH   = str(PERSISTENT_DIR / "clientes_geo_stats.json")

# =============================================================================
# ORDERS SPREADSHEET COLUMNS (ConsultaPedidos — "Mapa de Pedidos")
# =============================================================================
# Importada on-demand na sessão; NÃO é persistida no disco do Render.
PED_COL_PESSOA = "Pessoa"                         # id do revendedor
PED_COL_NOME = "NomePessoa"                        # nome do revendedor
PED_COL_PAPEL = "Papel"                            # segmentação (Bronze/Prata/Ouro/...)
PED_COL_QTDE_MATERIAIS = "QtdeMateriais"           # itens vendidos
PED_COL_VALOR = "ValorPraticado"                   # valor praticado do pedido
PED_COL_TIPO_ENTREGA = "Tipo de Entrega"           # Retirar na central / No endereço de entrega
PED_COL_CICLO = "Ciclo Marketing"                  # "10/2026" -> ciclo 10
PED_COL_ESTRUTURA_PAI = "EstruturaPai"             # 13707=Penedo / 13706=Palmeira
PED_COL_COD_ESTRUTURA = "Cód Estrutura"            # setor que atendeu
PED_COL_ESTRUTURA = "Estrutura"                    # nome do setor
PED_COL_TELEFONE = "Telefone"
# Cadastro (onde o revendedor é cadastrado)
PED_COL_LOGRADOURO = "Logradouro"
PED_COL_BAIRRO = "Bairro"
PED_COL_CIDADE = "Cidade"
# Entrega / retirada
PED_COL_LOGRADOURO_ENTREGA = "LogradouroEntrega"
PED_COL_BAIRRO_ENTREGA = "BairroEntregaRetirada"
PED_COL_CIDADE_ENTREGA = "CidadeEntregaRetirada"

PED_TIPO_RETIRADA = "Retirar na central de serviços"   # veio à unidade
PED_TIPO_ENTREGA_CASA = "No endereço de entrega"       # recebeu em casa

PED_REQUIRED_COLUMNS = [
    PED_COL_PESSOA,
    PED_COL_PAPEL,
    PED_COL_QTDE_MATERIAIS,
    PED_COL_VALOR,
    PED_COL_TIPO_ENTREGA,
    PED_COL_CIDADE_ENTREGA,
]

# EstruturaPai prefixo -> unidade gerenciadora
PED_UNIDADES = {
    "13707": "Penedo",
    "13706": "Palmeira dos Índios",
}

# =============================================================================
# RESELLER BASE (ConsultaRevendedores) — base permanente de cadastro
# =============================================================================
# 2 abas: 13707 (Penedo) e 13706 (Palmeira). Persistida no disco do Render.
REV_SHEETS = {"13707": "Penedo", "13706": "Palmeira dos Índios"}
REV_COL_CODIGO = "CodigoRevendedor"          # chave de cruzamento (~ Pessoa nos pedidos)
REV_COL_NOME = "Nome"
REV_COL_SITUACAO = "Situacao"                # Ativo / Inativo
REV_COL_CICLOS_INATIVIDADE = "CiclosInatividade"
REV_COL_PAPEL = "Papel"
REV_COL_COD_SETOR = "CodigoEstruturaComercial"
REV_COL_SETOR = "EstruturaComercial"
REV_COL_CICLO_PRIMEIRO = "CicloPrimeiroPedido"
REV_COL_CICLO_REATIVACAO = "CicloReativacao"
REV_COL_CICLO_CESSAMENTO = "CicloCessamento"
REV_COL_MOTIVO_CESSAMENTO = "MotivoCessamento"
REV_COL_TELEFONE = "TelCelular"
REV_COL_CIDADE = "CidadeResidencial"

REV_REQUIRED_COLUMNS = [REV_COL_CODIGO, REV_COL_SITUACAO]

# Persistente (sobrevive a restart/deploy via Render Disk)
REV_PARQUET_PATH = str(PERSISTENT_DIR / "revendedores_base.parquet")
REV_STATS_PATH   = str(PERSISTENT_DIR / "revendedores_base_stats.json")
