"""
Pydantic schemas for API request/response validation.
"""
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class UploadResponse(BaseModel):
    """Response after processing uploaded sales file."""
    success: bool
    message: str
    estatisticas: Optional[Dict[str, Any]] = None
    avisos: Optional[List[str]] = None


class MetricasGerais(BaseModel):
    """General dashboard metrics."""
    total_ativos: int
    total_multimarcas: int
    percent_multimarcas: int
    total_itens: int
    total_valor: float


class ClienteMetrica(BaseModel):
    """Customer-level metrics."""
    ciclo: str
    cliente_id: str
    setor: str
    codigo_revendedora: str
    nome_revendedora: str
    marcas_compradas: str
    qtde_marcas: int
    is_multimarcas: bool
    itens_total: float
    valor_total: float


class SetorMetrica(BaseModel):
    """Sector-level metrics."""
    ciclo: str
    setor: str
    clientes_ativos: int
    clientes_multimarcas: int
    percent_multimarcas: float
    itens_total: float
    valor_total: float


class VendaPorMarca(BaseModel):
    """Sales by brand."""
    marca: str
    itens: int
    valor: float
    vendas: int


class TopSetor(BaseModel):
    """Top sector by value."""
    setor: str
    clientes: int
    multimarcas: int
    valor: float


class EvolucaoCiclo(BaseModel):
    """Cycle evolution metrics."""
    ciclo: str
    clientes: int
    multimarcas: int
    percent: float
    valor: float


class AuditoriaItem(BaseModel):
    """Audit record item."""
    ciclo: str
    setor: str
    codigo_revendedora: str
    codigo_produto_original: str
    codigo_normalizado: str
    nome_produto: str
    motivo: str


class ProdutoNovo(BaseModel):
    """Unregistered product record."""
    sku: str
    nome: str
    qtde_vendas: int
    total_itens: int
    valor_total: float
    ciclos: str
    setores: str


class IAFMetrica(BaseModel):
    """IAF penetration metrics."""
    total_clientes: int
    clientes_iaf: int
    percentual: int
    tipo: str


class IAFSetorMetrica(BaseModel):
    """IAF metrics by sector."""
    setor: str
    clientes_ativos: int
    clientes_cabelos: int
    percent_cabelos: int
    clientes_make: int
    percent_make: int


class ClienteDetalhe(BaseModel):
    """Detailed customer information."""
    encontrado: bool
    cliente_id: Optional[str] = None
    nome: Optional[str] = None
    codigo: Optional[str] = None
    setor: Optional[str] = None
    total_itens: Optional[int] = None
    total_valor: Optional[float] = None
    marcas: Optional[List[str]] = None
    qtde_marcas: Optional[int] = None
    is_multimarcas: Optional[bool] = None
    compras: Optional[List[Dict[str, Any]]] = None


class FiltrosDisponiveis(BaseModel):
    """Available filter options."""
    ciclos: List[str]
    setores: List[str]
    marcas: List[str]
    gerencias: List[str] = []


class HealthCheck(BaseModel):
    """Health check response."""
    status: str
    timestamp: str
    service: str
