"""
FastAPI routes for Multimarks Analytics.
"""
import sqlite3
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response

import polars as pl

from app.api.dependencies import get_db
from app.api.schemas import (
    UploadResponse,
    MetricasGerais,
    FiltrosDisponiveis,
    HealthCheck,
)
from app.services.venda import (
    processar_planilha_vendas,
    obter_ciclos_unicos,
    obter_setores_unicos,
    obter_marcas_unicas,
)
from app.services.metricas import (
    calcular_metricas_cliente,
    calcular_metricas_gerais,
    calcular_vendas_por_marca,
    calcular_top_setores,
    calcular_evolucao_ciclos,
    aplicar_filtros,
    obter_detalhes_cliente,
)
from app.services.iaf import (
    cruzar_vendas_com_iaf,
    calcular_percentual_iaf,
    calcular_iaf_por_setor,
    listar_vendas_iaf,
)
from app.services.auditoria import (
    obter_estatisticas_auditoria,
    listar_auditoria,
    listar_produtos_novos,
)
from app.utils.exporters import exportar_csv, exportar_excel


# Router for API endpoints
api_router = APIRouter(prefix="/api")

# In-memory storage for current session data
# In production, use Redis or database sessions
_session_data = {
    "df_vendas": None,
    "df_clientes": None,
    "df_iaf": None,
}


def get_session_data():
    """Get current session data."""
    return _session_data


def set_session_data(key: str, value):
    """Set session data."""
    _session_data[key] = value


def clear_session_data():
    """Clear all session data."""
    _session_data["df_vendas"] = None
    _session_data["df_clientes"] = None
    _session_data["df_iaf"] = None


# =============================================================================
# HEALTH CHECK
# =============================================================================

@api_router.get("/health", response_model=HealthCheck)
async def health_check():
    """Health check endpoint for Uptime Robot."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "multimarks-analytics"
    }


@api_router.post("/clear")
async def clear_cache():
    """Clear session cache and force reload on next upload."""
    clear_session_data()
    return {
        "success": True,
        "message": "Cache limpo. Faca upload novamente da planilha."
    }


@api_router.get("/clear")
async def clear_cache_get():
    """Clear session cache (GET method for easy browser access)."""
    clear_session_data()
    return {
        "success": True,
        "message": "Cache limpo. Faca upload novamente da planilha."
    }


# =============================================================================
# UPLOAD
# =============================================================================

@api_router.post("/upload", response_model=UploadResponse)
async def upload_vendas(
    file: UploadFile = File(...),
    conn: sqlite3.Connection = Depends(get_db)
):
    """
    Upload and process a sales spreadsheet.

    Accepts CSV or Excel files with required columns.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="Arquivo nao fornecido")

    # Validate file extension
    if not file.filename.lower().endswith(('.csv', '.xlsx', '.xls')):
        raise HTTPException(
            status_code=400,
            detail="Formato de arquivo invalido. Use CSV ou Excel (.xlsx, .xls)"
        )

    try:
        # Read file content
        content = await file.read()

        # Process spreadsheet
        resultado = processar_planilha_vendas(content, file.filename, conn)

        # Calculate customer metrics
        df_clientes = calcular_metricas_cliente(resultado["df_vendas"])

        # Store in session
        set_session_data("df_vendas", resultado["df_vendas"])
        set_session_data("df_clientes", df_clientes)

        # Process IAF if available
        try:
            df_iaf = cruzar_vendas_com_iaf(resultado["df_vendas"], conn)
            set_session_data("df_iaf", df_iaf)
        except Exception:
            set_session_data("df_iaf", pl.DataFrame())

        return UploadResponse(
            success=True,
            message="Arquivo processado com sucesso",
            estatisticas=resultado["estatisticas"],
            avisos=resultado["avisos"]
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {str(e)}")


# =============================================================================
# FILTERS
# =============================================================================

@api_router.get("/filtros", response_model=FiltrosDisponiveis)
async def get_filtros():
    """Get available filter options from current data."""
    session = get_session_data()
    df_vendas = session.get("df_vendas")

    if df_vendas is None:
        return FiltrosDisponiveis(ciclos=[], setores=[], marcas=[])

    return FiltrosDisponiveis(
        ciclos=obter_ciclos_unicos(df_vendas),
        setores=obter_setores_unicos(df_vendas),
        marcas=obter_marcas_unicas(df_vendas)
    )


# =============================================================================
# DASHBOARD METRICS
# =============================================================================

@api_router.get("/metricas/gerais")
async def get_metricas_gerais(
    ciclos: Optional[str] = Query(None),
    setores: Optional[str] = Query(None),
):
    """Get general dashboard metrics."""
    session = get_session_data()
    df_vendas = session.get("df_vendas")
    df_clientes = session.get("df_clientes")

    if df_vendas is None or df_clientes is None:
        return {"error": "Nenhum dado carregado. Faca upload de uma planilha."}

    # Apply filters
    ciclos_list = ciclos.split(",") if ciclos else None
    setores_list = setores.split(",") if setores else None

    df_vendas_filtrado = aplicar_filtros(df_vendas, ciclos=ciclos_list, setores=setores_list)
    df_clientes_filtrado = aplicar_filtros(df_clientes, ciclos=ciclos_list, setores=setores_list)

    metricas = calcular_metricas_gerais(df_clientes_filtrado, df_vendas_filtrado)
    return metricas


@api_router.get("/metricas/marcas")
async def get_vendas_por_marca(
    ciclos: Optional[str] = Query(None),
    setores: Optional[str] = Query(None),
):
    """Get sales breakdown by brand."""
    session = get_session_data()
    df_vendas = session.get("df_vendas")

    if df_vendas is None:
        return []

    ciclos_list = ciclos.split(",") if ciclos else None
    setores_list = setores.split(",") if setores else None

    df_filtrado = aplicar_filtros(df_vendas, ciclos=ciclos_list, setores=setores_list)
    return calcular_vendas_por_marca(df_filtrado)


@api_router.get("/metricas/top-setores")
async def get_top_setores(
    limite: int = Query(5, ge=1, le=20),
    ciclos: Optional[str] = Query(None),
):
    """Get top sectors by value."""
    session = get_session_data()
    df_clientes = session.get("df_clientes")

    if df_clientes is None:
        return []

    ciclos_list = ciclos.split(",") if ciclos else None
    df_filtrado = aplicar_filtros(df_clientes, ciclos=ciclos_list)

    return calcular_top_setores(df_filtrado, limite=limite)


@api_router.get("/metricas/evolucao")
async def get_evolucao_ciclos(
    setores: Optional[str] = Query(None),
):
    """Get metrics evolution by cycle."""
    session = get_session_data()
    df_clientes = session.get("df_clientes")

    if df_clientes is None:
        return []

    setores_list = setores.split(",") if setores else None
    df_filtrado = aplicar_filtros(df_clientes, setores=setores_list)

    return calcular_evolucao_ciclos(df_filtrado)


# =============================================================================
# MULTIMARCAS
# =============================================================================

@api_router.get("/multimarcas")
async def get_multimarcas(
    ciclos: Optional[str] = Query(None),
    setores: Optional[str] = Query(None),
    limite: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get multi-brand customers list."""
    session = get_session_data()
    df_clientes = session.get("df_clientes")

    if df_clientes is None:
        return {"data": [], "total": 0}

    ciclos_list = ciclos.split(",") if ciclos else None
    setores_list = setores.split(",") if setores else None

    df_filtrado = aplicar_filtros(
        df_clientes,
        ciclos=ciclos_list,
        setores=setores_list,
        apenas_multimarcas=True
    )

    total = len(df_filtrado)
    df_paginado = df_filtrado.slice(offset, limite)

    data = [
        {
            "ciclo": row["CicloFaturamento"],
            "setor": row["Setor"],
            "codigo": row["CodigoRevendedora"],
            "nome": row["NomeRevendedora"],
            "qtde_marcas": row["QtdeMarcasDistintas"],
            "marcas": row["MarcasCompradas"],
            "itens": int(row["ItensTotal"] or 0),
            "valor": float(row["ValorTotal"] or 0),
        }
        for row in df_paginado.iter_rows(named=True)
    ]

    return {"data": data, "total": total}


@api_router.get("/multimarcas/export")
async def export_multimarcas(
    formato: str = Query("csv", regex="^(csv|xlsx)$"),
    ciclos: Optional[str] = Query(None),
    setores: Optional[str] = Query(None),
):
    """Export multi-brand customers to CSV or Excel."""
    session = get_session_data()
    df_clientes = session.get("df_clientes")

    if df_clientes is None:
        raise HTTPException(status_code=400, detail="Nenhum dado carregado")

    ciclos_list = ciclos.split(",") if ciclos else None
    setores_list = setores.split(",") if setores else None

    df_filtrado = aplicar_filtros(
        df_clientes,
        ciclos=ciclos_list,
        setores=setores_list,
        apenas_multimarcas=True
    )

    if formato == "xlsx":
        content = exportar_excel(df_filtrado, "Multimarcas")
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = "multimarcas.xlsx"
    else:
        content = exportar_csv(df_filtrado)
        media_type = "text/csv"
        filename = "multimarcas.csv"

    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# =============================================================================
# CLIENTE
# =============================================================================

@api_router.get("/clientes")
async def listar_clientes(
    busca: Optional[str] = Query(None),
    limite: int = Query(50, ge=1, le=200),
):
    """List customers for selection."""
    session = get_session_data()
    df_clientes = session.get("df_clientes")

    if df_clientes is None:
        return []

    df_unicos = df_clientes.select([
        "ClienteID",
        "NomeRevendedora",
        "CodigoRevendedora",
        "Setor"
    ]).unique()

    if busca:
        df_unicos = df_unicos.filter(
            pl.col("NomeRevendedora").str.to_lowercase().str.contains(busca.lower()) |
            pl.col("CodigoRevendedora").cast(pl.Utf8).str.contains(busca)
        )

    df_unicos = df_unicos.head(limite)

    return [
        {
            "cliente_id": row["ClienteID"],
            "nome": row["NomeRevendedora"],
            "codigo": row["CodigoRevendedora"],
            "setor": row["Setor"],
        }
        for row in df_unicos.iter_rows(named=True)
    ]


@api_router.get("/clientes/{cliente_id}")
async def get_cliente_detalhe(cliente_id: str):
    """Get detailed information for a specific customer."""
    session = get_session_data()
    df_vendas = session.get("df_vendas")

    if df_vendas is None:
        raise HTTPException(status_code=400, detail="Nenhum dado carregado")

    return obter_detalhes_cliente(df_vendas, cliente_id)


# =============================================================================
# AUDITORIA
# =============================================================================

@api_router.get("/auditoria/estatisticas")
async def get_auditoria_estatisticas():
    """Get audit statistics."""
    session = get_session_data()
    df_vendas = session.get("df_vendas")

    if df_vendas is None:
        return {"error": "Nenhum dado carregado"}

    return obter_estatisticas_auditoria(df_vendas)


@api_router.get("/auditoria")
async def get_auditoria(
    motivo: Optional[str] = Query(None),
    limite: int = Query(100, ge=1, le=1000),
):
    """Get audit records list."""
    session = get_session_data()
    df_vendas = session.get("df_vendas")

    if df_vendas is None:
        return []

    return listar_auditoria(df_vendas, motivo=motivo, limite=limite)


@api_router.get("/produtos-novos")
async def get_produtos_novos(
    limite: int = Query(100, ge=1, le=500),
):
    """Get unregistered products list."""
    session = get_session_data()
    df_vendas = session.get("df_vendas")

    if df_vendas is None:
        return []

    return listar_produtos_novos(df_vendas, limite=limite)


@api_router.get("/produtos-novos/export")
async def export_produtos_novos(
    formato: str = Query("csv", regex="^(csv|xlsx)$"),
):
    """Export unregistered products to CSV or Excel."""
    session = get_session_data()
    df_vendas = session.get("df_vendas")

    if df_vendas is None:
        raise HTTPException(status_code=400, detail="Nenhum dado carregado")

    from app.services.auditoria import gerar_produtos_nao_cadastrados
    df_novos = gerar_produtos_nao_cadastrados(df_vendas)

    if df_novos.is_empty():
        raise HTTPException(status_code=404, detail="Nenhum produto novo encontrado")

    if formato == "xlsx":
        content = exportar_excel(df_novos, "ProdutosNovos")
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = "produtos_novos.xlsx"
    else:
        content = exportar_csv(df_novos)
        media_type = "text/csv"
        filename = "produtos_novos.csv"

    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


# =============================================================================
# IAF
# =============================================================================

@api_router.get("/iaf/metricas")
async def get_iaf_metricas():
    """Get IAF penetration metrics."""
    session = get_session_data()
    df_clientes = session.get("df_clientes")
    df_iaf = session.get("df_iaf")

    if df_clientes is None:
        return {"error": "Nenhum dado carregado"}

    if df_iaf is None:
        df_iaf = pl.DataFrame()

    return {
        "cabelos": calcular_percentual_iaf(df_clientes, df_iaf, "Cabelos"),
        "make": calcular_percentual_iaf(df_clientes, df_iaf, "Make"),
        "total": calcular_percentual_iaf(df_clientes, df_iaf, None),
    }


@api_router.get("/iaf/por-setor")
async def get_iaf_por_setor():
    """Get IAF metrics by sector."""
    session = get_session_data()
    df_clientes = session.get("df_clientes")
    df_iaf = session.get("df_iaf")

    if df_clientes is None:
        return []

    if df_iaf is None:
        df_iaf = pl.DataFrame()

    return calcular_iaf_por_setor(df_clientes, df_iaf)


@api_router.get("/iaf/vendas")
async def get_iaf_vendas(
    tipo: Optional[str] = Query(None),
    setor: Optional[str] = Query(None),
    limite: int = Query(100, ge=1, le=500),
):
    """Get IAF sales list."""
    session = get_session_data()
    df_iaf = session.get("df_iaf")

    if df_iaf is None or df_iaf.is_empty():
        return []

    return listar_vendas_iaf(df_iaf, tipo_iaf=tipo, setor=setor, limite=limite)
