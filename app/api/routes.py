"""
FastAPI routes for Multimarks Analytics.
"""
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Cookie, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse, Response

import polars as pl

from app.api.dependencies import get_db
from app.api.schemas import (
    UploadResponse,
    MetricasGerais,
    FiltrosDisponiveis,
    HealthCheck,
)
from app.services.session import (
    get_session,
    get_session_data as get_session_by_id,
    set_session_value,
    clear_session,
    get_session_stats,
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
    calcular_top_setores_completo,
    calcular_evolucao_ciclos,
    calcular_resumo_ciclos,
    calcular_dados_setor_ciclo,
    calcular_combinacoes_marcas,
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
from app.services.categoria import (
    classificar_vendas,
    calcular_metricas_categoria,
    calcular_categoria_por_ciclo,
    calcular_categoria_por_setor,
    listar_produtos_categoria,
    obter_categorias_disponiveis,
)
from app.utils.exporters import exportar_csv, exportar_excel


# Router for API endpoints
api_router = APIRouter(prefix="/api")

# Cookie configuration
SESSION_COOKIE_NAME = "multimarks_session"
SESSION_COOKIE_MAX_AGE = 60 * 60 * 24  # 24 hours


def get_user_session(session_id: Optional[str] = Cookie(None, alias=SESSION_COOKIE_NAME)):
    """
    Dependency to get the current user's session.

    Returns tuple of (session_id, session_data).
    Creates new session if none exists.
    """
    return get_session(session_id)


def create_response_with_session(data: Any, session_id: str) -> JSONResponse:
    """Create a JSON response with session cookie set."""
    response = JSONResponse(content=data)
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        max_age=SESSION_COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
    )
    return response


# Legacy function for compatibility with page routes
def get_session_data():
    """
    Legacy function - returns empty dict for page route compatibility.
    Pages should check has_data via API instead.
    """
    return {"df_vendas": None}


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
async def clear_cache(
    session: tuple = Depends(get_user_session),
):
    """Clear session cache and force reload on next upload."""
    session_id, _ = session
    clear_session(session_id)
    return create_response_with_session({
        "success": True,
        "message": "Cache limpo. Faca upload novamente da planilha."
    }, session_id)


@api_router.get("/clear")
async def clear_cache_get(
    session: tuple = Depends(get_user_session),
):
    """Clear session cache (GET method for easy browser access)."""
    session_id, _ = session
    clear_session(session_id)
    return create_response_with_session({
        "success": True,
        "message": "Cache limpo. Faca upload novamente da planilha."
    }, session_id)


@api_router.get("/session/stats")
async def session_stats():
    """Get session statistics (for monitoring)."""
    return get_session_stats()


@api_router.get("/session/status")
async def session_status(
    session: tuple = Depends(get_user_session),
):
    """Check if current session has data loaded."""
    session_id, session_data = session
    has_data = session_data.get("df_vendas") is not None

    response = create_response_with_session({
        "has_data": has_data,
        "session_id": session_id[:8] + "...",  # Only show first 8 chars for privacy
    }, session_id)

    return response


# =============================================================================
# UPLOAD
# =============================================================================

@api_router.post("/upload")
async def upload_vendas(
    file: UploadFile = File(...),
    conn: sqlite3.Connection = Depends(get_db),
    session: tuple = Depends(get_user_session),
):
    """
    Upload and process a sales spreadsheet.

    Accepts CSV or Excel files with required columns.
    Automatically clears previous session data before processing.
    """
    session_id, session_data = session

    # Clear previous session data automatically
    clear_session(session_id)

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

        # Store in user's session
        set_session_value(session_id, "df_vendas", resultado["df_vendas"])
        set_session_value(session_id, "df_clientes", df_clientes)

        # Process IAF if available
        try:
            df_iaf = cruzar_vendas_com_iaf(resultado["df_vendas"], conn)
            set_session_value(session_id, "df_iaf", df_iaf)
            print(f"[Session {session_id[:8]}] IAF processado: {len(df_iaf)} registros")
        except Exception as e:
            print(f"[Session {session_id[:8]}] Erro ao processar IAF: {e}")
            set_session_value(session_id, "df_iaf", pl.DataFrame())

        response_data = {
            "success": True,
            "message": "Arquivo processado com sucesso",
            "estatisticas": resultado["estatisticas"],
            "avisos": resultado["avisos"]
        }

        return create_response_with_session(response_data, session_id)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {str(e)}")


# =============================================================================
# FILTERS
# =============================================================================

@api_router.get("/filtros", response_model=FiltrosDisponiveis)
async def get_filtros(
    session: tuple = Depends(get_user_session),
):
    """Get available filter options from current data."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")

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
    session: tuple = Depends(get_user_session),
):
    """Get general dashboard metrics."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")
    df_clientes = session_data.get("df_clientes")

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
    session: tuple = Depends(get_user_session),
):
    """Get sales breakdown by brand."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")

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
    session: tuple = Depends(get_user_session),
):
    """Get top sectors by value."""
    session_id, session_data = session
    df_clientes = session_data.get("df_clientes")

    if df_clientes is None:
        return []

    ciclos_list = ciclos.split(",") if ciclos else None
    df_filtrado = aplicar_filtros(df_clientes, ciclos=ciclos_list)

    return calcular_top_setores(df_filtrado, limite=limite)


@api_router.get("/metricas/evolucao")
async def get_evolucao_ciclos(
    setores: Optional[str] = Query(None),
    session: tuple = Depends(get_user_session),
):
    """Get metrics evolution by cycle."""
    session_id, session_data = session
    df_clientes = session_data.get("df_clientes")

    if df_clientes is None:
        return []

    setores_list = setores.split(",") if setores else None
    df_filtrado = aplicar_filtros(df_clientes, setores=setores_list)

    return calcular_evolucao_ciclos(df_filtrado)


@api_router.get("/metricas/top10-setores")
async def get_top10_setores(
    session: tuple = Depends(get_user_session),
):
    """Get top 10 sectors by value with complete data."""
    session_id, session_data = session
    df_clientes = session_data.get("df_clientes")

    if df_clientes is None:
        return []

    return calcular_top_setores_completo(df_clientes, limite=10)


@api_router.get("/metricas/resumo-ciclos")
async def get_resumo_ciclos(
    session: tuple = Depends(get_user_session),
):
    """Get summary metrics by cycle."""
    session_id, session_data = session
    df_clientes = session_data.get("df_clientes")
    df_vendas = session_data.get("df_vendas")

    if df_clientes is None or df_vendas is None:
        return []

    return calcular_resumo_ciclos(df_clientes, df_vendas)


@api_router.get("/metricas/dados-setor-ciclo")
async def get_dados_setor_ciclo(
    session: tuple = Depends(get_user_session),
):
    """Get detailed data by sector and cycle including gerencia."""
    session_id, session_data = session
    df_clientes = session_data.get("df_clientes")
    df_vendas = session_data.get("df_vendas")

    if df_clientes is None or df_vendas is None:
        return []

    return calcular_dados_setor_ciclo(df_clientes, df_vendas)


# =============================================================================
# MULTIMARCAS
# =============================================================================

@api_router.get("/multimarcas")
async def get_multimarcas(
    ciclos: Optional[str] = Query(None),
    setores: Optional[str] = Query(None),
    limite: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    session: tuple = Depends(get_user_session),
):
    """Get multi-brand customers list."""
    session_id, session_data = session
    df_clientes = session_data.get("df_clientes")

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


@api_router.get("/multimarcas/combinacoes")
async def get_combinacoes_marcas(
    limite: int = Query(20, ge=1, le=50),
    ciclos: Optional[str] = Query(None),
    setores: Optional[str] = Query(None),
    session: tuple = Depends(get_user_session),
):
    """Get most frequent brand combinations."""
    session_id, session_data = session
    df_clientes = session_data.get("df_clientes")

    if df_clientes is None:
        return []

    ciclos_list = ciclos.split(",") if ciclos else None
    setores_list = setores.split(",") if setores else None

    df_filtrado = aplicar_filtros(
        df_clientes,
        ciclos=ciclos_list,
        setores=setores_list,
        apenas_multimarcas=True
    )

    return calcular_combinacoes_marcas(df_filtrado, limite=limite)


@api_router.get("/multimarcas/export")
async def export_multimarcas(
    formato: str = Query("csv", pattern="^(csv|xlsx)$"),
    ciclos: Optional[str] = Query(None),
    setores: Optional[str] = Query(None),
    session: tuple = Depends(get_user_session),
):
    """Export multi-brand customers to CSV or Excel."""
    session_id, session_data = session
    df_clientes = session_data.get("df_clientes")

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
    session: tuple = Depends(get_user_session),
):
    """List customers for selection."""
    session_id, session_data = session
    df_clientes = session_data.get("df_clientes")

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
async def get_cliente_detalhe(
    cliente_id: str,
    session: tuple = Depends(get_user_session),
):
    """Get detailed information for a specific customer."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")

    if df_vendas is None:
        raise HTTPException(status_code=400, detail="Nenhum dado carregado")

    return obter_detalhes_cliente(df_vendas, cliente_id)


# =============================================================================
# AUDITORIA
# =============================================================================

@api_router.get("/auditoria/estatisticas")
async def get_auditoria_estatisticas(
    session: tuple = Depends(get_user_session),
):
    """Get audit statistics."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")

    if df_vendas is None:
        return {"error": "Nenhum dado carregado"}

    return obter_estatisticas_auditoria(df_vendas)


@api_router.get("/auditoria")
async def get_auditoria(
    motivo: Optional[str] = Query(None),
    limite: int = Query(100, ge=1, le=1000),
    session: tuple = Depends(get_user_session),
):
    """Get audit records list."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")

    if df_vendas is None:
        return []

    return listar_auditoria(df_vendas, motivo=motivo, limite=limite)


@api_router.get("/produtos-novos")
async def get_produtos_novos(
    limite: int = Query(100, ge=1, le=500),
    session: tuple = Depends(get_user_session),
):
    """Get unregistered products list."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")

    if df_vendas is None:
        return []

    return listar_produtos_novos(df_vendas, limite=limite)


@api_router.get("/produtos-novos/export")
async def export_produtos_novos(
    formato: str = Query("csv", pattern="^(csv|xlsx)$"),
    session: tuple = Depends(get_user_session),
):
    """Export unregistered products to CSV or Excel."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")

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
async def get_iaf_metricas(
    session: tuple = Depends(get_user_session),
):
    """Get IAF penetration metrics."""
    session_id, session_data = session
    df_clientes = session_data.get("df_clientes")
    df_iaf = session_data.get("df_iaf")

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
async def get_iaf_por_setor(
    session: tuple = Depends(get_user_session),
):
    """Get IAF metrics by sector."""
    session_id, session_data = session
    df_clientes = session_data.get("df_clientes")
    df_iaf = session_data.get("df_iaf")

    if df_clientes is None:
        return []

    if df_iaf is None:
        df_iaf = pl.DataFrame()

    return calcular_iaf_por_setor(df_clientes, df_iaf)


@api_router.get("/iaf/vendas")
async def get_iaf_vendas(
    tipo: Optional[str] = Query(None),
    setor: Optional[str] = Query(None),
    limite: int = Query(200, ge=1, le=500),
    session: tuple = Depends(get_user_session),
):
    """Get IAF sales list."""
    session_id, session_data = session
    df_iaf = session_data.get("df_iaf")

    if df_iaf is None:
        return []

    if isinstance(df_iaf, pl.DataFrame) and df_iaf.is_empty():
        return []

    return listar_vendas_iaf(df_iaf, tipo_iaf=tipo, setor=setor, limite=limite)


# =============================================================================
# CATEGORIAS
# =============================================================================

@api_router.get("/categorias/lista")
async def get_categorias_lista():
    """Get list of available categories."""
    return obter_categorias_disponiveis()


@api_router.get("/categorias/metricas")
async def get_categorias_metricas(
    session: tuple = Depends(get_user_session),
):
    """Get metrics by category."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")

    if df_vendas is None:
        return []

    # Classify and calculate metrics
    df_classificado = classificar_vendas(df_vendas)
    return calcular_metricas_categoria(df_classificado)


@api_router.get("/categorias/por-ciclo")
async def get_categorias_por_ciclo(
    session: tuple = Depends(get_user_session),
):
    """Get category metrics by cycle."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")

    if df_vendas is None:
        return []

    df_classificado = classificar_vendas(df_vendas)
    return calcular_categoria_por_ciclo(df_classificado)


@api_router.get("/categorias/por-setor")
async def get_categorias_por_setor(
    session: tuple = Depends(get_user_session),
):
    """Get category metrics by sector."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")

    if df_vendas is None:
        return []

    df_classificado = classificar_vendas(df_vendas)
    return calcular_categoria_por_setor(df_classificado)


@api_router.get("/categorias/{categoria}/produtos")
async def get_produtos_categoria(
    categoria: str,
    limite: int = Query(50, ge=1, le=200),
    session: tuple = Depends(get_user_session),
):
    """Get products in a specific category."""
    session_id, session_data = session
    df_vendas = session_data.get("df_vendas")

    if df_vendas is None:
        return []

    df_classificado = classificar_vendas(df_vendas)
    return listar_produtos_categoria(df_classificado, categoria, limite=limite)
