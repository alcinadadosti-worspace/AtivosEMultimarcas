"""
Multimarks Analytics - Main Application

FastAPI application for sales analysis and multi-brand customer tracking
for Grupo Boticario.
"""
import json
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path

import polars as pl
from fastapi import Cookie, FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from app.config import DATABASE_PATH, DATA_DIR, GEO_PARQUET_PATH, GEO_STATS_PATH
from app.api.routes import api_router, SESSION_COOKIE_NAME
from app.api.dependencies import get_db
from app.services.session import get_session
from app.utils.formatters import formatar_moeda, formatar_numero, formatar_percentual


# =============================================================================
# LIFESPAN
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    print("=" * 60)
    print("Multimarks Analytics - Starting")
    print("=" * 60)

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Check database — run import in background thread if missing so uvicorn
    # starts immediately and the health check passes on first deploy with disk.
    if not os.path.exists(DATABASE_PATH):
        print("[INFO] Database not found — running import_db.py in background...")
        import threading
        import import_db as _import_db
        threading.Thread(target=_import_db.main, daemon=True).start()
    else:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM produtos")
        count = cursor.fetchone()[0]
        print(f"[INFO] Database loaded: {count} products")
        conn.close()

    # Load persistent geo data (clients spreadsheet)
    if os.path.exists(GEO_PARQUET_PATH):
        try:
            app.state.df_geo = pl.read_parquet(GEO_PARQUET_PATH)
            app.state.df_geo_stats = {}
            if os.path.exists(GEO_STATS_PATH):
                with open(GEO_STATS_PATH, encoding="utf-8") as f:
                    app.state.df_geo_stats = json.load(f)
            print(f"[INFO] Geo data loaded: {len(app.state.df_geo)} clients")
        except Exception as e:
            print(f"[WARN] Failed to load geo data: {e}")
            app.state.df_geo = None
            app.state.df_geo_stats = {}
    else:
        app.state.df_geo = None
        app.state.df_geo_stats = {}

    yield

    # Shutdown
    print("[INFO] Shutting down...")


# =============================================================================
# APP SETUP
# =============================================================================

app = FastAPI(
    title="Multimarks Analytics",
    description="Sales analysis and multi-brand customer tracking",
    version="1.0.0",
    lifespan=lifespan,
)

# Mount static files
BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

# Templates
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.cache = None  # Disable cache to avoid unhashable dict key bug in Jinja2

# Add custom filters to Jinja2
templates.env.filters["moeda"] = formatar_moeda
templates.env.filters["numero"] = formatar_numero
templates.env.filters["percentual"] = formatar_percentual

# Include API router
app.include_router(api_router)


# =============================================================================
# PAGE ROUTES
# =============================================================================

def get_user_has_data(session_id: str = Cookie(None, alias=SESSION_COOKIE_NAME)) -> bool:
    """Check if user has data loaded in their session."""
    if not session_id:
        return False
    _, session_data = get_session(session_id)
    return session_data.get("df_vendas") is not None


@app.get("/", response_class=HTMLResponse)
async def page_home(request: Request):
    """Home page - redirects to dashboard."""
    return templates.TemplateResponse(request, "pages/dashboard.html", {"page": "dashboard"})


@app.get("/dashboard", response_class=HTMLResponse)
async def page_dashboard(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Dashboard page with overview metrics."""
    return templates.TemplateResponse(request, "pages/dashboard.html", {"page": "dashboard", "has_data": has_data})


@app.get("/multimarcas", response_class=HTMLResponse)
async def page_multimarcas(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Multi-brand customers page."""
    return templates.TemplateResponse(request, "pages/multimarcas.html", {"page": "multimarcas", "has_data": has_data})


@app.get("/produtos-novos", response_class=HTMLResponse)
async def page_produtos_novos(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Unregistered products page."""
    return templates.TemplateResponse(request, "pages/produtos_novos.html", {"page": "produtos_novos", "has_data": has_data})


@app.get("/auditoria", response_class=HTMLResponse)
async def page_auditoria(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Audit page for SKU matching issues."""
    return templates.TemplateResponse(request, "pages/auditoria.html", {"page": "auditoria", "has_data": has_data})


@app.get("/cliente", response_class=HTMLResponse)
async def page_cliente(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Customer detail page."""
    return templates.TemplateResponse(request, "pages/cliente.html", {"page": "cliente", "has_data": has_data})


@app.get("/iaf", response_class=HTMLResponse)
async def page_iaf(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """IAF premium tracking page."""
    return templates.TemplateResponse(request, "pages/iaf.html", {"page": "iaf", "has_data": has_data})


@app.get("/categorias", response_class=HTMLResponse)
async def page_categorias(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Category analytics page."""
    return templates.TemplateResponse(request, "pages/categorias.html", {"page": "categorias", "has_data": has_data})


@app.get("/ranking", response_class=HTMLResponse)
async def page_ranking(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Reseller ranking and comparison page."""
    return templates.TemplateResponse(request, "pages/ranking.html", {"page": "ranking", "has_data": has_data})


@app.get("/bairros", response_class=HTMLResponse)
async def page_bairros(
    request: Request,
):
    """Geographic analysis page — neighborhoods, cities and heat map."""
    return templates.TemplateResponse(request, "pages/bairros.html", {"page": "bairros"})


@app.get("/meta-setor", response_class=HTMLResponse)
async def page_meta_setor(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Goal tracking by sector page."""
    return templates.TemplateResponse(request, "pages/meta_setor.html", {"page": "meta_setor", "has_data": has_data})


# =============================================================================
# HEALTH CHECK (root level)
# =============================================================================

@app.api_route("/health", methods=["GET", "HEAD"])
async def health():
    """Health check endpoint."""
    from datetime import datetime
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "multimarks-analytics"
    }


# =============================================================================
# RUN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
