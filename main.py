"""
Multimarks Analytics - Main Application

FastAPI application for sales analysis and multi-brand customer tracking
for Grupo Boticario.
"""
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Cookie, FastAPI, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from app.config import DATABASE_PATH, DATA_DIR
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

    # Check database
    if os.path.exists(DATABASE_PATH):
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM produtos")
        count = cursor.fetchone()[0]
        print(f"[INFO] Database loaded: {count} products")
        conn.close()
    else:
        print("[WARN] Database not found. Run import_db.py first.")

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
    return templates.TemplateResponse("pages/dashboard.html", {
        "request": request,
        "page": "dashboard",
    })


@app.get("/dashboard", response_class=HTMLResponse)
async def page_dashboard(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Dashboard page with overview metrics."""
    return templates.TemplateResponse("pages/dashboard.html", {
        "request": request,
        "page": "dashboard",
        "has_data": has_data,
    })


@app.get("/multimarcas", response_class=HTMLResponse)
async def page_multimarcas(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Multi-brand customers page."""
    return templates.TemplateResponse("pages/multimarcas.html", {
        "request": request,
        "page": "multimarcas",
        "has_data": has_data,
    })


@app.get("/produtos-novos", response_class=HTMLResponse)
async def page_produtos_novos(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Unregistered products page."""
    return templates.TemplateResponse("pages/produtos_novos.html", {
        "request": request,
        "page": "produtos_novos",
        "has_data": has_data,
    })


@app.get("/auditoria", response_class=HTMLResponse)
async def page_auditoria(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Audit page for SKU matching issues."""
    return templates.TemplateResponse("pages/auditoria.html", {
        "request": request,
        "page": "auditoria",
        "has_data": has_data,
    })


@app.get("/cliente", response_class=HTMLResponse)
async def page_cliente(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Customer detail page."""
    return templates.TemplateResponse("pages/cliente.html", {
        "request": request,
        "page": "cliente",
        "has_data": has_data,
    })


@app.get("/iaf", response_class=HTMLResponse)
async def page_iaf(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """IAF premium tracking page."""
    return templates.TemplateResponse("pages/iaf.html", {
        "request": request,
        "page": "iaf",
        "has_data": has_data,
    })


@app.get("/categorias", response_class=HTMLResponse)
async def page_categorias(
    request: Request,
    has_data: bool = Depends(get_user_has_data),
):
    """Category analytics page."""
    return templates.TemplateResponse("pages/categorias.html", {
        "request": request,
        "page": "categorias",
        "has_data": has_data,
    })


# =============================================================================
# HEALTH CHECK (root level)
# =============================================================================

@app.get("/health")
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
