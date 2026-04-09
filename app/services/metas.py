"""
Service for reading and matching sector goals from metas.xlsx.

The spreadsheet has one row per sector (gerencia 13707 only).
Sector names in the spreadsheet use short keys like "BRONZE 1";
the app stores full names like "Bronze 1 / CORURIPE / FELIZ DESERTO".
Matching is done by checking whether the app name starts with the
spreadsheet key (case-insensitive).
"""
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.config import BASE_DIR

METAS_PATH = BASE_DIR / "metas.xlsx"

# Column names in metas.xlsx
_COL_SETOR        = "SETOR"
_COL_SUPERVISORA  = "SUPERVISORA"
_COL_RECEITA      = "RECEITA"
_COL_ATIVO        = "ATIVO"
_COL_RPA          = "RPA"
_COL_MULTI_PCT    = "MULTIMARCA (%)"
_COL_MULTI_QTD    = "MULTIMARCA (Qtd)"
_COL_CABELO_PCT   = "CABELO (%)"
_COL_CABELO_QTD   = "CABELO (Qtd)"
_COL_MAKE_PCT     = "MAKE (%)"
_COL_MAKE_QTD     = "MAKE (Qtd)"


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_brl(value: str) -> float:
    """Parse Brazilian currency string 'R$ 50.000,00' → 50000.0."""
    if not value:
        return 0.0
    cleaned = str(value).replace("R$", "").replace(" ", "").strip()
    # Remove thousands separator (.) then swap decimal separator (, → .)
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_pct(value: str) -> float:
    """Parse '0.73' → 73.0  or  '73' → 73.0  (always returns percentage points)."""
    try:
        v = float(str(value).strip().replace(",", "."))
        # Values ≤ 1.0 are fractions (0.73 = 73%)
        return round(v * 100, 1) if v <= 1.0 else round(v, 1)
    except (ValueError, TypeError):
        return 0.0


def _parse_int(value: str) -> int:
    try:
        return int(str(value).strip())
    except (ValueError, TypeError):
        return 0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ler_planilha_metas() -> List[Dict[str, Any]]:
    """
    Read metas.xlsx and return a list of parsed meta dicts.

    Uses openpyxl so that numeric cells are returned as floats (no need to
    parse currency strings for those columns) and accented names are read
    correctly as Unicode strings.

    Returns [] if the file does not exist or cannot be read.
    Each dict has keys:
        setor, supervisora,
        receita, ativo, rpa,
        multimarca_pct, multimarca_qtd,
        cabelo_pct, cabelo_qtd,
        make_pct, make_qtd
    """
    if not METAS_PATH.exists():
        return []

    try:
        from openpyxl import load_workbook
        wb = load_workbook(str(METAS_PATH), data_only=True)
        ws = wb.active
    except Exception as exc:
        print(f"[WARN] Could not read metas.xlsx: {exc}")
        return []

    # Build column index from header row
    headers = [str(cell.value or "").strip() for cell in next(ws.iter_rows(min_row=1, max_row=1))]
    col_idx = {h: i for i, h in enumerate(headers)}

    def _get(row_vals: tuple, col_name: str):
        i = col_idx.get(col_name)
        return row_vals[i] if i is not None else None

    result: List[Dict[str, Any]] = []
    for row_vals in ws.iter_rows(min_row=2, values_only=True):
        setor = str(_get(row_vals, _COL_SETOR) or "").strip()
        if not setor:
            continue

        raw_receita = _get(row_vals, _COL_RECEITA)
        raw_rpa     = _get(row_vals, _COL_RPA)

        result.append(
            {
                "setor":          setor,
                "supervisora":    str(_get(row_vals, _COL_SUPERVISORA) or "").strip(),
                # RECEITA / RPA may be strings ("R$ 50.000,00") or numbers
                "receita":        _parse_brl(str(raw_receita)) if isinstance(raw_receita, str) else float(raw_receita or 0),
                "ativo":          _parse_int(str(_get(row_vals, _COL_ATIVO) or 0)),
                "rpa":            _parse_brl(str(raw_rpa)) if isinstance(raw_rpa, str) else float(raw_rpa or 0),
                # Percentages come as floats like 0.73 → convert to 73.0
                "multimarca_pct": _parse_pct(str(_get(row_vals, _COL_MULTI_PCT) or 0)),
                "multimarca_qtd": _parse_int(str(_get(row_vals, _COL_MULTI_QTD) or 0)),
                "cabelo_pct":     _parse_pct(str(_get(row_vals, _COL_CABELO_PCT) or 0)),
                "cabelo_qtd":     _parse_int(str(_get(row_vals, _COL_CABELO_QTD) or 0)),
                "make_pct":       _parse_pct(str(_get(row_vals, _COL_MAKE_PCT) or 0)),
                "make_qtd":       _parse_int(str(_get(row_vals, _COL_MAKE_QTD) or 0)),
            }
        )
    return result


def encontrar_meta_setor(
    nome_setor_app: str,
    metas: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    Find the planilha meta row whose key matches the app's sector name.

    "BRONZE 1" matches "Bronze 1 / CORURIPE / FELIZ DESERTO" because the
    normalised app name starts with the normalised key followed by a
    separator character (' ', '/', '-') or equals it exactly.
    """
    nome_norm = nome_setor_app.strip().upper()
    for meta in metas:
        chave = meta["setor"].strip().upper()
        if nome_norm == chave:
            return meta
        if nome_norm.startswith(chave) and len(nome_norm) > len(chave):
            if nome_norm[len(chave)] in (" ", "/", "-"):
                return meta
    return None
