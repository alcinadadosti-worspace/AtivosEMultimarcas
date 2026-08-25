"""
Testes do recorte "hoje" da meta diária: normalização da DataCaptacao,
detecção de planilha só-do-dia vs acumulada, meta do dia e blocos Slack.

Nenhum teste toca a API do Slack.
"""
import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import VENDAS_COL_DATA_ISO
from app.services.calendario_ciclos import posicao_ciclo
from app.services.metas import acumulado_valido, calcular_meta_do_dia
from app.services.slack_service import build_blocks_diario
from app.services.venda import normalizar_data_iso, obter_periodo_datas


class TestNormalizarDataIso:
    def test_formatos(self):
        assert normalizar_data_iso("2026-08-25T00:00:00.000") == "2026-08-25"   # xlsx
        assert normalizar_data_iso("25/08/2026 00:00:00") == "2026-08-25"       # CSV
        assert normalizar_data_iso("2026-08-25") == "2026-08-25"
        assert normalizar_data_iso("") is None
        assert normalizar_data_iso(None) is None
        assert normalizar_data_iso("ontem") is None


class TestPeriodoDatas:
    def test_periodo(self):
        df = pl.DataFrame({VENDAS_COL_DATA_ISO: ["2026-08-25", "2026-08-24", None, "2026-08-25"]})
        p = obter_periodo_datas(df)
        assert p["tem_data"] is True
        assert (p["data_min"], p["data_max"], p["n_dias"]) == ("2026-08-24", "2026-08-25", 2)
        assert p["dias"] == ["2026-08-24", "2026-08-25"]

    def test_sem_coluna(self):
        p = obter_periodo_datas(pl.DataFrame({"x": [1]}))
        assert p["tem_data"] is False and p["n_dias"] == 0
        assert obter_periodo_datas(None)["tem_data"] is False


class TestAcumuladoValido:
    POS = posicao_ciclo("12/2026", date(2026, 8, 25))   # início 10/08

    def test_planilha_do_ciclo(self):
        assert acumulado_valido({"tem_data": True, "data_min": "2026-08-07"}, self.POS)   # captação do ciclo anterior
        assert acumulado_valido({"tem_data": True, "data_min": "2026-08-10"}, self.POS)   # começa no dia 1
        assert acumulado_valido({"tem_data": True, "data_min": "2026-08-12"}, self.POS)   # tolerância de 2 dias

    def test_planilha_so_do_dia(self):
        assert not acumulado_valido({"tem_data": True, "data_min": "2026-08-24"}, self.POS)
        assert not acumulado_valido({"tem_data": True, "data_min": "2026-08-13"}, self.POS)

    def test_sem_data_assume_acumulado(self):
        assert acumulado_valido(None, self.POS)
        assert acumulado_valido({"tem_data": False}, self.POS)


DADOS = {
    "receita": 25000, "meta_receita": 45000,
    "clientes_ativos": 55, "meta_ativo": 70,
    "rpa": 455, "meta_rpa": 500,
    "multimarca": 60, "meta_multimarca": 73,
    "cabelos": 30, "meta_cabelos": 36,
    "make": 28, "meta_make": 35,
    "clientes_multimarcas": 40, "meta_multimarca_qtd": 66,
    "clientes_cabelos": 30, "meta_cabelos_qtd": 32,
    "clientes_make": 33, "meta_make_qtd": 32,
}
HOJE = {"data": "2026-08-25", "receita": 3200, "clientes_ativos": 5,
        "clientes_multimarcas": 2, "clientes_cabelos": 1, "clientes_make": 3}


class TestCalcularMetaDoDia:
    POS = posicao_ciclo("12/2026", date(2026, 8, 25))   # 20 dias úteis

    def test_contra_meta_do_dia(self):
        r = calcular_meta_do_dia({**DADOS, "hoje": HOJE}, self.POS)
        por = {i["chave"]: i for i in r["itens"]}
        assert por["receita"]["meta_dia"] == pytest.approx(2250.0)
        assert por["receita"]["pct"] == pytest.approx(3200 / 2250 * 100)
        assert por["receita"]["status"] == "batida"
        assert list(por) == ["receita", "clientes_multimarcas", "clientes_cabelos", "clientes_make"]   # os 4 da gerência
        assert por["clientes_multimarcas"]["status"] == "abaixo"   # 2 < 3,3
        assert por["clientes_cabelos"]["meta_dia"] == pytest.approx(1.6)
        assert por["clientes_cabelos"]["status"] == "abaixo"      # 1 < 1,6
        assert por["clientes_make"]["status"] == "batida"         # 3 ≥ 1,6
        assert r["status_geral"] == "abaixo"
        assert r["data"] == "2026-08-25"

    def test_sem_recorte(self):
        r = calcular_meta_do_dia(DADOS, self.POS)
        assert r["itens"] == [] and r["status_geral"] == "sem_recorte"

    def test_dia_zerado(self):
        zero = {"data": "2026-08-25", "receita": 0, "clientes_ativos": 0,
                "clientes_multimarcas": 0, "clientes_cabelos": 0, "clientes_make": 0}
        r = calcular_meta_do_dia({**DADOS, "hoje": zero}, self.POS)
        assert all(i["status"] == "abaixo" and i["pct"] == 0 for i in r["itens"])


def _texto(blocks):
    partes = []
    for b in blocks:
        if b["type"] == "section" and "text" in b:
            partes.append(b["text"]["text"])
        elif b["type"] == "context":
            partes.append(b["elements"][0]["text"])
    return "\n".join(partes)


class TestBlocksComRecorte:
    POS = posicao_ciclo("12/2026", date(2026, 8, 25))

    def test_planilha_acumulada_mostra_hoje_e_acumulado(self):
        d = {**DADOS, "hoje": HOJE, "planilha": {"tem_data": True, "data_min": "2026-08-07", "data_max": "2026-08-25", "n_dias": 19}}
        t = _texto(build_blocks_diario("KARINE", "X", d, self.POS))
        assert "Hoje (25/08)" in t
        assert "R$ 3.200" in t and "meta do dia R$ 2.250" in t
        assert "Acumulado no ciclo" in t
        assert "esperado até hoje" in t
        assert "↳ ritmo" in t                               # detalhe de ritmo por indicador
        assert "meta do dia R$ 2.250" in t                   # linha do Hoje no padrão do card do ciclo
        assert "razões" not in t and "RPA" not in t          # só Receita / Cabelo / Make
        assert "não está disponível" not in t

    def test_planilha_so_do_dia_esconde_acumulado(self):
        d = {**DADOS, "hoje": HOJE, "planilha": {"tem_data": True, "data_min": "2026-08-24", "data_max": "2026-08-25", "n_dias": 2}}
        t = _texto(build_blocks_diario("KARINE", "X", d, self.POS))
        assert "Hoje (25/08)" in t
        assert "Acumulado no ciclo" not in t
        assert "↳" not in t                          # nenhuma linha de ritmo acumulado
        assert "cobre só 24/08 → 25/08" in t
        assert "Dia abaixo da meta" in t          # status vem do dia, não do acumulado

    def test_sem_recorte_mantem_formato_antigo(self):
        t = _texto(build_blocks_diario("KARINE", "X", DADOS, self.POS))
        assert "Hoje (" not in t
        assert "Acumulado no ciclo" in t
        assert "esperado até hoje" in t
