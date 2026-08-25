"""
Testes do calendário de ciclos e da meta diária (ritmo do dia).

Nenhum teste toca a API do Slack — só monta blocos em memória.
"""
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.calendario_ciclos import (
    CALENDARIO_CICLOS,
    DIAS_EXTENSAO,
    ciclo_da_data,
    dias_uteis_entre,
    eh_dia_util,
    obter_ciclo,
    parse_ciclo,
    posicao_ciclo,
)
from app.services.metas import calcular_meta_diaria
from app.services.slack_service import build_blocks, build_blocks_diario, enviar_meta_slack


# ---------------------------------------------------------------------------
# Calendário
# ---------------------------------------------------------------------------

class TestCalendario:
    def test_parse_ciclo(self):
        assert parse_ciclo("12/2026") == (2026, 12)
        assert parse_ciclo("1/2026") == (2026, 1)
        assert parse_ciclo("12", ano_padrao=2026) == (2026, 12)
        assert parse_ciclo("") is None
        assert parse_ciclo("abc") is None
        assert parse_ciclo("12/x") is None

    def test_dia_util_seg_a_sabado_sem_feriado(self):
        assert eh_dia_util(date(2026, 8, 24))       # segunda
        assert eh_dia_util(date(2026, 8, 29))       # sábado conta
        assert not eh_dia_util(date(2026, 8, 30))   # domingo não
        assert not eh_dia_util(date(2026, 9, 7))    # Independência
        assert not eh_dia_util(date(2026, 4, 3))    # Sexta-feira Santa 2026
        assert eh_dia_util(date(2026, 2, 17))       # Carnaval NÃO é descontado

    def test_dias_uteis_entre(self):
        assert dias_uteis_entre(date(2026, 8, 10), date(2026, 8, 15)) == 6   # seg→sáb
        assert dias_uteis_entre(date(2026, 8, 10), date(2026, 8, 16)) == 6   # domingo não soma
        assert dias_uteis_entre(date(2026, 8, 16), date(2026, 8, 10)) == 0   # invertido

    def test_regra_fecha_com_a_tabela_da_gerencia(self):
        """Seg–sáb − feriados + 2 reproduz a coluna 'Úteis Geral (+2)' (13 e 14 têm 1 dia a menos na tabela)."""
        divergentes = []
        for (ano, num), (ini, fim, uteis) in CALENDARIO_CICLOS.items():
            calc = dias_uteis_entre(ini, fim) + DIAS_EXTENSAO
            if calc != uteis:
                divergentes.append((num, uteis, calc))
        assert divergentes == [(13, 18, 19), (14, 19, 20)]

    def test_obter_ciclo_e_extensao(self):
        c = obter_ciclo("12/2026")
        assert c["inicio"] == date(2026, 8, 10)
        assert c["fim"] == date(2026, 8, 30)
        assert c["dias_uteis"] == 20
        # fim 30/08 é domingo → extensão = seg 31/08 e ter 01/09
        assert c["fim_extensao"] == date(2026, 9, 1)
        assert obter_ciclo("18/2026") is None
        assert obter_ciclo("12/2030") is None

    def test_ciclo_da_data(self):
        assert ciclo_da_data(date(2026, 8, 25)) == "12/2026"
        assert ciclo_da_data(date(2025, 12, 26)) == "01/2026"
        assert ciclo_da_data(date(2026, 12, 25)) == "17/2026"
        assert ciclo_da_data(date(2030, 1, 1)) is None


class TestPosicaoCiclo:
    def test_meio_do_ciclo(self):
        p = posicao_ciclo("12/2026", date(2026, 8, 25))   # terça
        assert p["status"] == "andamento"
        assert p["dias_uteis"] == 20
        assert p["dia_atual"] == 14
        assert p["dias_concluidos"] == 13
        assert p["dias_restantes"] == 7          # inclui hoje
        assert p["hoje_util"] is True
        assert p["progresso_pct"] == 70.0

    def test_primeiro_dia(self):
        p = posicao_ciclo("12/2026", date(2026, 8, 10))
        assert p["dia_atual"] == 1
        assert p["dias_concluidos"] == 0
        assert p["dias_restantes"] == 20

    def test_domingo_nao_e_dia_util(self):
        p = posicao_ciclo("12/2026", date(2026, 8, 30))   # domingo, fim regular
        assert p["hoje_util"] is False
        assert p["dia_atual"] == 18
        assert p["dias_restantes"] == 2                    # só os 2 de extensão

    def test_extensao_e_encerrado(self):
        p = posicao_ciclo("12/2026", date(2026, 9, 1))
        assert p["status"] == "extensao"
        assert p["dia_atual"] == 20
        assert p["dias_restantes"] == 1

        p = posicao_ciclo("12/2026", date(2026, 9, 2))
        assert p["status"] == "encerrado"
        assert p["dias_restantes"] == 0
        assert p["dia_atual"] == 20

    def test_antes_do_inicio(self):
        p = posicao_ciclo("12/2026", date(2026, 8, 9))
        assert p["status"] == "antes"
        assert p["dia_atual"] == 0
        assert p["dias_restantes"] == 20

    def test_dia_atual_nunca_passa_do_total_da_tabela(self):
        # Ciclo 13: tabela diz 18, a regra daria 19 — o total da tabela manda.
        c = obter_ciclo("13/2026")
        p = posicao_ciclo("13/2026", c["fim_extensao"])
        assert p["dia_atual"] == 18

    def test_ciclo_desconhecido(self):
        assert posicao_ciclo("99/2026", date(2026, 8, 25)) is None


# ---------------------------------------------------------------------------
# Meta diária
# ---------------------------------------------------------------------------

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


class TestCalcularMetaDiaria:
    @pytest.fixture
    def posicao(self):
        return posicao_ciclo("12/2026", date(2026, 8, 25))   # dia 14 de 20, faltam 7

    def test_receita_atrasada(self, posicao):
        r = calcular_meta_diaria(DADOS, posicao)
        rec = next(i for i in r["itens"] if i["chave"] == "receita")
        assert rec["meta_dia"] == pytest.approx(2250.0)
        assert rec["esperado"] == pytest.approx(31500.0)       # 2250 × 14
        assert rec["falta"] == pytest.approx(20000.0)
        assert rec["necessario_dia"] == pytest.approx(20000 / 7)
        assert rec["pct_ritmo"] == pytest.approx(25000 / 31500 * 100)
        assert rec["status"] == "atrasado"

    def test_no_ritmo_e_batida(self, posicao):
        no_ritmo = calcular_meta_diaria({"receita": 32000, "meta_receita": 45000}, posicao)   # ≥ esperado 31.500
        assert no_ritmo["itens"][0]["status"] == "no_ritmo"
        assert no_ritmo["status_geral"] == "no_ritmo"
        batida = calcular_meta_diaria({"receita": 46000, "meta_receita": 45000}, posicao)
        assert batida["itens"][0]["status"] == "batida"
        assert batida["itens"][0]["falta"] == 0

    def test_so_receita(self, posicao):
        """Escolha da gerência: o aviso diário só traz Receita; o resto fica na meta do ciclo."""
        r = calcular_meta_diaria(DADOS, posicao)
        assert [i["chave"] for i in r["itens"]] == ["receita"]

    def test_sem_meta(self, posicao):
        r = calcular_meta_diaria({"receita": 100}, posicao)
        assert r["itens"] == []
        assert r["status_geral"] == "sem_meta"

    def test_primeiro_dia_nao_e_atrasado(self):
        p = posicao_ciclo("12/2026", date(2026, 8, 10))
        r = calcular_meta_diaria({"receita": 0, "meta_receita": 45000}, p)
        rec = r["itens"][0]
        assert rec["esperado"] == pytest.approx(2250.0)
        assert rec["status"] == "atrasado"   # 0 < 2250 no dia 1 já é atraso
        assert rec["necessario_dia"] == pytest.approx(45000 / 20)

    def test_sem_dias_restantes(self):
        p = posicao_ciclo("12/2026", date(2026, 9, 2))   # encerrado
        r = calcular_meta_diaria({"receita": 1000, "meta_receita": 45000}, p)
        assert r["itens"][0]["necessario_dia"] is None

    def test_tudo_batido(self, posicao):
        d = {"receita": 50000, "meta_receita": 45000,
             "clientes_cabelos": 40, "meta_cabelos_qtd": 32,
             "clientes_make": 33, "meta_make_qtd": 32}
        assert calcular_meta_diaria(d, posicao)["status_geral"] == "batida"


# ---------------------------------------------------------------------------
# Blocos Slack (sem chamar a API)
# ---------------------------------------------------------------------------

class TestBlocksDiario:
    def test_estrutura(self):
        pos = posicao_ciclo("12/2026", date(2026, 8, 25))
        blocks = build_blocks_diario("KARINE", "BRONZE 2 / PENEDO", DADOS, pos)
        assert blocks[0]["type"] == "header"
        assert "Meta Diária" in blocks[0]["text"]["text"]
        assert "BRONZE 2 / PENEDO" in blocks[0]["text"]["text"]
        texto = " ".join(
            b["text"]["text"] if "text" in b else " ".join(f["text"] for f in b.get("fields", []))
            for b in blocks if b["type"] == "section"
        )
        assert "KARINE" in texto
        assert "14 de 20" in texto
        assert "R$ 2.857/dia" in texto            # 20000 / 7
        assert "base R$ 2.250/dia" in texto
        assert "Abaixo do ritmo" in texto
        # mesmo padrão visual do card do ciclo: linha com barra e %
        assert "🔴  *Receita:* R$ 25.000  ›  meta R$ 45.000" in texto
        assert "⬜" in texto
        # Nenhum bloco passa do limite de 3000 chars do Slack
        for b in blocks:
            if b["type"] == "section" and "text" in b:
                assert len(b["text"]["text"]) < 3000

    def test_so_receita_cabelo_make_na_mensagem(self):
        pos = posicao_ciclo("12/2026", date(2026, 8, 25))
        blocks = build_blocks_diario("KARINE", "X", DADOS, pos)
        texto = " ".join(b["text"]["text"] for b in blocks if b["type"] == "section" and "text" in b)
        ctx = " ".join(b["elements"][0]["text"] for b in blocks if b["type"] == "context")
        assert "Receita" in texto
        for outro in ("Clientes Ativos", "Multimarca", "IAF Cabelo", "IAF Make"):
            assert outro not in texto
        assert "RPA" not in texto and "RPA" not in ctx

    def test_modo_ciclo_continua_igual(self):
        blocks = build_blocks("KARINE", "X", DADOS)
        assert "Meta por Setor" in blocks[0]["text"]["text"]

    def test_envio_diario_sem_posicao_nao_chama_slack(self, monkeypatch):
        import app.services.slack_service as svc
        monkeypatch.setattr(svc, "SLACK_BOT_TOKEN", "xoxb-fake")
        chamado = {"n": 0}

        class FakeClient:
            def conversations_open(self, users):
                chamado["n"] += 1
                return {"channel": {"id": "D1"}}

            def chat_postMessage(self, **kw):
                chamado["n"] += 1
                return {"ok": True}

        monkeypatch.setattr(svc, "_get_client", lambda: FakeClient())
        r = enviar_meta_slack("KARINE", "X", DADOS, modo="diario", posicao=None)
        assert r["ok"] is False
        assert chamado["n"] == 0


# ---------------------------------------------------------------------------
# Formatação dos cards (regressões)
# ---------------------------------------------------------------------------

class TestFormatacaoCard:
    def test_fmt_pct_nao_reescala_valores_pequenos(self):
        # Os valores chegam em pontos percentuais: 0,8% de IAF Make é "0.8%",
        # não "80.0%" (bug antigo: valores <= 1 eram tratados como fração).
        from app.services.slack_service import _fmt_pct
        assert _fmt_pct(0.8) == "0.8%"
        assert _fmt_pct(1.0) == "1.0%"
        assert _fmt_pct(86.7) == "86.7%"
        assert _fmt_pct("x") == "—"

    def test_card_ciclo_pct_pequeno_coerente_com_barra(self):
        dados = dict(DADOS, make=0.8, meta_make=60.0)
        blocks = build_blocks("KARINE", "X", dados)
        texto = " ".join(b["text"]["text"] for b in blocks if b["type"] == "section" and "text" in b)
        linha = next(l for l in texto.splitlines() if "IAF Make" in l)
        assert "0.8%" in linha and "80.0%" not in linha
        assert linha.rstrip().endswith("*1%*")

    def test_rodape_usa_data_de_brasilia(self, monkeypatch):
        # O Render roda em UTC: o rodapé "Gerado em" tem de vir de hoje_brasil().
        import app.services.slack_service as svc
        monkeypatch.setattr(svc, "hoje_brasil", lambda: date(2026, 8, 25))
        pos = posicao_ciclo("12/2026", date(2026, 8, 25))
        for blocks in (build_blocks("K", "X", DADOS), build_blocks_diario("K", "X", DADOS, pos)):
            rodape = [b for b in blocks if b["type"] == "context"][-1]["elements"][0]["text"]
            assert "Gerado em 25/08/2026" in rodape
