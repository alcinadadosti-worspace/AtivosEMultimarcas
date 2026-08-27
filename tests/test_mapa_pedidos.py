"""
Testes do Mapa de Pedidos — leitura da planilha e agregações.

Cobre os três bugs achados na auditoria do parser:
  1. grafias diferentes da mesma cidade partiam o município em duas linhas
     (e a segunda nunca achava o polígono do mapa);
  2. o segmento na tabela de drill-down não era o mesmo da rosquinha;
  3. a ordem de ciclo era alfabética e quebrava na virada do ano.

Tudo em memória — nenhum teste depende de arquivo em disco.
"""
import io
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import PED_COL_CICLO, PED_COL_PESSOA
from app.services.pedidos import (
    _atribuir_segmento_atual,
    calcular_composicao_cidades,
    calcular_detalhe_cidade,
    calcular_por_cidade,
    processar_planilha_pedidos,
)
from app.services.revendedores import ciclos_do_arquivo, cobertura_por_ciclo
from app.utils.normalizers import canonizar_cidade, chave_cidade


def _planilha(linhas: list[dict]) -> bytes:
    """Excel em memória no formato da Consulta de Pedidos."""
    buf = io.BytesIO()
    pl.DataFrame(linhas).write_excel(buf)
    return buf.getvalue()


def _linha(**kw) -> dict:
    base = {
        PED_COL_PESSOA: "1",
        "NomePessoa": "Fulana",
        "Papel": "Bronze",
        "QtdeMateriais": "1",
        "ValorPraticado": "100",
        "Tipo de Entrega": "No endereço de entrega",
        "CidadeEntregaRetirada": "PENEDO",
        "Cidade": "PENEDO",
        PED_COL_CICLO: "01/2026",
    }
    base.update(kw)
    return base


# ---------------------------------------------------------------------------
# 1. Grafia da cidade
# ---------------------------------------------------------------------------

class TestChaveCidade:
    def test_grafias_do_mesmo_municipio_dao_a_mesma_chave(self):
        esperado = "OLHODAGUAGRANDE"
        assert chave_cidade("OLHO D'ÁGUA GRANDE") == esperado
        assert chave_cidade("OLHO DÁGUA GRANDE") == esperado
        assert chave_cidade("Olho d'Água Grande") == esperado
        assert chave_cidade("  olho d agua grande  ") == esperado

    def test_municipios_diferentes_nao_colidem(self):
        assert chave_cidade("OLHO D'ÁGUA GRANDE") != chave_cidade("OLHO D'ÁGUA DAS FLORES")

    def test_vazio(self):
        assert chave_cidade(None) == ""
        assert chave_cidade("") == ""

    def test_canonizar_escolhe_a_grafia_mais_frequente(self):
        df = pl.DataFrame({
            "_cidade": ["OLHO D'ÁGUA GRANDE"] * 3 + ["OLHO DÁGUA GRANDE"],
        })
        out = canonizar_cidade(df, "_cidade")
        assert out["_cidade"].unique().to_list() == ["OLHO D'ÁGUA GRANDE"]

    def test_canonizar_nao_mexe_em_cidades_distintas(self):
        df = pl.DataFrame({"_cidade": ["PENEDO", "CORURIPE", "IGACI"]})
        out = canonizar_cidade(df, "_cidade")
        assert sorted(out["_cidade"].to_list()) == ["CORURIPE", "IGACI", "PENEDO"]


class TestCidadeNaAgregacao:
    def test_municipio_nao_se_parte_em_duas_linhas(self):
        conteudo = _planilha([
            _linha(**{PED_COL_PESSOA: "1", "CidadeEntregaRetirada": "OLHO D'ÁGUA GRANDE"}),
            _linha(**{PED_COL_PESSOA: "2", "CidadeEntregaRetirada": "OLHO D'ÁGUA GRANDE"}),
            _linha(**{PED_COL_PESSOA: "3", "CidadeEntregaRetirada": "OLHO DÁGUA GRANDE"}),
        ])
        df = processar_planilha_pedidos(conteudo, "p.xlsx")["df"]
        cidades = calcular_por_cidade(df)
        assert len(cidades) == 1
        assert cidades[0]["cidade"] == "OLHO D'ÁGUA GRANDE"   # a grafia que o IBGE usa
        assert cidades[0]["revendedores"] == 3
        assert cidades[0]["pedidos"] == 3


# ---------------------------------------------------------------------------
# 2. Segmento: tabela x rosquinha
# ---------------------------------------------------------------------------

class TestSegmentoDoDrillDown:
    def test_tabela_usa_o_segmento_do_ciclo_mais_recente(self):
        # Subiu de Revendedor (ciclo 01) para Bronze (ciclo 02).
        conteudo = _planilha([
            _linha(**{PED_COL_CICLO: "01/2026", "Papel": "Revendedor"}),
            _linha(**{PED_COL_CICLO: "02/2026", "Papel": "Bronze"}),
        ])
        df = processar_planilha_pedidos(conteudo, "p.xlsx")["df"]
        det = calcular_detalhe_cidade(df, "PENEDO")
        assert [r["segmento"] for r in det["revendedores"]] == ["Bronze"]

    def test_tabela_e_rosquinha_contam_igual(self):
        conteudo = _planilha([
            _linha(**{PED_COL_PESSOA: "1", PED_COL_CICLO: "01/2026", "Papel": "Revendedor"}),
            _linha(**{PED_COL_PESSOA: "1", PED_COL_CICLO: "02/2026", "Papel": "Bronze"}),
            _linha(**{PED_COL_PESSOA: "2", PED_COL_CICLO: "02/2026", "Papel": "Prata"}),
        ])
        df = processar_planilha_pedidos(conteudo, "p.xlsx")["df"]
        rosquinha = {
            s["segmento"]: s["revendedores"]
            for s in calcular_composicao_cidades(df)[0]["segmentos"]
        }
        tabela: dict = {}
        for r in calcular_detalhe_cidade(df, "PENEDO")["revendedores"]:
            tabela[r["segmento"]] = tabela.get(r["segmento"], 0) + 1
        assert rosquinha == tabela == {"Bronze": 1, "Prata": 1}


# ---------------------------------------------------------------------------
# 3. Virada do ano
# ---------------------------------------------------------------------------

class TestOrdemDeCiclo:
    def test_segmento_atual_atravessa_a_virada_do_ano(self):
        df = pl.DataFrame({
            PED_COL_PESSOA: ["1", "1"],
            PED_COL_CICLO: ["16/2026", "01/2027"],
            "_segmento": ["Bronze", "Prata"],
        })
        out = _atribuir_segmento_atual(df, [PED_COL_PESSOA])
        assert set(out["_seg_rev"].to_list()) == {"Prata"}

    def test_rotulo_do_ciclo_mostra_o_ano_quando_cruza(self):
        conteudo = _planilha([
            _linha(**{PED_COL_CICLO: "16/2026"}),
            _linha(**{PED_COL_PESSOA: "2", PED_COL_CICLO: "01/2027"}),
        ])
        res = processar_planilha_pedidos(conteudo, "p.xlsx")
        assert res["ciclo"] == "16/2026–01/2027"
        assert res["estatisticas"]["n_ciclos"] == 2

    def test_rotulo_curto_quando_e_um_ano_so(self):
        conteudo = _planilha([
            _linha(**{PED_COL_CICLO: "01/2026"}),
            _linha(**{PED_COL_PESSOA: "2", PED_COL_CICLO: "11/2026"}),
        ])
        res = processar_planilha_pedidos(conteudo, "p.xlsx")
        assert res["ciclo"] == "01–11"

    def test_ciclo_unico(self):
        res = processar_planilha_pedidos(_planilha([_linha()]), "p.xlsx")
        assert res["ciclo"] == "01"
        assert res["estatisticas"]["n_ciclos"] == 1

    def test_timeline_de_cobertura_em_ordem_cronologica(self):
        df = pl.DataFrame({
            PED_COL_PESSOA: ["1", "2"],
            PED_COL_CICLO: ["16/2026", "01/2027"],
            "_itens": [1, 1],
            "_valor": [10.0, 20.0],
        })
        ordem = [x["ciclo"] for x in cobertura_por_ciclo(df)]
        assert ordem == ["16/2026", "01/2027"]
        # e as duas funções que ordenam ciclo têm que concordar entre si
        assert ordem == ciclos_do_arquivo(df)
