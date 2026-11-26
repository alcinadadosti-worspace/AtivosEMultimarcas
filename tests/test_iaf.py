"""
Tests for the IAF heuristics (combos de cabelo / maquiagem).
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.iaf import is_hair_combo, is_makeup_product


class TestComboCabeloMatch:
    """Combos/sachês da linha Match contam como IAF Cabelos mesmo sem SH/COND no nome."""

    @pytest.mark.parametrize("nome", [
        "CJ SCH MATCH SCIENCE RECONS 3x7ml VDA",
        "COMBO MATCH SCIENCE CRESCIMENTO",
        "CJ SCH MATCH NUTR REGENERADORA 3x7ml",
        "CJ SCH MATCH HID ANTIF SH/COND VDA 2x7ml",
    ])
    def test_combo_match_e_cabelo(self, nome):
        assert is_hair_combo(nome)
        assert not is_makeup_product(nome)

    def test_match_avulso_nao_e_combo(self):
        # Item avulso só conta pela lista oficial — a regra vale apenas para combos
        assert not is_hair_combo("MATCH SERUM POS QUIMICA 50ml")

    def test_batom_perfect_match_continua_make(self):
        # Maquiagem é testada antes da regra de cabelo em cruzar_vendas_com_iaf
        nome = "COMBO NIINA SECRETS BATOM LIQUIDO SKINNY MATTE + BATOM DUO PERFECT MATCH AVELA"
        assert is_makeup_product(nome)
