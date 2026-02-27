"""
Tests for SKU and brand normalization functions.
"""
import math
import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.utils.normalizers import normalizar_sku, normalizar_marca


class TestNormalizarSKU:
    """Tests for normalizar_sku function."""

    def test_preserva_zeros_a_esquerda(self):
        """Should preserve leading zeros."""
        assert normalizar_sku("01234") == "01234"
        assert normalizar_sku("00123") == "00123"
        assert normalizar_sku("001") == "001"

    def test_remove_caracteres_nao_numericos(self):
        """Should remove non-numeric characters."""
        assert normalizar_sku("ABC123") == "123"
        assert normalizar_sku("12-34") == "1234"
        assert normalizar_sku("12.34") == "1234"
        assert normalizar_sku("SKU-001") == "001"

    def test_remove_espacos(self):
        """Should remove whitespace."""
        assert normalizar_sku("  01234  ") == "01234"
        assert normalizar_sku("01 234") == "01234"
        assert normalizar_sku("\t01234\n") == "01234"

    def test_trata_float_inteiro(self):
        """Should handle integer floats correctly."""
        assert normalizar_sku(1234.0) == "1234"
        assert normalizar_sku(12345.0) == "12345"

    def test_trata_float_com_decimal(self):
        """Should handle floats with decimals."""
        # Floats with decimals get converted to string representation
        assert normalizar_sku(1234.5) == "12345"

    def test_trata_none(self):
        """Should return empty string for None."""
        assert normalizar_sku(None) == ""

    def test_trata_nan(self):
        """Should return empty string for NaN."""
        assert normalizar_sku(float('nan')) == ""

    def test_trata_string_vazia(self):
        """Should return empty string for empty string."""
        assert normalizar_sku("") == ""
        assert normalizar_sku("   ") == ""

    def test_trata_inteiro(self):
        """Should handle integers correctly."""
        assert normalizar_sku(1234) == "1234"
        assert normalizar_sku(12345) == "12345"

    def test_remove_sufixo_ponto_zero(self):
        """Should remove .0 suffix from string representation."""
        assert normalizar_sku("1234.0") == "1234"
        assert normalizar_sku("01234.0") == "01234"


class TestNormalizarMarca:
    """Tests for normalizar_marca function."""

    def test_normaliza_oboticario(self):
        """Should normalize O Boticario variations."""
        assert normalizar_marca("OBOTICARIO") == "oBoticário"
        assert normalizar_marca("OBOTICÁRIO") == "oBoticário"
        assert normalizar_marca("O BOTICÁRIO") == "oBoticário"
        assert normalizar_marca("O BOTICARIO") == "oBoticário"
        assert normalizar_marca("BOT") == "oBoticário"

    def test_normaliza_eudora(self):
        """Should normalize Eudora variations."""
        assert normalizar_marca("EUD") == "Eudora"
        assert normalizar_marca("EUDORA") == "Eudora"
        assert normalizar_marca("eudora") == "Eudora"

    def test_normaliza_qdb(self):
        """Should normalize Quem Disse Berenice variations."""
        assert normalizar_marca("QDB") == "Quem Disse Berenice"
        assert normalizar_marca("QUEM DISSE BERENICE") == "Quem Disse Berenice"

    def test_normaliza_oui(self):
        """Should normalize O.U.I variations."""
        assert normalizar_marca("OUI") == "O.U.I"
        assert normalizar_marca("O.U.I") == "O.U.I"
        assert normalizar_marca("O.U.I.") == "O.U.I"

    def test_normaliza_aumigos(self):
        """Should normalize AuAmigos variations."""
        assert normalizar_marca("AUMIGOS") == "AuAmigos"
        assert normalizar_marca("AU MIGOS") == "AuAmigos"
        assert normalizar_marca("AU AMIGOS") == "AuAmigos"

    def test_trata_none(self):
        """Should return DESCONHECIDA for None."""
        assert normalizar_marca(None) == "DESCONHECIDA"

    def test_trata_nan(self):
        """Should return DESCONHECIDA for NaN."""
        assert normalizar_marca(float('nan')) == "DESCONHECIDA"

    def test_trata_string_vazia(self):
        """Should return DESCONHECIDA for empty string."""
        assert normalizar_marca("") == "DESCONHECIDA"
        assert normalizar_marca("   ") == "DESCONHECIDA"

    def test_marca_desconhecida(self):
        """Should return original for unknown brands."""
        assert normalizar_marca("OutraMarca") == "OutraMarca"
        assert normalizar_marca("Nova Marca") == "Nova Marca"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
