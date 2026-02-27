"""
Tests for SKU matching functions.
"""
import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import (
    MOTIVO_MATCH_EXATO,
    MOTIVO_MATCH_COM_ZERO,
    MOTIVO_MATCH_SEM_ZERO,
    MOTIVO_NAO_ENCONTRADO,
)
from app.services.produto import buscar_sku_no_indice, criar_indice_sku_em_memoria


class TestBuscarSKUNoIndice:
    """Tests for buscar_sku_no_indice function."""

    @pytest.fixture
    def indice_basico(self):
        """Create a basic test index."""
        return {
            "12345": {"sku": "12345", "nome": "Produto A", "marca": "oBoticário"},
            "01234": {"sku": "01234", "nome": "Produto B", "marca": "Eudora"},
            "99999": {"sku": "99999", "nome": "Produto C", "marca": "O.U.I"},
        }

    def test_match_exato(self, indice_basico):
        """Should find exact match."""
        marca, nome, motivo = buscar_sku_no_indice("12345", indice_basico)
        assert marca == "oBoticário"
        assert nome == "Produto A"
        assert motivo == MOTIVO_MATCH_EXATO

    def test_match_com_zero(self, indice_basico):
        """Should find match by adding leading zero."""
        # SKU 1234 should match 01234 in the index
        marca, nome, motivo = buscar_sku_no_indice("1234", indice_basico)
        assert marca == "Eudora"
        assert nome == "Produto B"
        assert motivo == MOTIVO_MATCH_COM_ZERO

    def test_match_sem_zero(self):
        """Should find match by removing leading zero."""
        indice = {
            "1234": {"sku": "1234", "nome": "Produto X", "marca": "Eudora"},
        }
        marca, nome, motivo = buscar_sku_no_indice("01234", indice)
        assert marca == "Eudora"
        assert nome == "Produto X"
        assert motivo == MOTIVO_MATCH_SEM_ZERO

    def test_nao_encontrado(self, indice_basico):
        """Should return NAO_ENCONTRADO when not found."""
        marca, nome, motivo = buscar_sku_no_indice("00000", indice_basico)
        assert marca is None
        assert nome is None
        assert motivo == MOTIVO_NAO_ENCONTRADO

    def test_sku_vazio(self, indice_basico):
        """Should handle empty SKU."""
        marca, nome, motivo = buscar_sku_no_indice("", indice_basico)
        assert marca is None
        assert motivo == MOTIVO_NAO_ENCONTRADO

    def test_sku_none(self, indice_basico):
        """Should handle None SKU."""
        marca, nome, motivo = buscar_sku_no_indice(None, indice_basico)
        assert marca is None
        assert motivo == MOTIVO_NAO_ENCONTRADO

    def test_normaliza_antes_de_buscar(self, indice_basico):
        """Should normalize SKU before searching."""
        # SKU with spaces and non-numeric chars
        marca, nome, motivo = buscar_sku_no_indice("  SKU-12345  ", indice_basico)
        assert marca == "oBoticário"
        assert motivo == MOTIVO_MATCH_EXATO


class TestCriarIndiceEmMemoria:
    """Tests for criar_indice_sku_em_memoria function."""

    def test_cria_variacoes_com_zero(self):
        """Index should include variations with leading zero."""
        # This would need a mock database connection
        # For now, just document the expected behavior
        pass

    def test_cria_variacoes_sem_zero(self):
        """Index should include variations without leading zero."""
        # This would need a mock database connection
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
