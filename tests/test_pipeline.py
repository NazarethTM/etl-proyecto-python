"""
Tests básicos del pipeline ETL.
Ejecutar con: pytest tests/test_pipeline.py -v
"""

import sys
from pathlib import Path

# Para que pytest encuentre el módulo etl_pipeline
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl_pipeline import (
    validate_dni,
    validate_phone,
    validate_email,
    mask_card,
    capitalize_name,
    remove_accents,
    clean_phone,
)


# ── DNI ──────────────────────────────────────
class TestValidateDni:
    def test_dni_valido(self):
        # DNI inventado con letra correcta para el test
        # 12345678Z → 12345678 % 23 = 14 → letra Z
        assert validate_dni("12345678Z") is True

    def test_dni_letra_incorrecta(self):
        assert validate_dni("12345678A") is False

    def test_dni_demasiado_corto(self):
        assert validate_dni("1234Z") is False

    def test_dni_con_espacios(self):
        # Debe limpiarse internamente
        assert validate_dni("12345678 Z") is True

    def test_dni_vacio(self):
        assert validate_dni("") is False


# ── Teléfono ─────────────────────────────────
class TestValidatePhone:
    def test_telefono_9_digitos(self):
        assert validate_phone("612345678") is True

    def test_telefono_con_prefijo(self):
        assert validate_phone("+34612345678") is True

    def test_telefono_letras(self):
        assert validate_phone("abc") is False

    def test_telefono_muy_corto(self):
        assert validate_phone("1234") is False

    def test_clean_phone(self):
        assert clean_phone("+34 612-345-678") == "34612345678"


# ── Correo ───────────────────────────────────
class TestValidateEmail:
    def test_correo_valido(self):
        assert validate_email("test@example.com") is True

    def test_correo_sin_arroba(self):
        assert validate_email("testexample.com") is False

    def test_correo_sin_dominio(self):
        assert validate_email("test@") is False


# ── Tarjeta ──────────────────────────────────
class TestMaskCard:
    def test_mascara_con_espacios(self):
        assert mask_card("4532 1234 5678 9012") == "XXXX-XXXX-XXXX-9012"

    def test_mascara_con_guiones(self):
        assert mask_card("5500-0000-0000-0004") == "XXXX-XXXX-XXXX-0004"

    def test_mascara_sin_separadores(self):
        assert mask_card("378282246310005") == "XXXX-XXXX-XXXX-0005"

    def test_mascara_numero_corto(self):
        assert mask_card("12") == "XXXX-XXXX-XXXX-XXXX"


# ── Nombres ──────────────────────────────────
class TestNameUtils:
    def test_capitalize_simple(self):
        assert capitalize_name("JOSE") == "Jose"

    def test_capitalize_con_guion(self):
        assert capitalize_name("ANA-MARIA") == "Ana-Maria"

    def test_remove_accents(self):
        assert remove_accents("María") == "Maria"
        assert remove_accents("JOSÉ") == "JOSE"
        assert remove_accents("Ñoño") == "Nono"
