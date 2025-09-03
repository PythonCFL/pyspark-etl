# tests/test_functions.py
"""
Tests básicos para Databricks SCIM Toolkit.
Estos tests son de ejemplo y validan funciones auxiliares.
Se ejecutan con pytest.
"""

import pytest
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from functions import generar_nombre_grupo

def test_generar_nombre_grupo_dev():
    nombre = generar_nombre_grupo("DEV", "MEDI_AMBIENT")
    assert "dev" in nombre
    assert "medi_ambient" in nombre

def test_generar_nombre_grupo_pro():
    nombre = generar_nombre_grupo("PRO", "SALUT")
    assert "pro" in nombre
    assert "salut" in nombre