# main.py
"""
Script de pruebas para el Databricks SCIM Toolkit.
Aquí importamos todas las funciones de functions.py
y las ejecutamos en bloques modulares (1–10).
Descomenta el bloque que quieras probar.
"""

from functions import (
    log,
    obtener_token,
    revisar_usuario_existe,
    buscar_grupos_usuario_por_texto,
    crear_usuario,
    añadir_usuario_a_grupo,
    generar_nombre_grupo,
    crear_grupo,
    crear_grupo_rol_schema,
    crear_grupo_rol_catalogo,
    añadir_grupo_a_grupo,
    conceder_select_tablas,
    TENANT_ID,
    ACCOUNT_ID,
    ENTORNO,
)

# ==============================
# 🚀 MAIN
# ==============================
def main():
    log("🚀 Iniciando pruebas SCIM Toolkit...")

    # 0) Obtener token
    token = obtener_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default", ENTORNO, TENANT_ID)

    # 1) Revisar si usuario existe
    # usuario = revisar_usuario_existe(token, "testcarlos@email.com")

    # 2) Buscar grupos del usuario (ejemplo con filtro "wspdts")
    # if usuario:
    #     grupos = buscar_grupos_usuario_por_texto(token, usuario["id"], "wspdts")
    #     log(f"Grupos encontrados: {grupos}")

    # 3) Crear usuario
    # nuevo = crear_usuario(token, {"email": "nuevo@gencat.com", "display": "Carlos Nuevo"})
    # log(f"Usuario creado: {nuevo}")

    # 4) Añadir usuario a un grupo
    # if usuario:
    #     añadir_usuario_a_grupo(token, usuario["id"], "NOMBRE_GRUPO")

    # 5) Generar nombre de grupo marketplace
    # nombre_marketplace = generar_nombre_grupo(ENTORNO, "MARKETING")
    # log(f"Nombre marketplace generado: {nombre_marketplace}")

    # 6) Crear grupo
    # crear_grupo(token, "grupo_de_prueba")

    # 7) Crear grupo rol_<entorno>_<schema>_use_schema
    # crear_grupo_rol_schema(token, ENTORNO, "brz_esquema_prueba")

    # 8) Crear grupo rol_<entorno>_<dominio3>_use_catalog
    # crear_grupo_rol_catalogo(token, ENTORNO, "MARKETING")

    # 9) Anidar un grupo dentro de otro
    # añadir_grupo_a_grupo(token, "grupo_hijo", "grupo_padre")

    # 10) Conceder SELECT a usuario en tablas de Unity Catalog
    # WORKSPACE_URL = "https://adb-xxxxxxxxxxxx.xx.azuredatabricks.net"
    # tablas = [
    #     "catalogo.schema.tabla1",
    #     "catalogo.schema.tabla2",
    # ]
    # conceder_select_tablas(token, WORKSPACE_URL, "testcarlos@email.com", tablas)

    log("✅ Fin de ejecución modular.\n")


if __name__ == "__main__":
    main()
