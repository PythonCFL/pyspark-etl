# -*- coding: utf-8 -*-
"""
functions.py – Toolkit SCIM para automatizar gestión de usuarios, grupos y permisos en Databricks.

Contiene funciones modulares para:
1. Revisar si usuario existe
2. Buscar grupos de un usuario por texto
3. Crear usuario
4. Añadir usuario a un grupo
5. Generar nombre de grupo marketplace
6. Crear grupo SCIM (si no existe)
7. Crear grupo rol_<entorno>_<schema>_use_schema
8. Crear grupo rol_<entorno>_<dominio3>_use_catalog
9. Anidar grupo dentro de otro grupo
10. Conceder permisos SELECT sobre tablas de Unity Catalog
"""

import json
import requests
from typing import Optional, Dict, Any, List, Union
from datetime import datetime
from urllib.parse import quote

# ==============================
# 🎯 CONFIGURACIÓN
# ==============================
TENANT_ID   = "TU_TENANT_ID"
ACCOUNT_ID  = "TU_ACCOUNT_ID"
ENTORNO     = "PRO"  # Opciones: DEV | PRE | PRO

LOG_FILE = "scim_account_log.txt"

# ==============================
# 🧾 LOGGING
# ==============================
def log(msg: str):
    """
    Imprime y guarda en fichero el mensaje de log con timestamp.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{timestamp}] {msg}"
    print(full_msg)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(full_msg + "\n")

# ==============================
# 🔐 TOKEN CLIENT CREDENTIALS
# ==============================
def obtener_token(scope: str, entorno: str, tenant_id: str) -> str:
    """
    Genera un token OAuth2 usando client credentials según el entorno.
    """
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    entorno = entorno.lower()

    if entorno == 'dev':
        client_id     = "CLIENT_ID_DEV"
        client_secret = "SECRET_DEV"
    elif entorno == 'pre':
        client_id     = "CLIENT_ID_PRE"
        client_secret = "SECRET_PRE"
    else:
        client_id     = "CLIENT_ID_PRO"
        client_secret = "SECRET_PRO"

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": scope,
    }

    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()["access_token"]

# ==============================
# 📚 HELPERS BASE API
# ==============================
def api_base(account_id): 
    return f"https://accounts.azuredatabricks.net/api/2.0/accounts/{account_id}/scim/v2"

def headers(token): 
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/scim+json"}

# ==============================
# 🔧 FUNCIONES MODULARES (1–10)
# ==============================

def revisar_usuario_existe(token: str, user_input: Union[str, Dict[str, Any]],
                           account_id: str = ACCOUNT_ID) -> Optional[Dict[str, Any]]:
    """
    (1) Revisa si un usuario existe en Databricks.
    Devuelve dict del usuario o None.
    """
    email = user_input if isinstance(user_input, str) else user_input.get("email")
    if not email:
        log("❌ revisar_usuario_existe: falta email en la entrada.")
        return None

    url = f"{api_base(account_id)}/Users"
    r = requests.get(url, headers=headers(token), params={"filter": f'userName eq "{email}"'})
    if r.status_code != 200:
        log(f"❌ Error: {r.status_code} – {r.text}")
        return None
    data = r.json().get("Resources", [])
    usuario = data[0] if data else None
    log("✅ Usuario existe" if usuario else "❌ Usuario NO existe")
    return usuario


def buscar_grupos_usuario_por_texto(token: str, user_id: str, texto: Optional[str],
                                    account_id: str = ACCOUNT_ID) -> List[str]:
    """
    (2) Lista grupos de un usuario que contengan cierto texto.
    Si texto vacío → devuelve todos.
    """
    url = f"{api_base(account_id)}/Groups"
    r = requests.get(url, headers=headers(token), params={"count": 1000})
    if r.status_code != 200:
        log(f"❌ Error al listar grupos: {r.status_code} – {r.text}")
        return []

    filtro = (texto or "").lower().strip()
    grupos = r.json().get("Resources", [])
    resultados = []
    for g in grupos:
        miembros = g.get("members", []) or []
        if any(m.get("value") == user_id for m in miembros):
            nombre = g.get("displayName", "")
            if not filtro or filtro in nombre.lower():
                resultados.append(nombre)

    log(f"🔎 Grupos encontrados: {resultados or '—'}")
    return resultados


def crear_usuario(token: str, user_input: Dict[str, Any], account_id: str = ACCOUNT_ID) -> Optional[Dict[str, Any]]:
    """
    (3) Crea un nuevo usuario en Databricks.
    """
    email = user_input.get("email")
    if not email:
        log("❌ crear_usuario: falta email.")
        return None

    payload = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "userName": email,
        "displayName": user_input.get("display", email.split("@")[0]),
        "name": {"givenName": user_input.get("given", "Nombre"),
                 "familyName": user_input.get("family", "Apellido")},
        "active": True,
        "emails": [{"value": email, "primary": True}]
    }
    url = f"{api_base(account_id)}/Users"
    r = requests.post(url, headers=headers(token), data=json.dumps(payload))
    if r.status_code not in (200, 201):
        log(f"❌ Error al crear usuario: {r.status_code} – {r.text}")
        return None
    log(f"✅ Usuario creado: {email}")
    return r.json()


def añadir_usuario_a_grupo(token: str, user_id: str, nombre_grupo: str,
                           account_id: str = ACCOUNT_ID) -> bool:
    """
    (4) Añade un usuario a un grupo específico.
    """
    # Buscar grupo
    url = f"{api_base(account_id)}/Groups"
    r = requests.get(url, headers=headers(token), params={"filter": f'displayName eq "{nombre_grupo}"'})
    if r.status_code != 200:
        log(f"❌ Error buscando grupo: {r.status_code}")
        return False
    grupos = r.json().get("Resources", [])
    if not grupos:
        log(f"❌ Grupo no encontrado: {nombre_grupo}")
        return False
    grupo_id = grupos[0]["id"]

    # Añadir miembro
    url_patch = f"{api_base(account_id)}/Groups/{grupo_id}"
    body = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [{
            "op": "add",
            "path": "members",
            "value": [{"value": user_id}]
        }]
    }
    r = requests.patch(url_patch, headers=headers(token), data=json.dumps(body))
    if r.status_code not in (200, 204):
        log(f"❌ Error añadiendo al grupo: {r.status_code} – {r.text}")
        return False
    log("✅ Usuario añadido al grupo")
    return True


def generar_nombre_grupo(entorno: str, dominio_valor: str) -> str:
    """
    (5) Genera nombre de grupo marketplace según entorno + dominio.
    """
    return f"pgc_{entorno.lower()}_{dominio_valor.lower()}_marketplace-ug"


def crear_grupo(token: str, nombre_grupo: str, account_id: str = ACCOUNT_ID) -> Optional[Dict[str, Any]]:
    """
    (6) Crea un grupo en Databricks si no existe.
    """
    url = f"{api_base(account_id)}/Groups"
    payload = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
        "displayName": nombre_grupo,
    }
    r = requests.post(url, headers=headers(token), data=json.dumps(payload))
    if r.status_code in (200, 201):
        log(f"✅ Grupo creado: {nombre_grupo}")
        return r.json()
    if r.status_code == 409:
        log(f"⚠️ El grupo ya existe: {nombre_grupo}")
        return {"displayName": nombre_grupo}
    log(f"❌ Error creando grupo: {r.status_code}")
    return None


def crear_grupo_rol_schema(token: str, entorno: str, schema_name: str, account_id: str = ACCOUNT_ID):
    """
    (7) Crea grupo SCIM rol_<entorno>_<schema>_use_schema.
    """
    nombre = f"rol_{entorno.lower()}_{schema_name.lower()}_use_schema"
    return crear_grupo(token, nombre, account_id)


def crear_grupo_rol_catalogo(token: str, entorno: str, dominio_valor: str, account_id: str = ACCOUNT_ID):
    """
    (8) Crea grupo SCIM rol_<entorno>_<dominio3>_use_catalog.
    """
    nombre = f"rol_{entorno.lower()}_{dominio_valor.lower()}_use_catalog"
    return crear_grupo(token, nombre, account_id)


def añadir_grupo_a_grupo(token: str, nombre_grupo_hijo: str, nombre_grupo_padre: str,
                         account_id: str = ACCOUNT_ID) -> bool:
    """
    (9) Anida un grupo dentro de otro grupo.
    """
    # Obtener IDs de los grupos
    hijo = crear_grupo(token, nombre_grupo_hijo, account_id)
    padre = crear_grupo(token, nombre_grupo_padre, account_id)

    url_patch = f"{api_base(account_id)}/Groups/{padre['id']}"
    body = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [{
            "op": "add",
            "path": "members",
            "value": [{"value": hijo["id"], "type": "Group"}]
        }]
    }
    r = requests.patch(url_patch, headers=headers(token), data=json.dumps(body))
    if r.status_code in (200, 204):
        log("✅ Grupo anidado correctamente")
        return True
    log(f"❌ Error anidando grupo: {r.status_code} – {r.text}")
    return False


def conceder_select_tablas(token: str, workspace_url: str,
                           user_input: Union[str, Dict[str, Any]],
                           tablas: List[str]) -> Dict[str, bool]:
    """
    (10) Concede SELECT sobre una lista de tablas Unity Catalog a un usuario.
    """
    email = user_input if isinstance(user_input, str) else user_input.get("email")
    if not email:
        log("❌ conceder_select_tablas: falta email.")
        return {}

    resultados: Dict[str, bool] = {}
    for t in tablas:
        full_name_encoded = quote(t.strip(), safe="")
        url = f"{workspace_url}/api/2.1/unity-catalog/permissions/tables/{full_name_encoded}"
        body = {"changes": [{"principal": email, "add": ["SELECT"]}]}

        r = requests.patch(url, headers={"Authorization": f"Bearer {token}"}, data=json.dumps(body))
        if r.status_code in (200, 201):
            log(f"✅ SELECT concedido en {t} a {email}")
            resultados[t] = True
        else:
            log(f"❌ Error concediendo SELECT en {t}: {r.status_code}")
            resultados[t] = False
    return resultados