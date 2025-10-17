import os
import bcrypt
import requests
import streamlit as st
from pymongo import MongoClient, ASCENDING
from bson import ObjectId
from datetime import datetime

st.set_page_config(page_title="SATisFacture", layout="wide")

API_CONVERT = "http://sat-api-alb-532045601.us-east-1.elb.amazonaws.com/convert-and-upload-certificates/"

def get_db():
    if "mongo_client" not in st.session_state:
        uri = os.getenv("MONGODB_URI", "mongodb+srv://adminBR:EtvimCyx32iSvNbA@basterisreyes.9aj1k.mongodb.net/sat_cfdi?retryWrites=true&w=majority")
        st.session_state.mongo_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        st.session_state.mongo_client.admin.command("ping")
    dbname = os.getenv("MONGODB_DB", "sat_cfdi")
    return st.session_state.mongo_client[dbname]

def ensure_group_collection(db):
    if "grupos" not in db.list_collection_names():
        db.create_collection("grupos")

def ensure_group_indexes(db):
    if "group_indexes_done" in st.session_state:
        return
    ensure_group_collection(db)
    db.grupos.create_index([("slug", ASCENDING)], unique=True)
    st.session_state.group_indexes_done = True

def ensure_user_indexes(db):
    if "user_indexes_done" in st.session_state:
        return
    db.usuarios.create_index([("username", ASCENDING)], unique=True)
    st.session_state.user_indexes_done = True

def ensure_logs_collection(db):
    if "uploads" not in db.list_collection_names():
        db.create_collection("uploads")

def ensure_default_admin(db):
    if not db.usuarios.find_one({"username": "admin"}):
        h = bcrypt.hashpw("admin123".encode(), bcrypt.gensalt()).decode()
        db.usuarios.insert_one({"username": "admin", "password_hash": h, "role": "admin", "active": True, "created_at": datetime.utcnow()})

def slugify(s):
    return "-".join(s.strip().lower().split())

def create_group(db, nombre):
    nombre = (nombre or "").strip()
    if not nombre:
        return False, "Nombre vacío"
    slug = slugify(nombre)
    if db.grupos.find_one({"slug": slug}, {"_id": 1}):
        return False, "El grupo ya existe"
    res = db.grupos.insert_one({"nombre": nombre, "slug": slug, "miembros": [], "creado_en": datetime.utcnow()})
    return True, str(res.inserted_id)

def list_groups(db):
    return list(db.grupos.find({}, {"nombre": 1}).sort("nombre", ASCENDING))

def list_clients(db):
    return list(db.clientes.find({}, {"rfc": 1, "razon_social": 1, "grupo_id": 1}).sort("rfc", ASCENDING))

def clients_without_group(db):
    return list(db.clientes.find({"$or": [{"grupo_id": {"$exists": False}}, {"grupo_id": None}]}, {"rfc": 1, "razon_social": 1}).sort("rfc", ASCENDING))

def clients_in_group(db, group_id):
    return list(db.clientes.find({"grupo_id": ObjectId(group_id)}, {"rfc": 1, "razon_social": 1}).sort("rfc", ASCENDING))

def assign_client_to_group(db, client_id, group_id):
    cli = db.clientes.find_one({"_id": ObjectId(client_id)})
    if not cli:
        return False, "Cliente no encontrado"
    prev_gid = cli.get("grupo_id")
    new_gid = ObjectId(group_id)
    db.clientes.update_one({"_id": cli["_id"]}, {"$set": {"grupo_id": new_gid}})
    if prev_gid and prev_gid != new_gid:
        db.grupos.update_one({"_id": prev_gid}, {"$pull": {"miembros": cli["_id"]}})
    db.grupos.update_one({"_id": new_gid}, {"$addToSet": {"miembros": cli["_id"]}})
    return True, "Asignado"

def remove_client_from_group(db, client_id, group_id):
    cid = ObjectId(client_id)
    gid = ObjectId(group_id)
    db.clientes.update_one({"_id": cid, "grupo_id": gid}, {"$unset": {"grupo_id": ""}})
    db.grupos.update_one({"_id": gid}, {"$pull": {"miembros": cid}})
    return True

def add_clients_to_group(db, group_id, client_ids):
    if not client_ids:
        return 0
    count = 0
    for cid in client_ids:
        ok, _ = assign_client_to_group(db, cid, group_id)
        if ok:
            count += 1
    return count

def delete_group(db, group_id):
    gid = ObjectId(group_id)
    db.clientes.update_many({"grupo_id": gid}, {"$unset": {"grupo_id": ""}})
    db.grupos.delete_one({"_id": gid})
    return True

def group_members(db, group_id):
    gid = ObjectId(group_id)
    return list(db.clientes.find({"grupo_id": gid}, {"rfc": 1, "razon_social": 1}).sort("rfc", ASCENDING))

def create_user(db, username, password, role, group_id=None):
    username = (username or "").strip()
    password = (password or "").strip()
    if not username or not password:
        return False, "Usuario y contraseña requeridos"
    if role not in ("admin", "cliente"):
        return False, "Rol inválido"
    if role == "cliente" and not group_id:
        return False, "Grupo requerido para cliente"
    if db.usuarios.find_one({"username": username}):
        return False, "Usuario ya existe"
    h = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    doc = {"username": username, "password_hash": h, "role": role, "active": True, "created_at": datetime.utcnow()}
    if role == "cliente":
        doc["group_id"] = ObjectId(group_id)
    db.usuarios.insert_one(doc)
    return True, "Usuario creado"

def verify_user(db, username, password):
    user = db.usuarios.find_one({"username": username})
    if not user or not user.get("active", True):
        return False, None
    try:
        ok = bcrypt.checkpw(password.encode(), user["password_hash"].encode())
    except Exception:
        ok = False
    return (ok, user if ok else None)

def log_upload(db, rfc, uploader_username, uploader_name, group_id, status_code, api_payload_summary=None):
    ensure_logs_collection(db)
    db.uploads.insert_one({
        "rfc": rfc,
        "uploader_username": uploader_username,
        "uploader_name": uploader_name,
        "group_id": ObjectId(group_id) if group_id else None,
        "status_code": status_code,
        "api_payload": api_payload_summary,
        "created_at": datetime.utcnow()
    })

if "stage" not in st.session_state:
    st.session_state.stage = "landing"
if "ui_section" not in st.session_state:
    st.session_state.ui_section = "Alta de grupo"
if "filter_group_idx" not in st.session_state:
    st.session_state.filter_group_idx = 0
if "filter_member_idx" not in st.session_state:
    st.session_state.filter_member_idx = 0
if "last_group_idx" not in st.session_state:
    st.session_state.last_group_idx = None
if "clients_loaded" not in st.session_state:
    st.session_state.clients_loaded = False
if "loaded_group_idx" not in st.session_state:
    st.session_state.loaded_group_idx = None
if "members_cache_labels" not in st.session_state:
    st.session_state.members_cache_labels = []
if "members_cache_ids" not in st.session_state:
    st.session_state.members_cache_ids = []
if "selected_client_id" not in st.session_state:
    st.session_state.selected_client_id = None
if "confirmed_client_id" not in st.session_state:
    st.session_state.confirmed_client_id = None
if "confirmed_group_id" not in st.session_state:
    st.session_state.confirmed_group_id = None
if "auth" not in st.session_state:
    st.session_state.auth = False
if "role" not in st.session_state:
    st.session_state.role = None
if "username" not in st.session_state:
    st.session_state.username = None
if "consent_confirmed" not in st.session_state:
    st.session_state.consent_confirmed = False

def go_to_app():
    st.session_state.stage = "app"
    st.rerun()

def go_to_landing():
    st.session_state.stage = "landing"
    st.session_state.auth = False
    st.session_state.role = None
    st.session_state.username = None
    st.session_state.consent_confirmed = False
    st.session_state.pop("consent_cb1", None)
    st.session_state.pop("consent_cb2", None)
    st.rerun()

def view_landing():
    db = get_db()
    ensure_user_indexes(db)
    ensure_default_admin(db)
    col_left, col_center, col_right = st.columns([1, 2, 1])
    with col_center:
        st.markdown("\n\n\n")
        st.title("SATisFacture")
        with st.form("login_form"):
            u = st.text_input("Usuario").strip()
            p = st.text_input("Contraseña", type="password")
            submitted = st.form_submit_button("Entrar")
        if submitted:
            ok, user = verify_user(db, u, p)
            if ok:
                st.session_state.auth = True
                st.session_state.role = user.get("role", "cliente")
                st.session_state.username = user.get("username")
                st.session_state.user_doc = user
                st.session_state.ui_section = "Alta de cliente" if st.session_state.role == "cliente" else "Alta de grupo"
                go_to_app()
            else:
                st.error("Usuario o contraseña inválidos")

def view_app():
    db = get_db()
    ensure_group_indexes(db)
    if st.session_state.role == "cliente":
        st.session_state.ui_section = "Alta de cliente"
    if not st.session_state.auth:
        st.warning("No has iniciado sesión.")
        return

    with st.sidebar:
        st.header("Menú")
        if st.session_state.role == "admin":
            focus = st.session_state.confirmed_client_id is not None
            if not focus:
                st.radio("Secciones", ["Alta de grupo","Alta de cliente","Clientes","Usuarios"], key="ui_section", label_visibility="collapsed")
                st.markdown("---")
                grupos = list_groups(db)
                g_labels = ["— Sin grupo —"] + [g["nombre"] for g in grupos]
                g_ids = ["SIN_GRUPO"] + [str(g["_id"]) for g in grupos]
                g_idx = st.selectbox("Grupo", options=list(range(len(g_ids))), format_func=lambda i: g_labels[i], index=st.session_state.filter_group_idx, key="filter_group_idx")
                if st.session_state.last_group_idx is None:
                    st.session_state.last_group_idx = g_idx
                elif g_idx != st.session_state.last_group_idx:
                    st.session_state.last_group_idx = g_idx
                    st.session_state.clients_loaded = False
                    st.session_state.loaded_group_idx = None
                    st.session_state.members_cache_labels = []
                    st.session_state.members_cache_ids = []
                    st.session_state.filter_member_idx = 0
                    st.session_state.selected_client_id = None
                    st.session_state.confirmed_client_id = None
                    st.session_state.confirmed_group_id = None
                if st.button("Actualizar clientes"):
                    sel_gid = g_ids[g_idx]
                    if sel_gid == "SIN_GRUPO":
                        members = clients_without_group(db)
                    else:
                        members = group_members(db, sel_gid)
                    st.session_state.members_cache_labels = [f'{m.get("rfc")} — {m.get("razon_social") or ""}'.strip() for m in members]
                    st.session_state.members_cache_ids = [str(m["_id"]) for m in members]
                    st.session_state.clients_loaded = True
                    st.session_state.loaded_group_idx = g_idx
                    st.session_state.filter_member_idx = 0
                    st.session_state.selected_client_id = None
                if st.session_state.clients_loaded and st.session_state.loaded_group_idx == g_idx and st.session_state.members_cache_ids:
                    m_labels = ["— Selecciona cliente —"] + st.session_state.members_cache_labels
                    m_ids = [None] + st.session_state.members_cache_ids
                    m_idx = st.selectbox("Cliente", options=list(range(len(m_ids))), format_func=lambda i: m_labels[i], index=st.session_state.filter_member_idx, key="filter_member_idx")
                    st.session_state.selected_client_id = m_ids[m_idx]
                else:
                    st.selectbox("Cliente", options=[0], format_func=lambda _: "— Selecciona cliente —", key="filter_member_idx")
                    st.session_state.selected_client_id = None
                if st.button("Seleccionar"):
                    if st.session_state.clients_loaded and st.session_state.selected_client_id:
                        st.session_state.confirmed_client_id = st.session_state.selected_client_id
                        st.session_state.confirmed_group_id = g_ids[g_idx]
                        st.rerun()
            else:
                cli = db.clientes.find_one({"_id": ObjectId(st.session_state.confirmed_client_id)}, {"rfc": 1, "razon_social": 1})
                title_cli = f'{cli.get("rfc")} — {cli.get("razon_social") or ""}' if cli else "Cliente seleccionado"
                st.success(title_cli)
                if st.button("← Cambiar selección"):
                    st.session_state.confirmed_client_id = None
                    st.session_state.confirmed_group_id = None
                    st.rerun()
            st.markdown("---")
            st.caption(f"Rol: {st.session_state.role} | Usuario: {st.session_state.username}")
            st.button("Cerrar sesión", on_click=go_to_landing)
        else:
            st.markdown("**Sección:** Alta de cliente")
            st.markdown("---")
            st.caption(f"Rol: {st.session_state.role} | Usuario: {st.session_state.username}")
            st.button("Cerrar sesión", on_click=go_to_landing)

    if st.session_state.role == "admin" and st.session_state.confirmed_client_id:
        doc_cli = db.clientes.find_one({"_id": ObjectId(st.session_state.confirmed_client_id)}, {"rfc": 1, "razon_social": 1})
        if doc_cli:
            rfc_sel = doc_cli.get("rfc")
            razon_sel = doc_cli.get("razon_social") or ""
            st.markdown("---")
            st.subheader(f"Cliente seleccionado: {rfc_sel} — {razon_sel}")
            tab1, tab2 = st.tabs(["Solicitudes iniciales", "Visualización"])
            with tab1:
                year = st.number_input("Año objetivo", min_value=2000, max_value=2100, value=datetime.utcnow().year, step=1, key="init_year")

                if st.button("Ejecutar solicitudes iniciales", use_container_width=True, key="btn_ejecutar_iniciales"):
                    payload = {"rfc": rfc_sel, "year": int(year)}
                    try:
                        resp = requests.post(
                            "http://sat-api-alb-532045601.us-east-1.elb.amazonaws.com/ejecutar-solicitudes-iniciales/",
                            json=payload,
                            timeout=(10, 3000)
                        )
                        if resp.status_code >= 400:
                            data = resp.json() if resp.headers.get("content-type","").startswith("application/json") else {"raw": resp.text}
                            st.error("Error al ejecutar solicitudes")
                            st.json(data, expanded=False)
                        else:
                            st.success("Solicitudes enviadas")
                    except requests.exceptions.RequestException as e:
                        st.error(f"Fallo al llamar al backend: {e}")

                st.markdown("---")
                st.subheader("Verificación")

                if st.button("Autenticar y verificar", use_container_width=True, key="btn_auth_verify"):
                    try:
                        with st.spinner("Autenticando con SAT…"):
                            a = requests.post(
                                "http://sat-api-alb-532045601.us-east-1.elb.amazonaws.com/auth-sat/",
                                json={"rfc": rfc_sel},
                                timeout=(10, 120)
                            )
                            a_data = a.json() if a.headers.get("content-type","").startswith("application/json") else {"raw": a.text}
                            if a.status_code >= 400:
                                st.error("Error al autenticar contra SAT")
                                st.json(a_data, expanded=False)
                                st.stop()
                            else:
                                st.success("Token SAT generado")

                        with st.spinner("Verificando solicitudes…"):
                            payload = {"rfc": rfc_sel, "year": int(year)}
                            v = requests.post(
                                "http://sat-api-alb-532045601.us-east-1.elb.amazonaws.com/verificar-solicitudes/",
                                json=payload,
                                timeout=(10, 600)
                            )
                            v_data = v.json() if v.headers.get("content-type","").startswith("application/json") else {"raw": v.text}
                            if v.status_code >= 400:
                                st.error("Error al verificar solicitudes")
                                st.json(v_data, expanded=False)
                            else:
                                items = []
                                if isinstance(v_data, dict):
                                    if isinstance(v_data.get("detalle"), list):
                                        items = v_data["detalle"]
                                    elif isinstance(v_data.get("solicitudes"), list):
                                        items = v_data["solicitudes"]
                                    elif isinstance(v_data.get("items"), list):
                                        items = v_data["items"]

                                if not items:
                                    st.info("Sin resultados para el año seleccionado.")
                                else:
                                    from collections import Counter
                                    estados = []
                                    box = st.container()
                                    for idx, it in enumerate(items, 1):
                                        idsol = it.get("id_solicitud") or it.get("idSolicitud") or it.get("id")
                                        estado = it.get("estado") or it.get("status") or it.get("EstadoSolicitud")
                                        estados.append(str(estado))
                                        paquetes = it.get("paquetes") or it.get("ids_paquetes") or it.get("IdsPaquetes")
                                        npaq = len(paquetes) if isinstance(paquetes, (list, tuple)) else (paquetes if isinstance(paquetes, int) else None)
                                        periodo = it.get("periodo") or (f'{it.get("fecha_inicio","")} → {it.get("fecha_fin","")}' if it.get("fecha_inicio") or it.get("fecha_fin") else None)
                                        line = f"**{idx}.** `{idsol or '—'}` • Estado: **{estado}**"
                                        if npaq is not None:
                                            line += f" • Paquetes: **{npaq}**"
                                        if periodo:
                                            line += f" • {periodo}"
                                        box.markdown(line)
                                    counts = Counter(estados)
                                    st.markdown("---")
                                    st.write({"total": len(items), "por_estado": dict(counts)})

                    except requests.exceptions.RequestException as e:
                        st.error(f"Fallo al llamar al backend: {e}")


            with tab2:
                st.caption("Filtra por año y visualiza CFDI y Metadata del cliente seleccionado")
                col1, col2 = st.columns([1,3])
                with col1:
                    year_v = st.number_input("Año", min_value=2000, max_value=2100, value=datetime.utcnow().year, step=1, key="viz_year")
                    ver_btn = st.button("Actualizar", key="viz_refresh")

                if ver_btn:
                    year_str = str(int(year_v))

                    cfdi_docs = list(
                        db.cfdi.find(
                            {"cliente": rfc_sel},
                            {"xml": 1, "uuid": 1, "cliente": 1, "fechaProcesado": 1}
                        ).sort("fechaProcesado", ASCENDING)
                    )
                    meta_docs = list(
                        db.metadata.find(
                            {"cliente": rfc_sel},
                            {
                                "Uuid": 1, "UUID": 1, "RfcEmisor": 1, "NombreEmisor": 1,
                                "RfcReceptor": 1, "NombreReceptor": 1, "Monto": 1,
                                "EfectoComprobante": 1, "FechaEmision": 1,
                                "FechaCertificacionSat": 1, "Estatus": 1,
                                "cliente": 1, "fechaProcesado": 1
                            }
                        ).sort("fechaProcesado", ASCENDING)
                    )

                    def _g(d, path, default=None):
                        cur = d
                        for k in path:
                            if isinstance(cur, dict) and k in cur:
                                cur = cur[k]
                            else:
                                return default
                        return cur

                    def _year_ok(string_or_date, year_text):
                        if string_or_date is None:
                            return False
                        if isinstance(string_or_date, datetime):
                            return str(string_or_date.year) == year_text
                        s = str(string_or_date)
                        return s.startswith(year_text + "-")

                    cfdi_filtered = []
                    for d in cfdi_docs:
                        fecha_attr = _g(d, ["xml","cfdi:Comprobante","@Fecha"])
                        if _year_ok(fecha_attr, year_str) or _year_ok(d.get("fechaProcesado"), year_str):
                            cfdi_filtered.append(d)

                    meta_filtered = []
                    for m in meta_docs:
                        if _year_ok(m.get("FechaEmision"), year_str) or _year_ok(m.get("fechaProcesado"), year_str):
                            meta_filtered.append(m)

                    cfdi_rows = []
                    for d in cfdi_filtered:
                        xml = d.get("xml") or {}
                        comp = xml.get("cfdi:Comprobante") or {}
                        emisor = xml.get("cfdi:Emisor") or comp.get("cfdi:Emisor") or {}
                        receptor = xml.get("cfdi:Receptor") or comp.get("cfdi:Receptor") or {}
                        compl = xml.get("cfdi:Complemento") or {}
                        timbre = compl.get("tfd:TimbreFiscalDigital") or {}
                        uuid = timbre.get("@UUID") or d.get("uuid")
                        fecha = comp.get("@Fecha")
                        total = comp.get("@Total")
                        tipo = comp.get("@TipoDeComprobante")
                        serie = comp.get("@Serie")
                        folio = comp.get("@Folio")
                        emisor_rfc = emisor.get("@Rfc") or emisor.get("@RFC")
                        receptor_rfc = receptor.get("@Rfc") or receptor.get("@RFC")
                        if emisor_rfc == rfc_sel:
                            sentido = "Emitido"
                        elif receptor_rfc == rfc_sel:
                            sentido = "Recibido"
                        else:
                            sentido = ""
                        cfdi_rows.append({
                            "Fecha": fecha,
                            "UUID": uuid,
                            "Tipo": tipo,
                            "Serie": serie,
                            "Folio": folio,
                            "Total": total,
                            "Emisor": emisor_rfc,
                            "Receptor": receptor_rfc,
                            "Sentido": sentido
                        })

                    meta_rows = []
                    for m in meta_filtered:
                        meta_rows.append({
                            "FechaEmision": m.get("FechaEmision"),
                            "UUID": m.get("Uuid") or m.get("UUID"),
                            "Efecto": m.get("EfectoComprobante"),
                            "Monto": m.get("Monto"),
                            "RfcEmisor": m.get("RfcEmisor"),
                            "RfcReceptor": m.get("RfcReceptor"),
                            "Estatus": m.get("Estatus"),
                            "FechaCertSAT": m.get("FechaCertificacionSat")
                        })

                    col_cfdi, col_meta = st.columns(2)
                    with col_cfdi:
                        st.subheader("CFDI")
                        if cfdi_rows:
                            st.dataframe(cfdi_rows, use_container_width=True, hide_index=True)
                        else:
                            st.info("Sin CFDI para el año seleccionado.")
                    with col_meta:
                        st.subheader("Metadata")
                        if meta_rows:
                            st.dataframe(meta_rows, use_container_width=True, hide_index=True)
                        else:
                            st.info("Sin Metadata para el año seleccionado.")
                else:
                    st.info("Elige un año y pulsa Actualizar para ver datos.")

        st.stop()

    if st.session_state.ui_section == "Alta de grupo" and st.session_state.role == "admin":
        st.title("Alta de grupo")
        col_new, col_manage = st.columns([1, 2])
        with col_new:
            with st.form("form_alta_grupo"):
                nuevo_grupo = st.text_input("Nombre del grupo").strip()
                submitted_g = st.form_submit_button("Guardar grupo")
            if submitted_g:
                ok, res = create_group(db, nuevo_grupo)
                if ok:
                    st.success(f"Grupo creado: {res}")
                else:
                    st.error(res)
        with col_manage:
            grupos = list_groups(db)
            if grupos:
                sel_idx = st.selectbox(
                    "Selecciona grupo",
                    list(range(len(grupos))),
                    format_func=lambda i: grupos[i]["nombre"]
                )
                gid = str(grupos[sel_idx]["_id"])

                # Mostrar miembros actuales
                miembros = list(
                    db.clientes.find({"grupo_id": ObjectId(gid)}, {"rfc": 1, "razon_social": 1}).sort("rfc", ASCENDING)
                )

                if miembros:
                    st.subheader("Miembros actuales")
                    st.dataframe(
                        [{"RFC": m.get("rfc"), "Razón social": m.get("razon_social")} for m in miembros],
                        use_container_width=True,
                        hide_index=True
                    )

                    # Nueva sección: quitar cliente del grupo
                    st.markdown("### Quitar cliente del grupo")
                    member_labels = [f'{m.get("rfc")} — {m.get("razon_social") or ""}' for m in miembros]
                    member_ids = [str(m["_id"]) for m in miembros]

                    sel_remove = st.selectbox(
                        "Selecciona cliente para quitar",
                        options=list(range(len(member_ids))),
                        format_func=lambda i: member_labels[i],
                        key=f"remove_select_{gid}"
                    )

                    if st.button("Quitar del grupo", type="secondary", key=f"remove_btn_{gid}"):
                        ok = remove_client_from_group(db, member_ids[sel_remove], gid)
                        if ok:
                            st.success("Cliente eliminado del grupo")
                            st.rerun()
                else:
                    st.info("Este grupo no tiene miembros.")

                # Agregar nuevos clientes al grupo
                candidatos = clients_without_group(db)
                if candidatos:
                    labels = [f'{c.get("rfc")} — {c.get("razon_social") or ""}'.strip() for c in candidatos]
                    ids = [str(c["_id"]) for c in candidatos]
                    pick = st.multiselect(
                        "Agregar clientes al grupo",
                        options=list(range(len(ids))),
                        format_func=lambda i: labels[i]
                    )
                    if st.button("Agregar al grupo", disabled=(len(pick) == 0), key=f"add_btn_{gid}"):
                        added = add_clients_to_group(db, gid, [ids[i] for i in pick])
                        st.success(f"Agregados: {added}")
                        st.rerun()

                st.markdown("---")
                if st.button("Eliminar grupo", type="secondary", key=f"delete_group_{gid}"):
                    delete_group(db, gid)
                    st.success("Grupo eliminado")
                    st.rerun()
            else:
                st.info("Aún no hay grupos.")


    if st.session_state.ui_section == "Usuarios" and st.session_state.role == "admin":
        st.title("Usuarios")
        with st.form("form_alta_usuario"):
            username = st.text_input("Usuario").strip()
            password = st.text_input("Contraseña", type="password")
            role = st.selectbox("Rol", ["cliente","admin"])
            gid = None
            if role == "cliente":
                grupos = list_groups(db)
                if grupos:
                    gidx = st.selectbox("Grupo", list(range(len(grupos))), format_func=lambda i: grupos[i]["nombre"])
                    gid = str(grupos[gidx]["_id"])
                else:
                    st.info("Primero crea un grupo en 'Alta de grupo'.")
            submitted_u = st.form_submit_button("Crear usuario")
        if submitted_u:
            ok, res = create_user(db, username, password, "admin" if role=="admin" else "cliente", gid if role=="cliente" else None)
            if ok:
                st.success(res)
            else:
                st.error(res)
        users = list(db.usuarios.find({}, {"username":1,"role":1,"active":1,"group_id":1}).sort("username", ASCENDING))
        if users:
            rows = []
            group_map = {str(g["_id"]): g["nombre"] for g in db.grupos.find({}, {"nombre": 1})}
            for u in users:
                gname = group_map.get(str(u.get("group_id"))) if u.get("group_id") else None
                rows.append({"Usuario":u["username"],"Rol":u.get("role"),"Activo":u.get("active",True),"Grupo":gname})
            st.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            st.info("Aún no hay usuarios (aparte del admin).")

    if st.session_state.ui_section == "Alta de cliente":
        st.title("Alta de cliente")
        if st.session_state.role == "cliente":
            user = st.session_state.user_doc
            gid = user.get("group_id")
            if not gid:
                st.error("Tu usuario no tiene grupo asignado.")
            else:
                gdoc = db.grupos.find_one({"_id": ObjectId(gid)}, {"nombre": 1})
                gname = gdoc["nombre"] if gdoc else "(grupo)"
                if not st.session_state.consent_confirmed:
                    st.subheader("Consentimiento previo")
                    notice = f"""
### Aviso de confidencialidad y consentimiento

**Basteris Reyes y Asociados** informa que los archivos de **e.firma (FIEL)** que usted comparta (.cer, .key y contraseña) serán utilizados **exclusivamente** para gestionar descargas de CFDI ante el SAT y realizar el análisis fiscal correspondiente al grupo **{gname}**, incluyendo precios de transferencia y cumplimiento de obligaciones fiscales.

**Seguridad y resguardo.** Los archivos se resguardan en infraestructura en la nube con cifrado en reposo y controles de acceso de mínimo privilegio; el acceso humano está protegido con autenticación multifactor. El contenido solo es accesible para personal autorizado de Basteris Reyes y Asociados y no se comparte con terceros salvo instrucción expresa o fundamento legal aplicable.

**Conservación y eliminación.** Se conservan durante la prestación del servicio y los plazos necesarios para obligaciones fiscales o contractuales. Puede solicitar su eliminación o limitación del tratamiento cuando proceda conforme a la normatividad aplicable.

Al aceptar, usted **reconoce y consiente** el tratamiento descrito.
"""
                    st.markdown(notice)
                    cb1 = st.checkbox("He leído y acepto el aviso de confidencialidad.", key="consent_cb1")
                    cb2 = st.checkbox("Soy consciente del uso y alcances de mis archivos de e.firma para los fines descritos.", key="consent_cb2")
                    if st.button("Aceptar", use_container_width=True, disabled=not (cb1 and cb2)):
                        st.session_state.consent_confirmed = True
                        st.rerun()
                    st.stop()
                with st.form("form_alta_cliente_cliente"):
                    st.text_input("Grupo", value=gname, disabled=True)
                    uploader_name = st.text_input("Tu nombre").strip()
                    rfc = st.text_input("RFC de la empresa").strip().upper()
                    razon = st.text_input("Nombre / Razón social de la empresa (opcional)").strip()
                    cer_file = st.file_uploader("Archivo .cer", type=["cer"], key="cer_up_cli")
                    key_file = st.file_uploader("Archivo .key", type=["key"], key="key_up_cli")
                    pass_file = st.file_uploader("Archivo password.txt", type=["txt","TXT"], key="pass_up_cli")
                    can_submit = all([uploader_name, rfc, cer_file is not None, key_file is not None, pass_file is not None])
                    submitted = st.form_submit_button("Subir certificados")
                if submitted:
                    try:
                        files = {
                            "cer_file": (cer_file.name, cer_file.read(), "application/octet-stream"),
                            "key_file": (key_file.name, key_file.read(), "application/octet-stream"),
                            "password_file": (pass_file.name, pass_file.read(), "text/plain"),
                        }
                        data = {"rfc": rfc}
                        resp = requests.post(API_CONVERT, files=files, data=data, timeout=120)
                        ct = resp.headers.get("content-type","")
                        payload = resp.json() if ct.startswith("application/json") else {"raw": resp.text}
                        if resp.status_code >= 400:
                            st.error("Error al subir certificados")
                            st.json(payload, expanded=False)
                        else:
                            doc = db.clientes.find_one({"rfc": rfc})
                            if doc:
                                updates = {"razon_social": razon or None}
                                if not doc.get("grupo_id"):
                                    updates["grupo_id"] = ObjectId(gid)
                                db.clientes.update_one({"_id": doc["_id"]}, {"$set": updates})
                            st.success("Certificados subidos")
                            doc = db.clientes.find_one({"rfc": rfc})
                            st.json(
                                {
                                    "api": payload,
                                    "consent": {
                                        "accepted": True,
                                        "uploader_username": st.session_state.username,
                                        "uploader_name": uploader_name,
                                        "group": gname,
                                    },
                                    "mongo": {
                                        "found": bool(doc),
                                        "cliente": {
                                            "_id": str(doc["_id"]) if doc else None,
                                            "rfc": (doc.get("rfc") if doc else rfc),
                                            "razon_social": (doc.get("razon_social") if doc else (razon or None)),
                                        },
                                    },
                                },
                                expanded=False,
                            )
                            db.uploads.insert_one({
                                "rfc": rfc,
                                "uploader_username": st.session_state.username,
                                "uploader_name": uploader_name,
                                "group_id": ObjectId(gid),
                                "consent_registered": True,
                                "status_code": resp.status_code,
                                "created_at": datetime.utcnow(),
                            })
                    except requests.exceptions.RequestException as e:
                        st.error(f"Fallo al llamar al backend: {e}")
        else:
            with st.form("form_alta_cliente_admin"):
                rfc = st.text_input("RFC del cliente").strip().upper()
                razon = st.text_input("Nombre / Razón social (opcional)")
                cer_file = st.file_uploader("Archivo .cer", type=["cer"], key="cer_up")
                key_file = st.file_uploader("Archivo .key", type=["key"], key="key_up")
                pass_file = st.file_uploader("Archivo password.txt", type=["txt","TXT"], key="pass_up")
                submitted = st.form_submit_button("Guardar y subir certificados")
            if submitted:
                errs = []
                if not rfc:
                    errs.append("RFC obligatorio.")
                if cer_file is None:
                    errs.append("Falta el archivo .cer.")
                if key_file is None:
                    errs.append("Falta el archivo .key.")
                if pass_file is None:
                    errs.append("Falta el archivo password.txt.")
                if errs:
                    st.error("\n".join(errs))
                else:
                    try:
                        files = {
                            "cer_file": (cer_file.name, cer_file.read(), "application/octet-stream"),
                            "key_file": (key_file.name, key_file.read(), "application/octet-stream"),
                            "password_file": (pass_file.name, pass_file.read(), "text/plain"),
                        }
                        data = {"rfc": rfc}
                        resp = requests.post(API_CONVERT, files=files, data=data, timeout=120)
                        ct = resp.headers.get("content-type","")
                        payload = resp.json() if ct.startswith("application/json") else {"raw": resp.text}
                        if resp.status_code >= 400:
                            st.error("Error al subir certificados")
                            st.json(payload, expanded=False)
                        else:
                            st.success("Certificados subidos")
                            doc = db.clientes.find_one({"rfc": rfc})
                            st.json({"api": payload, "mongo": {"found": bool(doc), "cliente": {"_id": str(doc["_id"]) if doc else None, "rfc": rfc, "razon_social": (doc.get("razon_social") if doc else razon or None)}}}, expanded=False)
                    except requests.exceptions.RequestException as e:
                        st.error(f"Fallo al llamar al backend: {e}")

    if st.session_state.ui_section == "Clientes" and st.session_state.role == "admin":
        st.title("Clientes")
        data = list_clients(db)
        if data:
            group_map = {str(g["_id"]): g["nombre"] for g in db.grupos.find({}, {"nombre": 1})}
            rows = []
            for c in data:
                gid = c.get("grupo_id")
                rows.append({"RFC": c.get("rfc"), "Razón social": c.get("razon_social"), "Grupo": group_map.get(str(gid)) if gid else None})
            st.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            st.info("Aún no hay clientes registrados.")

if "stage" not in st.session_state:
    st.session_state.stage = "landing"

if st.session_state.stage == "landing":
    view_landing()
else:
    view_app()
