# SATisFacture - Frontend Demo (Streamlit)

## Descripción del Proyecto

Frontend de demostración desarrollado en Streamlit para **SATisFacture**, que permite a usuarios y administradores gestionar certificados FIEL, realizar solicitudes al SAT, y visualizar CFDI y metadata de forma intuitiva.

Este frontend sirve como **interfaz de pruebas interna** para validar el flujo completo de operaciones con el backend de integración SAT alojado en AWS ECS.

### Tipo de Proyecto

**Aplicación Web Interactiva** - Demo frontend en Streamlit

### Usuarios Objetivo

1. **Administradores:** Gestión completa de grupos, clientes, usuarios y solicitudes SAT
2. **Clientes:** Subida de certificados FIEL y visualización de sus propios datos fiscales
3. **Equipo de desarrollo:** Pruebas y validación del flujo de integración SAT

## Características Principales

### Para Administradores

- **Gestión de grupos:** Crear, modificar y eliminar grupos de empresas
- **Gestión de clientes:** Asignar RFCs a grupos, administrar certificados FIEL
- **Gestión de usuarios:** Crear usuarios con roles (admin/cliente) y asignarlos a grupos
- **Solicitudes SAT:** Ejecutar solicitudes iniciales automáticas para múltiples años
- **Verificación masiva:** Verificar estado de solicitudes por año
- **Visualización de datos:** Consultar CFDI y metadata por cliente y año

### Para Clientes

- **Consentimiento informado:** Aceptación de términos y condiciones para uso de FIEL
- **Subida de certificados:** Upload de archivos .cer, .key y password.txt
- **Historial de certificados:** Ver certificados subidos por el grupo
- **Validación automática:** Verificación de archivos antes de enviar al backend

## Stack Tecnológico

### Lenguajes y Frameworks

- **Python:** 3.12.9
- **Streamlit:** Framework para aplicaciones web interactivas - [https://streamlit.io/](https://streamlit.io/)

### Bibliotecas Principales

- **streamlit:** Framework principal para la interfaz web
- **bcrypt:** Hashing de contraseñas para autenticación local
- **requests:** Cliente HTTP para comunicación con el backend SAT
- **pymongo:** Driver de MongoDB para acceso directo a base de datos

## Dependencias Externas

### Servicios Requeridos

#### 1. Backend SAT API (AWS ECS)

- **URL Base:** `http://sat-api-alb-532045601.us-east-1.elb.amazonaws.com`
- **Endpoints utilizados:**
  - `/convert-and-upload-certificates/` - Subida de certificados FIEL
  - `/auth-sat/` - Autenticación con el SAT
  - `/ejecutar-solicitudes-iniciales/` - Solicitudes masivas por año
  - `/verificar-solicitudes/` - Verificación de estado de solicitudes

#### 2. Base de Datos MongoDB

- **Tipo:** MongoDB Atlas (cloud)
- **URI:** Configurada en variable de entorno `MONGODB_URI`
- **Base de datos:** Configurada en variable de entorno `MONGODB_DB` (default: `sat_cfdi`)
- **Colecciones utilizadas:**
  - `usuarios` - Autenticación y roles
  - `grupos` - Grupos de empresas
  - `clientes` - RFCs y certificados
  - `uploads` - Log de certificados subidos
  - `cfdi` - Comprobantes fiscales procesados
  - `metadata` - Metadata de comprobantes

## Iniciar el Proyecto

### Instalar Dependencias

```bash
# Instalar dependencias del proyecto
pip install -r requirements.txt
```

**Contenido de `requirements.txt`:**
```
streamlit
bcrypt
requests
pymongo
```

### Configuración de Variables de Entorno

Crear un archivo `.env` en la raíz del proyecto o configurar variables de entorno del sistema:

```bash
# MongoDB
MONGODB_URI=mongodb+srv://usuario:password@cluster.mongodb.net/sat_cfdi?retryWrites=true&w=majority
MONGODB_DB=sat_cfdi
```

**Nota:** Si no se configuran variables de entorno, la aplicación usa valores por defecto hardcodeados (solo para desarrollo).

### Iniciar la Aplicación

#### Modo Local

```bash
# Iniciar servidor Streamlit
streamlit run app.py
```

La aplicación estará disponible en:
- **URL local:** http://localhost:8501
- **URL de red:** Se mostrará en la terminal al iniciar

#### Configuración de Puerto

```bash
# Especificar puerto personalizado
streamlit run app.py --server.port 8080
```

#### Modo Producción

```bash
# Con configuración específica
streamlit run app.py \
    --server.port 8501 \
    --server.address 0.0.0.0 \
    --server.headless true
```

## Arquitectura de la Aplicación

### Estructura de Sesión

La aplicación utiliza `st.session_state` para mantener el estado durante la sesión del usuario:

```python
# Estados principales
st.session_state.stage           # "landing" | "app"
st.session_state.auth             # True | False
st.session_state.role             # "admin" | "cliente"
st.session_state.username         # Nombre de usuario autenticado
st.session_state.consent_confirmed # True | False (solo clientes)

# Estados de interfaz
st.session_state.ui_section       # Sección activa del menú
st.session_state.confirmed_client_id   # Cliente seleccionado (admin)
st.session_state.confirmed_client_rfc  # RFC del cliente seleccionado
st.session_state.sat_token_ready       # Estado de autenticación SAT
```

### Flujo de Navegación

```
Landing (Login)
    ↓
App Principal
    ↓
    ├── ADMIN
    │   ├── Alta de grupo
    │   ├── Alta de cliente
    │   ├── Clientes (listado)
    │   ├── Usuarios (gestión)
    │   └── [Cliente seleccionado]
    │       ├── Solicitudes iniciales
    │       └── Visualización (CFDI/Metadata)
    │
    └── CLIENTE
        └── Alta de cliente (subir FIEL)
```

## Funcionalidades Detalladas

### Sistema de Autenticación

#### Credenciales por Defecto

```
Usuario: admin
Contraseña: admin123
Rol: admin
```

**⚠️ IMPORTANTE:** Cambiar credenciales en producción.

#### Proceso de Login

1. Usuario ingresa credenciales en formulario
2. Sistema verifica contra colección `usuarios` en MongoDB
3. Contraseña se valida con bcrypt
4. Si es válido, se guarda información en `session_state`
5. Redirección a vista correspondiente según rol

### Gestión de Grupos (Admin)

#### Crear Grupo

```
1. Ir a "Alta de grupo"
2. Ingresar nombre del grupo
3. Sistema genera slug automáticamente
4. Click en "Guardar grupo"
```

**Validaciones:**
- Nombre no puede estar vacío
- Slug debe ser único en la base de datos

#### Gestionar Miembros

```
1. Seleccionar grupo existente
2. Ver lista de miembros actuales
3. Opciones:
   - Agregar clientes sin grupo
   - Quitar clientes del grupo
   - Eliminar grupo completo
```

### Gestión de Clientes

#### Alta de Cliente (Admin)

```
1. Ir a "Alta de cliente"
2. Ingresar:
   - RFC (obligatorio, se convierte a mayúsculas)
   - Razón social (opcional)
   - Archivo .cer
   - Archivo .key
   - Archivo password.txt
3. Click en "Guardar y subir certificados"
```

**Proceso backend:**
1. Archivos se envían a `/convert-and-upload-certificates/`
2. Backend convierte .cer/.key a formato .pem
3. Archivos se almacenan en S3
4. Cliente se registra en MongoDB (si no existe)

#### Alta de Cliente (Cliente)

**Flujo con consentimiento:**

```
1. Usuario cliente inicia sesión
2. Sistema verifica grupo asignado
3. Mostrar aviso de confidencialidad
4. Usuario acepta dos checkboxes:
   ✓ He leído y acepto el aviso
   ✓ Soy consciente del uso de mi e.firma
5. Click en "Aceptar"
6. Formulario de subida:
   - Nombre del usuario
   - RFC de la empresa
   - Razón social (opcional)
   - Archivos FIEL
7. Sistema valida que password.txt se llame exactamente así
8. Upload y registro en base de datos
```

**Validaciones especiales:**
- No se puede subir el mismo RFC dos veces por un usuario
- Archivo de contraseña debe llamarse exactamente `password.txt`
- Si el usuario ya subió certificados, puede subir otros para RFCs diferentes

### Solicitudes SAT (Admin con Cliente Seleccionado)

#### Ejecutar Solicitudes Iniciales

```
1. Seleccionar cliente desde sidebar
2. Ir a tab "Solicitudes iniciales"
3. Seleccionar año objetivo
4. Click en "Ejecutar solicitudes iniciales"
```

**Proceso:**
- Envía solicitud al backend para generar automáticamente:
  - CFDI Emitidos: 12 solicitudes (1 por mes)
  - CFDI Recibidos: 12 solicitudes (1 por mes)
  - Metadata Emitidos: 2 solicitudes (semestral)
  - Metadata Recibidos: 2 solicitudes (semestral)
- Total: 28 solicitudes por año
- También genera solicitudes para año-1 y primeros 3 meses de año+1

**Tiempo de ejecución:** ~10 minutos

La interfaz usa `ThreadPoolExecutor` para no bloquear la UI durante la espera.

#### Verificar Solicitudes

**Prerequisito:** Autenticación SAT

```
1. Click en "Autenticar con SAT"
2. Esperar confirmación
3. Click en "Verificar solicitudes"
4. Sistema consulta estado de todas las solicitudes del año
```

**Tabla de resultados muestra:**
- ID de solicitud
- Estado (1-6)
- Código de respuesta SAT
- Mensaje
- Número de CFDIs
- Número de paquetes disponibles

### Visualización de Datos

#### Ver CFDI y Metadata

```
1. Con cliente seleccionado
2. Ir a tab "Visualización"
3. Seleccionar año
4. Click en "Actualizar"
```

**Tabla CFDI muestra:**
- Fecha de emisión
- UUID
- Tipo de comprobante
- Serie y Folio
- Total
- Emisor y Receptor
- Sentido (Emitido/Recibido)

**Tabla Metadata muestra:**
- Fecha de emisión
- UUID
- Efecto (I/E/P)
- Monto
- RFCs emisor y receptor
- Estatus (1=Vigente, 0=Cancelado)
- Fecha de certificación SAT

**Filtrado:**
- Solo muestra documentos del año seleccionado
- Verifica tanto `FechaEmision` como `fechaProcesado`

## Estructura del Código

### Funciones Principales

#### Base de Datos

```python
get_db()                          # Obtiene conexión MongoDB
ensure_group_collection(db)       # Crea colección grupos si no existe
ensure_group_indexes(db)          # Crea índices únicos
ensure_user_indexes(db)           # Crea índices de usuarios
ensure_logs_collection(db)        # Crea colección uploads
ensure_default_admin(db)          # Crea usuario admin por defecto
```

#### Gestión de Grupos

```python
create_group(db, nombre)                      # Crear nuevo grupo
list_groups(db)                                # Listar todos los grupos
group_members(db, group_id)                   # Miembros de un grupo
assign_client_to_group(db, client_id, group_id) # Asignar cliente
remove_client_from_group(db, client_id, group_id) # Quitar cliente
delete_group(db, group_id)                    # Eliminar grupo
```

#### Gestión de Clientes

```python
list_clients(db)                  # Listar todos los clientes
clients_without_group(db)         # Clientes sin grupo asignado
clients_in_group(db, group_id)   # Clientes de un grupo específico
```

#### Gestión de Usuarios

```python
create_user(db, username, password, role, group_id) # Crear usuario
verify_user(db, username, password)                  # Validar login
```

#### Logging

```python
log_upload(db, rfc, uploader_username, uploader_name, group_id, status_code) 
# Registra subidas de certificados
```

#### Navegación

```python
go_to_app()        # Cambiar a vista de aplicación
go_to_landing()    # Volver a login (logout)
view_landing()     # Renderizar pantalla de login
view_app()         # Renderizar aplicación principal
```

## Seguridad

### Implementado

- **Hashing de contraseñas:** bcrypt con salt automático
- **Autenticación de sesión:** Estado guardado en `session_state`
- **Roles y permisos:** Admin vs Cliente con vistas diferenciadas
- **Validación de archivos:** Nombres y tipos de archivo
- **Consentimiento informado:** Aceptación explícita para clientes
- **Logging de operaciones:** Registro de uploads en BD

### Limitaciones Conocidas

- No hay JWT - autenticación solo vive en sesión de Streamlit
- Conexión directa a MongoDB - no hay capa de API para datos
- Credenciales de admin hardcodeadas en código
- Sin rate limiting para prevenir abuso
- Sin 2FA para usuarios admin

### Mejoras de Seguridad (Roadmap)

- Implementar JWT para autenticación persistente
- Mover acceso a MongoDB detrás de API
- Sistema de gestión de credenciales de admin
- 2FA para usuarios admin
- Logs de auditoría para operaciones sensibles
- Rate limiting por usuario
- Timeout de sesión automático

## Integración con Backend

### Endpoints Utilizados

#### 1. Convertir y Subir Certificados

```python
POST /convert-and-upload-certificates/

# Payload (multipart/form-data)
{
    "rfc": "RFC123456789",
    "cer_file": <archivo .cer>,
    "key_file": <archivo .key>,
    "password_file": <archivo password.txt>
}

# Response (200 OK)
{
    "status": "success",
    "message": "FIEL almacenada correctamente"
}
```

#### 2. Autenticar con SAT

```python
POST /auth-sat/

# Payload (form-data)
{
    "rfc": "RFC123456789"
}

# Response (200 OK)
{
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

#### 3. Ejecutar Solicitudes Iniciales

```python
POST /ejecutar-solicitudes-iniciales/

# Payload (form-data)
{
    "rfc": "RFC123456789",
    "year": 2024
}

# Response (200 OK)
{
    "status": "success",
    "solicitudes_creadas": 28,
    "detalle": [...]
}
```

**Timeout:** Este endpoint puede tardar ~10 minutos. El frontend usa `timeout=None`.

#### 4. Verificar Solicitudes

```python
POST /verificar-solicitudes/

# Payload (form-data)
{
    "rfc": "RFC123456789",
    "year": 2024
}

# Response (200 OK)
{
    "message": "Verificacion completada",
    "resultados": [
        {
            "id_solicitud": "uuid",
            "estado": "3",
            "codigo_estados": "5000",
            "numero_cfdis": "150",
            "paquetes": ["paquete_01", "paquete_02"]
        }
    ]
}
```

### Manejo de Errores

```python
# Timeout personalizado
requests.post(url, data=payload, timeout=(10, 120))
# (10 segundos para conectar, 120 segundos para respuesta)

# Sin timeout (operaciones largas)
requests.post(url, data=payload, timeout=None)

# Manejo de errores
try:
    response = requests.post(url, data=payload)
    if response.status_code >= 400:
        st.error("Error en la operación")
        st.json(response.json(), expanded=False)
    else:
        st.success("Operación exitosa")
except requests.exceptions.RequestException as e:
    st.error(f"Fallo al conectar: {e}")
```

## Troubleshooting

### Problemas Comunes

#### 1. Error: "No module named 'streamlit'"

**Causa:** Dependencias no instaladas

**Solución:**
```bash
pip install -r requirements.txt
```

#### 2. Error: "ServerSelectionTimeoutError"

**Causa:** No se puede conectar a MongoDB

**Solución:**
```bash
# Verificar URI de MongoDB
echo $MONGODB_URI

# Probar conectividad
mongosh "mongodb+srv://..."

# Verificar whitelist de IP en MongoDB Atlas
```

#### 3. Error: "Connection refused" al llamar backend

**Causa:** Backend no está disponible o URL incorrecta

**Solución:**
```bash
# Verificar que el backend esté corriendo
curl http://sat-api-alb-532045601.us-east-1.elb.amazonaws.com/health

# Verificar logs de ECS si está en AWS
aws ecs list-tasks --cluster sat-api --service-name sat-api-service
```

#### 4. Sesión se pierde al recargar página

**Causa:** Comportamiento normal de Streamlit

**Solución:**
- No recargar la página manualmente (F5)
- Usar botones de navegación de la app
- En producción, implementar JWT para persistencia

#### 5. "Este grupo no tiene miembros" pero sí hay clientes

**Causa:** Cache de sidebar desactualizado

**Solución:**
```
1. Click en "Actualizar clientes" en el sidebar
2. O cambiar de grupo y volver
```

#### 6. Error al verificar: "Internal Server Error"

**Causa:** No existen solicitudes para el año seleccionado

**Solución:**
- Primero ejecutar "Solicitudes iniciales" para ese año
- Esperar a que se completen las solicitudes
- Luego intentar verificar

## Deployment

### Local (Desarrollo)

```bash
# 1. Clonar repositorio
git clone <repo-url>
cd satisfacture-frontend

# 2. Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar variables de entorno
export MONGODB_URI="mongodb+srv://..."
export MONGODB_DB="sat_cfdi"

# 5. Ejecutar
streamlit run app.py
```

### Docker (Producción)

```dockerfile
# Dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

```bash
# Build y run
docker build -t satisfacture-frontend .
docker run -p 8501:8501 \
    -e MONGODB_URI="mongodb+srv://..." \
    -e MONGODB_DB="sat_cfdi" \
    satisfacture-frontend
```

### Streamlit Cloud

```toml
# .streamlit/config.toml
[server]
headless = true
port = 8501

[browser]
gatherUsageStats = false
```

```toml
# secrets.toml (no incluir en git)
MONGODB_URI = "mongodb+srv://..."
MONGODB_DB = "sat_cfdi"
```

## Limitaciones Conocidas

### Técnicas

1. **Sin persistencia de sesión:** Al recargar página se pierde sesión
2. **Conexión directa a BD:** No hay capa de abstracción API
3. **Sin paginación:** Todas las consultas traen todos los registros
4. **Sin caché:** Cada interacción consulta BD nuevamente
5. **Timeouts largos:** Solicitudes iniciales pueden tardar 10+ minutos

### Funcionales

1. **Sin edición de grupos:** No se puede renombrar un grupo
2. **Sin edición de clientes:** No se puede modificar RFC o razón social
3. **Sin eliminación de clientes:** No hay opción de borrar clientes
4. **Sin búsqueda:** No hay filtros de búsqueda en listados
5. **Sin exportación:** No se pueden exportar datos a CSV/Excel

### UX

1. **Recarga completa:** Streamlit recarga la página completa en cada interacción
2. **Sin confirmaciones:** Operaciones destructivas no piden confirmación
3. **Sin feedback visual:** Algunas operaciones no muestran spinner de carga
4. **Sin breadcrumbs:** No hay indicador de ubicación en la app
5. **Sin historial:** No se puede volver atrás sin perder estado

## Roadmap

### Corto Plazo (1-2 meses)

- [ ] Implementar confirmaciones para operaciones destructivas
- [ ] Agregar búsqueda y filtros en listados
- [ ] Mejorar feedback visual con spinners y progress bars
- [ ] Implementar paginación para grandes volúmenes de datos
- [ ] Agregar exportación a CSV/Excel

### Mediano Plazo (3-6 meses)

- [ ] Migrar a React/Next.js para mejor UX
- [ ] Implementar JWT para persistencia de sesión
- [ ] Agregar edición de grupos y clientes
- [ ] Sistema de notificaciones en tiempo real
- [ ] Dashboard con métricas y gráficas

### Largo Plazo (6+ meses)

- [ ] Aplicación móvil nativa
- [ ] Chat bot integrado
- [ ] IA para insights automáticos
- [ ] Sistema de alertas y notificaciones
- [ ] Multi-idioma (español/inglés)

## Contacto y Soporte

### Desarrollador

- **Nombre:** César
- **Rol:** Data Engineer
- **Email:** apps@basterisreyes.com

### Reportar Problemas

Para reportar bugs o solicitar funcionalidades:

1. Describir el problema o solicitud
2. Incluir pasos para reproducir (si es bug)
3. Adjuntar capturas de pantalla si aplica
4. Especificar navegador y versión de Python

## Notas Importantes

### Solo para Desarrollo

Esta aplicación es una **demo interna** y no está diseñada para producción a gran escala. Para uso en producción, se recomienda:

1. Migrar a framework frontend más robusto (React/Next.js)
2. Implementar autenticación con JWT
3. Mover acceso a BD detrás de API REST
4. Implementar logging y monitoreo profesional
5. Agregar tests automatizados

### Seguridad

- No usar en producción sin cambiar credenciales de admin
- No exponer a internet público sin autenticación adicional
- Considerar implementar rate limiting
- Auditar logs de acceso regularmente

### Performance

- Optimizar consultas a MongoDB con índices apropiados
- Implementar caché para datos que no cambian frecuentemente
- Considerar paginación para listados grandes
- Monitorear uso de memoria con múltiples usuarios concurrentes

---

**SATisFacture Frontend** - Demo interactiva para gestión fiscal automatizada 🇲🇽


*Última actualización: Noviembre 2025*
