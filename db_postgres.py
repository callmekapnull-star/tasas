# db_postgres.py
# Capa de acceso a PostgreSQL (Render managed).
# Incluye: tasas BCV, lecturas P2P, feriados y configuración dinámica
# (admins, canales, config clave/valor).

import os
import asyncpg
from datetime import datetime

# aqui va la URL de la base de datos de Render (Render la inyecta automáticamente)
DATABASE_URL = os.environ.get("DATABASE_URL")


async def obtener_pool():
    """Crea el pool de conexiones a PostgreSQL."""
    return await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
        ssl="require",
    )


async def inicializar_db(pool):
    """Crea todas las tablas si no existen."""
    async with pool.acquire() as conn:
        await conn.execute("""
            -- ============================================================
            --  TASAS BCV (una por fecha de vigencia)
            -- ============================================================
            CREATE TABLE IF NOT EXISTS tasas_bcv (
                fecha      DATE PRIMARY KEY,
                usd        NUMERIC(12,4) NOT NULL,
                eur        NUMERIC(12,4) NOT NULL,
                creado_en  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- ============================================================
            --  LECTURAS P2P (una por ciclo de consulta a Binance)
            -- ============================================================
            CREATE TABLE IF NOT EXISTS p2p_lecturas (
                id         SERIAL PRIMARY KEY,
                fecha      DATE NOT NULL,
                hora       TIME NOT NULL,
                compra     NUMERIC(12,4) NOT NULL,
                venta      NUMERIC(12,4) NOT NULL,
                vol_compra NUMERIC(18,4),
                vol_venta  NUMERIC(18,4),
                creado_en  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_p2p_lect_fecha
                ON p2p_lecturas(fecha, hora);

            -- ============================================================
            --  FERIADOS (calendario bancario)
            -- ============================================================
            CREATE TABLE IF NOT EXISTS feriados (
                fecha  DATE PRIMARY KEY,
                nombre TEXT NOT NULL,
                tipo   TEXT NOT NULL DEFAULT 'feriado'
            );

            -- ============================================================
            --  ADMINS (múltiples admins para el bot)
            -- ============================================================
            CREATE TABLE IF NOT EXISTS admins (
                user_id      BIGINT PRIMARY KEY,
                username     TEXT,
                agregado_en  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- ============================================================
            --  CANALES (uno marcado como principal)
            -- ============================================================
            CREATE TABLE IF NOT EXISTS canales (
                chat_id      TEXT PRIMARY KEY,
                nombre       TEXT,
                principal    BOOLEAN NOT NULL DEFAULT FALSE,
                agregado_en  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- Solo puede haber un canal principal
            CREATE UNIQUE INDEX IF NOT EXISTS idx_canal_principal_unico
                ON canales (principal) WHERE principal = TRUE;

            -- ============================================================
            --  CONFIG (clave/valor genérico)
            -- ============================================================
            CREATE TABLE IF NOT EXISTS config (
                clave          TEXT PRIMARY KEY,
                valor          TEXT,
                actualizado_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        print("[DB] Tablas listas")


# ============================================================
#  TASAS BCV
# ============================================================
async def guardar_tasa_bcv(pool, fecha, usd, eur):
    """Inserta o actualiza la tasa BCV de una fecha."""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO tasas_bcv (fecha, usd, eur)
            VALUES ($1, $2, $3)
            ON CONFLICT (fecha) DO UPDATE SET
                usd = EXCLUDED.usd,
                eur = EXCLUDED.eur,
                creado_en = NOW()
        """, datetime.strptime(fecha, "%Y-%m-%d").date(), usd, eur)


async def obtener_ultima_tasa_bcv(pool):
    """Devuelve la tasa BCV más reciente."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM tasas_bcv ORDER BY fecha DESC LIMIT 1"
        )
        return dict(row) if row else None


async def obtener_tasa_bcv(pool, fecha):
    """Devuelve la tasa BCV de una fecha exacta."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM tasas_bcv WHERE fecha = $1",
            datetime.strptime(fecha, "%Y-%m-%d").date()
        )
        return dict(row) if row else None


async def obtener_historial_bcv(pool, desde, hasta):
    """Historial BCV entre dos fechas (inclusive)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM tasas_bcv
            WHERE fecha BETWEEN $1 AND $2
            ORDER BY fecha DESC
        """, datetime.strptime(desde, "%Y-%m-%d").date(),
             datetime.strptime(hasta, "%Y-%m-%d").date())
        return [dict(r) for r in rows]


# ============================================================
#  P2P
# ============================================================
async def guardar_lectura_p2p(pool, fecha, hora, compra, venta, vol_compra, vol_venta):
    """Guarda una lectura puntual de Binance P2P."""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO p2p_lecturas
                (fecha, hora, compra, venta, vol_compra, vol_venta)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, fecha, hora, compra, venta, vol_compra, vol_venta)


async def obtener_ultima_lectura_p2p(pool):
    """Devuelve la última lectura P2P guardada."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT * FROM p2p_lecturas
            ORDER BY fecha DESC, hora DESC
            LIMIT 1
        """)
        return dict(row) if row else None


async def obtener_historial_p2p(pool, desde, hasta, limite=30):
    """Historial P2P entre dos fechas (inclusive)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM p2p_lecturas
            WHERE fecha BETWEEN $1 AND $2
            ORDER BY fecha DESC, hora DESC
            LIMIT $3
        """, datetime.strptime(desde, "%Y-%m-%d").date(),
             datetime.strptime(hasta, "%Y-%m-%d").date(), limite)
        return [dict(r) for r in rows]


# ============================================================
#  FERIADOS
# ============================================================
async def guardar_feriados(pool, feriados):
    """feriados: lista de dicts {fecha, nombre, tipo}."""
    async with pool.acquire() as conn:
        for f in feriados:
            await conn.execute("""
                INSERT INTO feriados (fecha, nombre, tipo)
                VALUES ($1, $2, $3)
                ON CONFLICT (fecha) DO UPDATE SET
                    nombre = EXCLUDED.nombre,
                    tipo = EXCLUDED.tipo
            """, datetime.strptime(f["fecha"], "%Y-%m-%d").date(),
                 f["nombre"], f.get("tipo", "feriado"))


async def es_feriado(pool, fecha_iso):
    """Devuelve el feriado si la fecha coincide, o None."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM feriados WHERE fecha = $1",
            datetime.strptime(fecha_iso, "%Y-%m-%d").date()
        )
        return dict(row) if row else None


async def listar_feriados(pool, anio):
    """Lista los feriados de un año."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM feriados
            WHERE EXTRACT(YEAR FROM fecha) = $1
            ORDER BY fecha
        """, anio)
        return [dict(r) for r in rows]


# ============================================================
#  ADMINS
# ============================================================
async def agregar_admin(pool, user_id, username=None):
    """Agrega o actualiza un admin."""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO admins (user_id, username)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
        """, user_id, username)


async def eliminar_admin(pool, user_id):
    """Elimina un admin. Devuelve True si existía."""
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM admins WHERE user_id = $1", user_id)
        return result.endswith("1")


async def listar_admins(pool):
    """Devuelve la lista de admins."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM admins ORDER BY agregado_en")
        return [dict(r) for r in rows]


async def es_admin_db(pool, user_id):
    """Verifica si un user_id es admin consultando la DB."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM admins WHERE user_id = $1", user_id
        )
        return row is not None


# ============================================================
#  CANALES
# ============================================================
async def establecer_canal_principal(pool, chat_id, nombre=None):
    """Marca un canal como principal (desmarca cualquier otro)."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE canales SET principal = FALSE")
            await conn.execute("""
                INSERT INTO canales (chat_id, nombre, principal)
                VALUES ($1, $2, TRUE)
                ON CONFLICT (chat_id) DO UPDATE SET
                    nombre = EXCLUDED.nombre,
                    principal = TRUE
            """, chat_id, nombre)


async def obtener_canal_principal(pool):
    """Devuelve el canal marcado como principal."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM canales WHERE principal = TRUE LIMIT 1"
        )
        return dict(row) if row else None


async def agregar_canal(pool, chat_id, nombre=None, principal=False):
    """Agrega un canal (sin marcarlo necesariamente como principal)."""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO canales (chat_id, nombre, principal)
            VALUES ($1, $2, $3)
            ON CONFLICT (chat_id) DO UPDATE SET
                nombre = EXCLUDED.nombre
        """, chat_id, nombre, principal)


async def eliminar_canal(pool, chat_id):
    """Elimina un canal. Devuelve True si existía."""
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM canales WHERE chat_id = $1", chat_id)
        return result.endswith("1")


async def listar_canales(pool):
    """Lista todos los canales registrados (principal primero)."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM canales ORDER BY principal DESC, agregado_en"
        )
        return [dict(r) for r in rows]


# ============================================================
#  CONFIG (clave/valor genérico)
# ============================================================
async def set_config(pool, clave, valor):
    """Guarda o actualiza un valor de configuración."""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO config (clave, valor)
            VALUES ($1, $2)
            ON CONFLICT (clave) DO UPDATE SET
                valor = EXCLUDED.valor,
                actualizado_en = NOW()
        """, clave, str(valor))


async def get_config(pool, clave, default=None):
    """Obtiene un valor de configuración."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT valor FROM config WHERE clave = $1", clave)
        return row["valor"] if row else default


async def eliminar_config(pool, clave):
    """Elimina una clave de configuración."""
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM config WHERE clave = $1", clave)
        return result.endswith("1")


async def listar_config(pool):
    """Lista toda la configuración."""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM config ORDER BY clave")
        return [dict(r) for r in rows]