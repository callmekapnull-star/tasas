# rates_service.py
# Microservicio FastAPI: consulta Binance P2P y cachea.

import os
import time
import asyncio
import asyncpg
from datetime import datetime
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

from monitor_p2p import fetch_rates
from db_postgres import inicializar_db, guardar_lectura_p2p

# aqui va la URL de la base de datos de Render
DATABASE_URL = os.environ.get("DATABASE_URL")

# aqui va el intervalo de refresco de la caché (segundos)
CACHE_TTL = 30

# aqui va el monto con el que se consulta Binance P2P
MONTO_CONSULTA = 100000

_cache = {
    "ts": 0,
    "data": None,
    "error": None,          # último error (string) o None
    "error_ts": 0,          # cuándo ocurrió
}
_pool = None
_lock = asyncio.Lock()


# ============================================================
#  REFRESCO DE CACHÉ
# ============================================================
async def _refrescar_periodico():
    while True:
        try:
            await _actualizar_cache()
        except Exception as e:
            print(f"[rates_service] error en refresco periódico: {type(e).__name__}: {e}")
        await asyncio.sleep(CACHE_TTL)


async def _actualizar_cache():
    """Consulta Binance y actualiza la caché. No lanza si falla: registra el error."""
    global _pool

    async with _lock:
        loop = asyncio.get_running_loop()
        ahora = datetime.now()

        try:
            data = await loop.run_in_executor(None, fetch_rates, MONTO_CONSULTA)
        except Exception as e:
            _cache["error"] = f"{type(e).__name__}: {e}"
            _cache["error_ts"] = ahora.timestamp()
            print(f"[rates_service] fetch_rates falló: {_cache['error']}")
            raise

        vol_compra = sum(float(a.get("disponible") or 0) for a in data["detalles"]["compra"])
        vol_venta = sum(float(a.get("disponible") or 0) for a in data["detalles"]["venta"])

        _cache["ts"] = ahora.timestamp()
        _cache["data"] = {
            "compra": data["compra"],
            "venta": data["venta"],
            "detalles": data["detalles"],
            "hora": ahora.strftime("%H:%M:%S"),
            "fecha": ahora.strftime("%Y-%m-%d"),
            "vol_compra": vol_compra,
            "vol_venta": vol_venta,
        }
        _cache["error"] = None
        _cache["error_ts"] = 0

        # Persistir en DB (no romper si falla)
        if _pool:
            try:
                await guardar_lectura_p2p(
                    _pool,
                    ahora.date(),
                    ahora.time(),
                    data["compra"],
                    data["venta"],
                    vol_compra,
                    vol_venta,
                )
            except Exception as e:
                print(f"[rates_service] error guardando en DB: {type(e).__name__}: {e}")


# ============================================================
#  LIFESPAN
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool

    # 1) Pool de DB
    try:
        _pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=2, max_size=10, ssl="require"
        )
        await inicializar_db(_pool)
        print("[rates_service] DB lista")
    except Exception as e:
        print(f"[rates_service] no pude inicializar DB: {type(e).__name__}: {e}")
        _pool = None

    # 2) Warm-up: primer fetch antes de aceptar tráfico
    try:
        await _actualizar_cache()
        print("[rates_service] caché inicial poblada")
    except Exception as e:
        print(f"[rates_service] warm-up falló (se reintentará en background): {e}")

    # 3) Refresco periódico en background
    tarea = asyncio.create_task(_refrescar_periodico())

    yield

    tarea.cancel()
    try:
        await tarea
    except asyncio.CancelledError:
        pass
    if _pool:
        await _pool.close()


app = FastAPI(lifespan=lifespan)


# ============================================================
#  ENDPOINTS
# ============================================================
@app.get("/rates")
async def get_rates():
    edad = time.time() - _cache["ts"] if _cache["ts"] else None

    # Si no hay datos o están viejos, intentar refrescar ahora
    if not _cache["data"] or (edad is not None and edad > 60):
        try:
            await _actualizar_cache()
        except Exception as e:
            if _cache["data"]:
                # Devolvemos lo viejo pero marcado como stale
                return JSONResponse({
                    **_cache["data"],
                    "stale": True,
                    "error_actual": f"{type(e).__name__}: {e}",
                })
            # No hay nada: 503 con el error real
            return JSONResponse({
                "error": "no hay datos disponibles",
                "detalle": _cache.get("error") or f"{type(e).__name__}: {e}",
                "hint": (
                    "Verifica en los logs de este servicio si Binance está "
                    "bloqueando la IP (ConnectionError/Timeout/403/451) "
                    "o si RATES_SERVICE_URL del bot apunta a este servicio."
                ),
            }, status_code=503)

    return JSONResponse(_cache["data"])


@app.get("/health")
async def health():
    edad = time.time() - _cache["ts"] if _cache["ts"] else None
    return {
        "ok": True,
        "tiene_datos": _cache["data"] is not None,
        "cache_age_seg": round(edad, 1) if edad is not None else None,
        "ultimo_error": _cache.get("error"),
    }


@app.get("/debug")
async def debug():
    """Endpoint extra para diagnosticar sin adivinar."""
    return {
        "cache_ts": _cache["ts"],
        "cache_age_seg": round(time.time() - _cache["ts"], 1) if _cache["ts"] else None,
        "tiene_datos": _cache["data"] is not None,
        "ultimo_error": _cache.get("error"),
        "error_ts": _cache["error_ts"],
        "cache_sample": {
            "compra": _cache["data"]["compra"] if _cache["data"] else None,
            "venta": _cache["data"]["venta"] if _cache["data"] else None,
        } if _cache["data"] else None,
    }
