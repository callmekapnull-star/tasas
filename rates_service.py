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

_cache = {"ts": 0, "data": None}
_pool = None


async def _refrescar_periodico():
    while True:
        try:
            await _actualizar_cache()
        except Exception as e:
            print(f"[rates_service] error: {e}")
        await asyncio.sleep(CACHE_TTL)


async def _actualizar_cache():
    global _pool
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(None, fetch_rates, 100000)

    ahora = datetime.now()

    vol_compra = sum(float(a["disponible"] or 0) for a in data["detalles"]["compra"])
    vol_venta = sum(float(a["disponible"] or 0) for a in data["detalles"]["venta"])

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
            print(f"[rates_service] error guardando: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    _pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=10, ssl="require"
    )
    await inicializar_db(_pool)
    asyncio.create_task(_refrescar_periodico())
    yield
    await _pool.close()


app = FastAPI(lifespan=lifespan)


@app.get("/rates")
async def get_rates():
    if not _cache["data"] or (time.time() - _cache["ts"]) > 60:
        try:
            await _actualizar_cache()
        except Exception as e:
            if _cache["data"]:
                return JSONResponse({**_cache["data"], "stale": True})
            return JSONResponse({"error": str(e)}, status_code=503)
    return JSONResponse(_cache["data"])


@app.get("/health")
async def health():
    return {"ok": True, "cache_age": time.time() - _cache["ts"]}