# main_bot.py
# Entry point para Render (webhook).

import os
import asyncpg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
from telegram import Update

from bot_telegram import inicializar_aplicacion

# aqui va la URL de la base de datos de Render (inyectada automáticamente)
DATABASE_URL = os.environ.get("DATABASE_URL")

# aqui va la URL pública del bot en Render (Environment Variable: WEBHOOK_URL)
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")

# aqui va el puerto que Render asigna automáticamente
PORT = int(os.environ.get("PORT", 8000))

app = FastAPI()
_application = None
_pool = None


@app.on_event("startup")
async def startup():
    global _application, _pool
    _pool = await asyncpg.create_pool(
        DATABASE_URL, min_size=2, max_size=10, ssl="require"
    )
    _application = await inicializar_aplicacion(_pool)
    await _application.initialize()

    if WEBHOOK_URL:
        await _application.bot.set_webhook(
            url=f"{WEBHOOK_URL}/webhook", drop_pending_updates=True
        )
        print(f"[bot] Webhook configurado: {WEBHOOK_URL}/webhook")


@app.on_event("shutdown")
async def shutdown():
    if _application:
        await _application.shutdown()
    if _pool:
        await _pool.close()


@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, _application.bot)
    await _application.process_update(update)
    return JSONResponse({"ok": True})


@app.get("/")
async def root():
    return {"status": "bot running"}


@app.get("/health")
async def health():
    return {"ok": True}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)