# bot_telegram.py
# Bot de Telegram para Render + PostgreSQL.
# Incluye: bootstrap de admins/canal desde env vars, caché en memoria,
#          comandos públicos de cálculo y comandos de admin.

import os
import json
import time
import math
import asyncio
import aiohttp
from io import BytesIO
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont
from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, ContextTypes

from db_postgres import (
    inicializar_db,
    guardar_tasa_bcv,
    obtener_ultima_tasa_bcv,
    obtener_historial_bcv,
    # admins
    agregar_admin,
    eliminar_admin,
    listar_admins,
    # canales
    establecer_canal_principal,
    obtener_canal_principal,
    listar_canales,
    eliminar_canal,
    # config
    set_config,
    get_config,
    listar_config,
)
from monitor_p2p import consultar_tasas_bcv, fetch_rates

# ============================================================
#  CONFIGURACIÓN (Environment Variables de Render)
# ============================================================
# aqui va el token del bot (Environment Variable: BOT_TOKEN)
TOKEN = os.environ["BOT_TOKEN"]

# aqui va el admin inicial (solo se usa en el primer arranque)
ADMIN_ID_INICIAL = int(os.environ["ADMIN_ID"])

# aqui va el canal inicial (solo se usa en el primer arranque)
CANAL_INICIAL = os.environ.get("CANAL", "@BancaYDivisaVe")

# aqui va la URL pública del bot (Render la asigna)
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")

# aqui van las comisiones
COMISION_INTERVENCION = 0.005
COMISION_MENUDEO = 0.012
COOLDOWN_SEGUNDOS = 2
INTERVALO_BUSQUEDA = 300
ARCHIVO_PLANTILLA = "plantilla.jpg"

# aqui va el monto con el que se consulta Binance P2P (en VES)
MONTO_BINANCE = 100000

# ============================================================
#  ESTADO EN MEMORIA
# ============================================================
tasas_actuales = {
    "usd": None,
    "eur": None,
    "fecha": None,
    "usd_anterior": None,
}

ultimo_uso = {}
procesando = asyncio.Lock()
_pool = None

# aqui va la caché de configuración (se puebla al arrancar y al cambiar)
_config_cache = {
    "admins": set(),       # set de user_ids
    "canal": None,         # chat_id del canal principal
    "canal_nombre": None,  # nombre legible
}


def set_pool(pool):
    global _pool
    _pool = pool


# ============================================================
#  BOOTSTRAP Y CACHÉ
# ============================================================
async def bootstrap_config(pool):
    """Puebla admins/canales con los valores iniciales si están vacíos."""
    admins = await listar_admins(pool)
    if not admins:
        await agregar_admin(pool, ADMIN_ID_INICIAL, username="admin_inicial")
        print(f"[bootstrap] Admin inicial agregado: {ADMIN_ID_INICIAL}")

    canal = await obtener_canal_principal(pool)
    if not canal:
        await establecer_canal_principal(pool, CANAL_INICIAL, nombre=CANAL_INICIAL)
        print(f"[bootstrap] Canal inicial agregado: {CANAL_INICIAL}")


async def recargar_config(pool):
    """Recarga la caché de admins y canal desde la DB."""
    admins = await listar_admins(pool)
    _config_cache["admins"] = {a["user_id"] for a in admins}

    canal = await obtener_canal_principal(pool)
    _config_cache["canal"] = canal["chat_id"] if canal else None
    _config_cache["canal_nombre"] = canal["nombre"] if canal else None


def es_admin(user_id):
    """Verifica si un user_id está en la caché de admins."""
    return user_id in _config_cache["admins"]


def canal_actual():
    """Devuelve el chat_id del canal principal."""
    return _config_cache["canal"]


# ============================================================
#  UTILIDADES
# ============================================================
def redondear(numero):
    return float(Decimal(str(numero)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def formatear_numero(numero):
    numero = redondear(numero)
    return f"{numero:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fecha_espanol(fecha_str=None):
    dias = {
        "Monday": "LUNES", "Tuesday": "MARTES", "Wednesday": "MIÉRCOLES",
        "Thursday": "JUEVES", "Friday": "VIERNES", "Saturday": "SÁBADO",
        "Sunday": "DOMINGO",
    }
    meses = {
        "January": "Enero", "February": "Febrero", "March": "Marzo",
        "April": "Abril", "May": "Mayo", "June": "Junio", "July": "Julio",
        "August": "Agosto", "September": "Septiembre", "October": "Octubre",
        "November": "Noviembre", "December": "Diciembre",
    }
    if fecha_str:
        anio, mes, dia = fecha_str.split("-")
        fecha = datetime(int(anio), int(mes), int(dia))
    else:
        fecha = datetime.now()
    return f"{dias[fecha.strftime('%A')]}, {int(fecha.day)} de {meses[fecha.strftime('%B')]} {fecha.year}"


def fecha_imagen(fecha_str=None):
    dias = {
        "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
        "Thursday": "Jueves", "Friday": "Viernes", "Saturday": "Sábado",
        "Sunday": "Domingo",
    }
    meses = {
        "January": "Enero", "February": "Febrero", "March": "Marzo",
        "April": "Abril", "May": "Mayo", "June": "Junio", "July": "Julio",
        "August": "Agosto", "September": "Septiembre", "October": "Octubre",
        "November": "Noviembre", "December": "Diciembre",
    }
    if fecha_str:
        anio, mes, dia = fecha_str.split("-")
        fecha = datetime(int(anio), int(mes), int(dia))
    else:
        fecha = datetime.now()
    return f"{dias[fecha.strftime('%A')]}, {int(fecha.day)} De {meses[fecha.strftime('%B')]} {fecha.year}"


# ============================================================
#  TASAS BCV
# ============================================================
def tasa_oficial_usd():
    return redondear(tasas_actuales["usd"])


def tasa_oficial_eur():
    return redondear(tasas_actuales["eur"])


def tasa_intervencion_usd():
    return redondear(tasa_oficial_usd() * (1 + COMISION_INTERVENCION))


def tasa_menudeo_usd():
    return redondear(tasa_oficial_usd() * (1 + COMISION_MENUDEO))


def tasa_intervencion_eur():
    return redondear(tasa_oficial_eur() * (1 + COMISION_INTERVENCION))


def tasa_menudeo_eur():
    return redondear(tasa_oficial_eur() * (1 + COMISION_MENUDEO))


async def cargar_tasas_desde_db():
    global tasas_actuales
    if _pool is None:
        return
    ultima = await obtener_ultima_tasa_bcv(_pool)
    if ultima:
        tasas_actuales["usd"] = float(ultima["usd"])
        tasas_actuales["eur"] = float(ultima["eur"])
        tasas_actuales["fecha"] = ultima["fecha"].strftime("%Y-%m-%d")
        print(f"[DB] Tasas cargadas: USD={ultima['usd']} EUR={ultima['eur']}")


async def aplicar_nuevas_tasas_async(nuevas):
    global tasas_actuales
    if tasas_actuales["usd"] is not None and tasas_cambiaron(nuevas):
        tasas_actuales["usd_anterior"] = tasas_actuales["usd"]

    tasas_actuales["usd"] = nuevas["usd"]
    tasas_actuales["eur"] = nuevas["eur"]
    tasas_actuales["fecha"] = nuevas["fecha"]

    if _pool is not None:
        await guardar_tasa_bcv(_pool, nuevas["fecha"], nuevas["usd"], nuevas["eur"])


def tasas_cambiaron(nuevas):
    if tasas_actuales["usd"] is None:
        return True
    return (
        abs(nuevas["usd"] - tasas_actuales["usd"]) > 0.0001
        or abs(nuevas["eur"] - tasas_actuales["eur"]) > 0.0001
        or nuevas["fecha"] != tasas_actuales["fecha"]
    )


def texto_variacion_usd():
    if tasas_actuales["usd"] is None or tasas_actuales.get("usd_anterior") is None:
        return None
    actual = tasa_oficial_usd()
    anterior = redondear(tasas_actuales["usd_anterior"])
    if anterior == 0:
        return None
    diferencia = redondear(actual - anterior)
    porcentaje = redondear(((actual - anterior) / anterior) * 100)
    if diferencia > 0:
        return (
            f"📈 *Aumento:* {formatear_numero(diferencia)} Bs\n"
            f"📊 *Variación:* +{formatear_numero(porcentaje)}%"
        )
    if diferencia < 0:
        return (
            f"📉 *Bajada:* {formatear_numero(abs(diferencia))} Bs\n"
            f"📊 *Variación:* {formatear_numero(porcentaje)}%"
        )
    return "➖ *Sin variación* respecto a la tasa anterior"


def verificar_cooldown(user_id):
    ahora = time.time()
    if user_id in ultimo_uso:
        restante = COOLDOWN_SEGUNDOS - (ahora - ultimo_uso[user_id])
        if restante > 0:
            return False, math.ceil(restante)
    ultimo_uso[user_id] = ahora
    return True, 0


# ============================================================
#  IMAGEN DE TASA
# ============================================================
def _fuente(size, bold=False):
    candidatos = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold
        else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf" if bold
        else "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]
    for ruta in candidatos:
        if os.path.exists(ruta):
            return ImageFont.truetype(ruta, size)
    return ImageFont.load_default()


def generar_imagen_tasa():
    if not os.path.exists(ARCHIVO_PLANTILLA):
        raise FileNotFoundError("No encuentro plantilla.jpg")

    base = Image.open(ARCHIVO_PLANTILLA).convert("RGB")
    sx = base.width / 896
    sy = base.height / 1195
    draw = ImageDraw.Draw(base)

    def caja(x1, y1, x2, y2):
        return (int(x1 * sx), int(y1 * sy), int(x2 * sx), int(y2 * sy))

    caja_usd = caja(280, 620, 810, 765)
    caja_eur = caja(280, 800, 810, 940)
    caja_fecha = caja(90, 965, 810, 1055)

    escala = min(sx, sy)
    fuente_monto = _fuente(int(52 * escala), True)
    fuente_fecha = _fuente(int(28 * escala), True)
    color_texto = (72, 22, 28)

    def texto_centro(text, box, font, fill):
        x1, y1, x2, y2 = box
        bbox = draw.textbbox((0, 0), text, font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        x = x1 + (x2 - x1 - tw) // 2
        y = y1 + (y2 - y1 - th) // 2 - 2
        draw.text((x, y), text, font=font, fill=fill)

    texto_centro(f"{formatear_numero(tasa_oficial_usd())} Bs", caja_usd, fuente_monto, color_texto)
    texto_centro(f"{formatear_numero(tasa_oficial_eur())} Bs", caja_eur, fuente_monto, color_texto)
    texto_centro(fecha_imagen(tasas_actuales["fecha"]), caja_fecha, fuente_fecha, color_texto)

    bio = BytesIO()
    base.save(bio, format="PNG")
    bio.seek(0)
    return bio


def texto_tasas_canal():
    base = (
        f"*BANCA & DIVISA*\n\n"
        f"*Tasa oficial del Banco Central*\n"
        f"{fecha_espanol(tasas_actuales['fecha'])}\n\n"
        f"Dólar (USD)   {formatear_numero(tasa_oficial_usd())} Bs\n"
        f"Euro  (EUR)   {formatear_numero(tasa_oficial_eur())} Bs"
    )
    var = texto_variacion_usd()
    if var:
        base += f"\n\n{var}"
    base += f"\n\n@BancaYDivisaVe\n¡La información, a tu alcance!"
    return base


async def publicar_en_canal(bot, tambien_privado=False):
    if tasas_actuales["usd"] is None:
        return False
    canal = canal_actual()
    if not canal:
        print("[publicar] No hay canal principal configurado")
        return False
    try:
        foto = generar_imagen_tasa()
        await bot.send_photo(
            chat_id=canal,
            photo=InputFile(foto, filename="tasa_bcv.png"),
            caption=texto_tasas_canal(),
            parse_mode="Markdown",
        )
        if tambien_privado:
            foto2 = generar_imagen_tasa()
            await bot.send_photo(
                chat_id=ADMIN_ID_INICIAL,
                photo=InputFile(foto2, filename="tasa_bcv.png"),
                caption="Vista previa (también se publicó en el canal)",
                parse_mode="Markdown",
            )
        return True
    except Exception as e:
        print(f"Error publicando en el canal: {e}")
        return False


# ============================================================
#  COMANDOS PÚBLICOS
# ============================================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    es_adm = es_admin(update.effective_user.id)

    mensaje = (
        "*BANCA & CALCULOS*\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "Herramienta de cálculo con tasa BCV.\n\n"
        "*Comandos públicos:*\n"
        "`/usd 10` — Dólares → bolívares\n"
        "`/eur 10` — Euros → bolívares\n"
        "`/bs 10000` — Bolívares → dólares\n"
        "`/bcv 100` — Sin comisiones\n"
        "`/usdt` — Binance P2P\n"
        "`/tasas` — Tasas del día\n"
        "`/historial AAAA-MM-DD AAAA-MM-DD` — Historial BCV\n"
    )
    if es_adm:
        mensaje += (
            "\n*Comandos de admin:*\n"
            "`/config` — ver configuración\n"
            "`/admins` — listar admins\n"
            "`/addadmin <id> [nombre]` — agregar admin\n"
            "`/deladmin <id>` — eliminar admin\n"
            "`/canal` — ver canal actual\n"
            "`/canal @nuevo` — cambiar canal\n"
            "`/canales` — listar canales\n"
            "`/reload` — actualizar tasas\n"
            "`/buscar` — activar búsqueda de tasa\n"
            "`/montos` — publicar tabla de montos\n"
        )

    await update.message.reply_text(mensaje, parse_mode="Markdown")


async def ayuda(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


async def comando_usd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    permitido, espera = verificar_cooldown(user_id)
    if not permitido:
        await update.message.reply_text(f"Espera *{espera}* segundos.", parse_mode="Markdown")
        return
    if tasas_actuales["usd"] is None:
        await update.message.reply_text("Las tasas no están disponibles.")
        return
    if not context.args:
        await update.message.reply_text("Formato: `/usd 10`", parse_mode="Markdown")
        return
    try:
        monto = float(context.args[0].replace(",", "."))
        if monto <= 0:
            raise ValueError
    except Exception:
        await update.message.reply_text("Monto no válido.\nEjemplo: `/usd 10`", parse_mode="Markdown")
        return

    async with procesando:
        tasa = tasa_oficial_usd()
        tasa_id = tasa_intervencion_usd()
        tasa_men = tasa_menudeo_usd()
        respuesta = (
            f"*BANCA & CALCULOS*\n\n"
            f"*Tasa oficial USD*\n{formatear_numero(tasa)} Bs\n\n"
            f"*Monto solicitado*\n{formatear_numero(monto)} USD\n\n"
            f"────────────────\n"
            f"*Intervención 0,5%*\n"
            f"{formatear_numero(tasa_id)} → {formatear_numero(monto * tasa_id)} Bs\n\n"
            f"*Menudeo 1,2%*\n"
            f"{formatear_numero(tasa_men)} → {formatear_numero(monto * tasa_men)} Bs\n"
            f"────────────────\n\n"
            f"{fecha_espanol()}"
        )
        await update.message.reply_text(respuesta, parse_mode="Markdown")


async def comando_eur(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    permitido, espera = verificar_cooldown(user_id)
    if not permitido:
        await update.message.reply_text(f"Espera *{espera}* segundos.", parse_mode="Markdown")
        return
    if tasas_actuales["eur"] is None:
        await update.message.reply_text("Las tasas no están disponibles.")
        return
    if not context.args:
        await update.message.reply_text("Formato: `/eur 10`", parse_mode="Markdown")
        return
    try:
        monto = float(context.args[0].replace(",", "."))
        if monto <= 0:
            raise ValueError
    except Exception:
        await update.message.reply_text("Monto no válido.\nEjemplo: `/eur 10`", parse_mode="Markdown")
        return

    async with procesando:
        tasa = tasa_oficial_eur()
        tasa_id = tasa_intervencion_eur()
        tasa_men = tasa_menudeo_eur()
        respuesta = (
            f"*BANCA & CALCULOS*\n\n"
            f"*Tasa oficial EUR*\n{formatear_numero(tasa)} Bs\n\n"
            f"*Monto solicitado*\n{formatear_numero(monto)} EUR\n\n"
            f"────────────────\n"
            f"*Intervención 0,5%*\n"
            f"{formatear_numero(tasa_id)} → {formatear_numero(monto * tasa_id)} Bs\n\n"
            f"*Menudeo 1,2%*\n"
            f"{formatear_numero(tasa_men)} → {formatear_numero(monto * tasa_men)} Bs\n"
            f"────────────────\n\n"
            f"{fecha_espanol()}"
        )
        await update.message.reply_text(respuesta, parse_mode="Markdown")


async def comando_bs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    permitido, espera = verificar_cooldown(user_id)
    if not permitido:
        await update.message.reply_text(f"Espera *{espera}* segundos.", parse_mode="Markdown")
        return
    if tasas_actuales["usd"] is None:
        await update.message.reply_text("Las tasas no están disponibles.")
        return
    if not context.args:
        await update.message.reply_text("Formato: `/bs 10000`", parse_mode="Markdown")
        return
    try:
        monto = float(context.args[0].replace(",", "."))
        if monto <= 0:
            raise ValueError
    except Exception:
        await update.message.reply_text("Monto no válido.\nEjemplo: `/bs 10000`", parse_mode="Markdown")
        return

    async with procesando:
        tasa = tasa_oficial_usd()
        tasa_id = tasa_intervencion_usd()
        tasa_men = tasa_menudeo_usd()
        respuesta = (
            f"*BANCA & CALCULOS*\n\n"
            f"*Tasa oficial USD*\n{formatear_numero(tasa)} Bs\n\n"
            f"*Monto solicitado*\n{formatear_numero(monto)} BS\n\n"
            f"────────────────\n"
            f"*Intervención 0,5%*\n"
            f"{formatear_numero(tasa_id)} → {formatear_numero(monto / tasa_id)} USD\n\n"
            f"*Menudeo 1,2%*\n"
            f"{formatear_numero(tasa_men)} → {formatear_numero(monto / tasa_men)} USD\n"
            f"────────────────\n\n"
            f"{fecha_espanol()}"
        )
        await update.message.reply_text(respuesta, parse_mode="Markdown")


async def comando_bcv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    permitido, espera = verificar_cooldown(user_id)
    if not permitido:
        await update.message.reply_text(f"Espera *{espera}* segundos.", parse_mode="Markdown")
        return
    if tasas_actuales["usd"] is None or tasas_actuales["eur"] is None:
        await update.message.reply_text("Las tasas no están disponibles.")
        return
    if not context.args:
        await update.message.reply_text("Formato: `/bcv 100`", parse_mode="Markdown")
        return
    try:
        monto = float(context.args[0].replace(",", "."))
        if monto <= 0:
            raise ValueError
    except Exception:
        await update.message.reply_text("Monto no válido.\nEjemplo: `/bcv 100`", parse_mode="Markdown")
        return

    async with procesando:
        tasa_u = tasa_oficial_usd()
        tasa_e = tasa_oficial_eur()
        respuesta = (
            f"*BANCA & CALCULOS*\n\n"
            f"*Monto solicitado*\n{formatear_numero(monto)}\n\n"
            f"────────────────\n"
            f"*USD*\n{formatear_numero(monto * tasa_u)} Bs\n"
            f"Tasa: {formatear_numero(tasa_u)}\n\n"
            f"*EUR*\n{formatear_numero(monto * tasa_e)} Bs\n"
            f"Tasa: {formatear_numero(tasa_e)}\n"
            f"────────────────\n\n"
            f"Sin comisiones · Tasa oficial BCV\n"
            f"{fecha_espanol()}"
        )
        await update.message.reply_text(respuesta, parse_mode="Markdown")


async def comando_usdt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    permitido, espera = verificar_cooldown(user_id)
    if not permitido:
        await update.message.reply_text(f"Espera *{espera}* segundos.", parse_mode="Markdown")
        return

    # Consulta Binance P2P directamente (en un hilo para no bloquear el event loop)
    print(f"[bot] /usdt → consultando Binance P2P (monto={MONTO_BINANCE})")
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, fetch_rates, MONTO_BINANCE)
    except Exception as e:
        err = f"{type(e).__name__}: {str(e)[:300]}"
        print(f"[bot] /usdt error Binance: {err}")
        await update.message.reply_text(
            f"⚠️ No pude consultar Binance P2P.\n\nError:\n`{err}`",
            parse_mode="Markdown",
        )
        return

    if not data or (data["compra"] == 0 and data["venta"] == 0):
        await update.message.reply_text(
            "Binance no devolvió datos en este momento.\nIntenta de nuevo en unos minutos."
        )
        return

    compra = data["compra"]
    venta = data["venta"]

    brecha = 0
    if tasas_actuales["usd"]:
        brecha = ((venta - tasa_oficial_usd()) / tasa_oficial_usd()) * 100
    texto_brecha = "por encima del BCV" if brecha >= 0 else "por debajo del BCV"

    respuesta = (
        f"*BANCA & CRIPTOS | INFORMA*\n\n"
        f"*Fecha:* {datetime.now().strftime('%d/%m')}\n\n"
        f"Compra  →  {formatear_numero(compra)} VES\n"
        f"Venta   →  {formatear_numero(venta)} VES\n\n"
        f"*Brecha cambiaria*\n"
        f"{formatear_numero(abs(brecha))}% {texto_brecha}\n\n"
        f"@BancaYDivisaVe\n¡La información, a tu alcance!"
    )
    await update.message.reply_text(respuesta, parse_mode="Markdown")


async def comando_tasas(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    permitido, espera = verificar_cooldown(user_id)
    if not permitido:
        await update.message.reply_text(f"Espera *{espera}* segundos.", parse_mode="Markdown")
        return
    if tasas_actuales["usd"] is None:
        await update.message.reply_text("Las tasas no están disponibles.")
        return

    respuesta = (
        f"*BANCA & DIVISA*\n\n"
        f"*Tasa oficial del Banco Central*\n"
        f"{fecha_espanol(tasas_actuales['fecha'])}\n\n"
        f"Dólar (USD)   {formatear_numero(tasa_oficial_usd())} Bs\n"
        f"Euro  (EUR)   {formatear_numero(tasa_oficial_eur())} Bs"
    )
    var = texto_variacion_usd()
    if var:
        respuesta += f"\n\n{var}"
    respuesta += f"\n\n@BancaYDivisaVe\n¡La información, a tu alcance!"
    await update.message.reply_text(respuesta, parse_mode="Markdown")


async def comando_historial(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or len(context.args) < 2:
        await update.message.reply_text(
            "Uso: `/historial AAAA-MM-DD AAAA-MM-DD`\nEjemplo: `/historial 2026-09-01 2026-10-03`",
            parse_mode="Markdown",
        )
        return

    desde, hasta = context.args[0], context.args[1]
    registros = await obtener_historial_bcv(_pool, desde, hasta)

    if not registros:
        await update.message.reply_text(f"No hay tasas entre {desde} y {hasta}.")
        return

    lineas = [f"*Historial BCV* ({desde} → {hasta})\n"]
    for r in registros[:30]:
        lineas.append(
            f"`{r['fecha']}`  USD: {formatear_numero(float(r['usd']))}  EUR: {formatear_numero(float(r['eur']))}"
        )
    if len(registros) > 30:
        lineas.append(f"\n_... y {len(registros) - 30} registros más._")

    await update.message.reply_text("\n".join(lineas), parse_mode="Markdown")


# ============================================================
#  COMANDOS DE ADMIN
# ============================================================
async def comando_buscar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_admin(update.effective_user.id):
        return
    jobs = context.job_queue.get_jobs_by_name("busqueda_tasa")
    if jobs:
        await update.message.reply_text("La búsqueda ya está activa.")
        return
    context.job_queue.run_repeating(
        tarea_buscar, interval=INTERVALO_BUSQUEDA, first=5, name="busqueda_tasa"
    )
    await update.message.reply_text(
        "Búsqueda activada. Revisaré cada 5 minutos.", parse_mode="Markdown"
    )


async def tarea_buscar(context: ContextTypes.DEFAULT_TYPE):
    nuevas = consultar_tasas_bcv()
    if not nuevas or not tasas_cambiaron(nuevas):
        return

    await aplicar_nuevas_tasas_async(nuevas)
    ok = await publicar_en_canal(context.bot)

    for job in context.job_queue.get_jobs_by_name("busqueda_tasa"):
        job.schedule_removal()

    await context.bot.send_message(
        chat_id=ADMIN_ID_INICIAL,
        text="Nueva tasa publicada." if ok else "Tasa actualizada, pero no se pudo publicar.",
    )


async def reload_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_admin(update.effective_user.id):
        return
    mensaje_espera = await update.message.reply_text("Actualizando tasas...")
    nuevas = consultar_tasas_bcv()
    if not nuevas:
        await mensaje_espera.edit_text("No se pudieron actualizar las tasas.")
        return

    cambio = tasas_cambiaron(nuevas)
    await aplicar_nuevas_tasas_async(nuevas)

    texto = (
        f"Tasas actualizadas\n\n"
        f"USD: {formatear_numero(tasa_oficial_usd())} Bs\n"
        f"EUR: {formatear_numero(tasa_oficial_eur())} Bs\n\n"
        f"Fecha valor: {fecha_espanol(tasas_actuales['fecha'])}"
    )
    if cambio:
        ok = await publicar_en_canal(context.bot)
        texto += "\n\nSe publicó en el canal." if ok else "\n\nNo se pudo publicar."
    else:
        texto += "\n\nLa tasa no cambió."

    await mensaje_espera.edit_text(texto)


async def comando_montos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_admin(update.effective_user.id):
        return
    if tasas_actuales["usd"] is None:
        await update.message.reply_text("Las tasas no están disponibles.")
        return

    canal = canal_actual()
    if not canal:
        await update.message.reply_text("No hay canal principal configurado.")
        return

    espera = await update.message.reply_text("Enviando mensajes al canal...")

    bcv = tasa_oficial_usd()
    intervencion = tasa_intervencion_usd()

    mensaje1 = (
        "🔔 *TASA DE INTERVENCIÓN* 📌\n\n"
        "🖥 Tasa de Intervención Bancaria. 💱\n\n"
        "📊 *Fórmula y resultado:*\n"
        f"( {formatear_numero(bcv)} ) + 0,50% = Tasa ( {formatear_numero(intervencion)} )\n\n"
        "@BancaYDivisaVe ✅\n🔗 ¡La información, a tu alcance!"
    )

    lineas = []
    for monto in range(50, 1001, 50):
        total = redondear(monto * intervencion)
        lineas.append(f"💵 {monto} $ = {formatear_numero(total)} Bs")
    tabla = "\n".join(lineas)

    mensaje2 = (
        "🔔 *MONTOS* 📌\n\n"
        f"Montos de compra calculados con La Tasa De Intervención "
        f"{formatear_numero(intervencion)} Bs. 💱\n\n"
        f"```\n{tabla}\n```\n\n"
        "@BancaYDivisaVe ✅\n🔗 ¡La información, a tu alcance!"
    )

    try:
        await context.bot.send_message(chat_id=canal, text=mensaje1, parse_mode="Markdown")
        await asyncio.sleep(5)
        await context.bot.send_message(chat_id=canal, text=mensaje2, parse_mode="Markdown")
        await espera.edit_text("Mensajes enviados al canal.")
    except Exception as e:
        print(f"Error en /montos: {e}")
        await espera.edit_text("No pude publicar en el canal.")


# ============================================================
#  ADMINISTRACIÓN DE ADMINS
# ============================================================
async def cmd_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_admin(update.effective_user.id):
        return
    admins = await listar_admins(_pool)
    lineas = ["*Admins actuales:*\n"]
    for a in admins:
        nombre = a["username"] or "sin_username"
        lineas.append(f"• `{a['user_id']}` — {nombre}")
    lineas.append("\nPara agregar: `/addadmin <user_id> [username]`")
    lineas.append("Para eliminar: `/deladmin <user_id>`")
    await update.message.reply_text("\n".join(lineas), parse_mode="Markdown")


async def cmd_addadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text(
            "Uso: `/addadmin <user_id> [username]`\n"
            "Ejemplo: `/addadmin 8625261339 juan`",
            parse_mode="Markdown",
        )
        return
    try:
        nuevo_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id debe ser numérico.")
        return

    username = context.args[1] if len(context.args) > 1 else None
    await agregar_admin(_pool, nuevo_id, username)
    await recargar_config(_pool)
    await update.message.reply_text(
        f"✅ Admin agregado: `{nuevo_id}`",
        parse_mode="Markdown",
    )


async def cmd_deladmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_admin(update.effective_user.id):
        return
    if not context.args:
        await update.message.reply_text("Uso: `/deladmin <user_id>`", parse_mode="Markdown")
        return
    try:
        objetivo = int(context.args[0])
    except ValueError:
        await update.message.reply_text("El user_id debe ser numérico.")
        return

    admins = await listar_admins(_pool)
    if len(admins) <= 1 and objetivo == update.effective_user.id:
        await update.message.reply_text("⚠️ No puedes eliminarte: eres el único admin.")
        return

    ok = await eliminar_admin(_pool, objetivo)
    await recargar_config(_pool)
    await update.message.reply_text(
        f"✅ Admin `{objetivo}` eliminado." if ok else f"❌ No encontré al admin `{objetivo}`.",
        parse_mode="Markdown",
    )


# ============================================================
#  ADMINISTRACIÓN DEL CANAL
# ============================================================
async def cmd_canal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_admin(update.effective_user.id):
        return

    if not context.args:
        canal = await obtener_canal_principal(_pool)
        if canal:
            await update.message.reply_text(
                f"*Canal principal actual:*\n`{canal['chat_id']}`",
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text("No hay canal configurado.")
        return

    nuevo_canal = context.args[0]
    if not nuevo_canal.startswith("@") and not nuevo_canal.startswith("-"):
        nuevo_canal = "@" + nuevo_canal

    try:
        await context.bot.get_chat(nuevo_canal)
    except Exception as e:
        await update.message.reply_text(
            f"No pude acceder a `{nuevo_canal}`.\n"
            f"Asegúrate de que el bot sea administrador del canal.\n\nError: {e}",
            parse_mode="Markdown",
        )
        return

    await establecer_canal_principal(_pool, nuevo_canal, nombre=nuevo_canal)
    await recargar_config(_pool)
    await update.message.reply_text(
        f"✅ Canal principal cambiado a `{nuevo_canal}`.",
        parse_mode="Markdown",
    )


async def cmd_canales(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_admin(update.effective_user.id):
        return
    canales = await listar_canales(_pool)
    if not canales:
        await update.message.reply_text("No hay canales registrados.")
        return
    lineas = ["*Canales registrados:*\n"]
    for c in canales:
        marca = "⭐" if c["principal"] else "  "
        lineas.append(f"{marca} `{c['chat_id']}`")
    await update.message.reply_text("\n".join(lineas), parse_mode="Markdown")


# ============================================================
#  CONFIG GENERAL
# ============================================================
async def cmd_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not es_admin(update.effective_user.id):
        return

    admins = await listar_admins(_pool)
    canal = await obtener_canal_principal(_pool)

    texto = (
        f"⚙️ *Configuración actual*\n\n"
        f"*Admins ({len(admins)}):*\n"
        + "\n".join(f"• `{a['user_id']}`" for a in admins)
        + f"\n\n*Canal principal:*\n"
        f"`{canal['chat_id'] if canal else 'No configurado'}`\n\n"
        f"*Comandos:*\n"
        f"`/admins` — listar admins\n"
        f"`/addadmin <id> [nombre]` — agregar admin\n"
        f"`/deladmin <id>` — eliminar admin\n"
        f"`/canal` — ver canal actual\n"
        f"`/canal @nuevo` — cambiar canal\n"
        f"`/canales` — listar canales\n"
    )
    await update.message.reply_text(texto, parse_mode="Markdown")


# ============================================================
#  INICIALIZACIÓN
# ============================================================
async def inicializar_aplicacion(pool):
    """Crea la Application de Telegram y registra todos los handlers."""
    set_pool(pool)
    await inicializar_db(pool)
    await bootstrap_config(pool)   # ← puebla admins/canal si está vacío
    await recargar_config(pool)    # ← carga caché en memoria
    await cargar_tasas_desde_db()

    # Si no hay tasas en DB, consultar BCV ahora
    if tasas_actuales["usd"] is None:
        nuevas = consultar_tasas_bcv()
        if nuevas:
            await aplicar_nuevas_tasas_async(nuevas)

    application = Application.builder().token(TOKEN).build()

    # Comandos públicos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ayuda", ayuda))
    application.add_handler(CommandHandler("usd", comando_usd))
    application.add_handler(CommandHandler("eur", comando_eur))
    application.add_handler(CommandHandler("bs", comando_bs))
    application.add_handler(CommandHandler("bcv", comando_bcv))
    application.add_handler(CommandHandler("tasas", comando_tasas))
    application.add_handler(CommandHandler("usdt", comando_usdt))
    application.add_handler(CommandHandler("historial", comando_historial))

    # Comandos de admin (verifican es_admin internamente)
    application.add_handler(CommandHandler("reload", reload_bot))
    application.add_handler(CommandHandler("buscar", comando_buscar))
    application.add_handler(CommandHandler("montos", comando_montos))
    application.add_handler(CommandHandler("config", cmd_config))
    application.add_handler(CommandHandler("admins", cmd_admins))
    application.add_handler(CommandHandler("addadmin", cmd_addadmin))
    application.add_handler(CommandHandler("deladmin", cmd_deladmin))
    application.add_handler(CommandHandler("canal", cmd_canal))
    application.add_handler(CommandHandler("canales", cmd_canales))

    return application
