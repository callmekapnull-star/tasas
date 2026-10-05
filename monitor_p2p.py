# monitor_p2p.py
# Solo lo esencial: consultar Binance P2P y scrapear el BCV.

import gzip
import zlib
import json
import requests
from datetime import datetime

# ============================================================
#  BINANCE P2P
# ============================================================
URL_BINANCE_P2P = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"

HEADERS_BINANCE = {
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}

# aqui van los métodos de pago que aceptamos
PAY_TYPES_DEFAULT = ["Banesco", "Mercantil", "Provincial"]

# aqui va el monto por defecto a consultar (en VES)
IMPORTE_DEFAULT = 100000


def obtener_tasas_binance(monto_consulta, trade_type, pay_types=None, fiat="VES"):
    """Consulta el endpoint P2P de Binance. Retorna el JSON completo."""
    if pay_types is None:
        pay_types = PAY_TYPES_DEFAULT

    body = {
        "proMerchantAds": False,
        "page": 1,
        "rows": 10,
        "payTypes": pay_types,
        "countries": [],
        "publisherType": None,
        "asset": "USDT",
        "fiat": fiat,
        "tradeType": trade_type,
        "filterType": "all",
        "transAmount": str(monto_consulta),
    }

    r = requests.post(
        URL_BINANCE_P2P,
        data=json.dumps(body),
        headers=HEADERS_BINANCE,
        timeout=20,
        verify=False,
    )
    r.raise_for_status()

    # Descompresión explícita por si requests no lo hace
    encoding = r.headers.get("Content-Encoding", "").lower()
    raw = r.content
    try:
        if encoding == "gzip":
            raw = gzip.decompress(raw)
        elif encoding == "deflate":
            raw = zlib.decompress(raw)
        elif encoding == "br":
            try:
                import brotli
                raw = brotli.decompress(raw)
            except ImportError:
                pass
    except Exception:
        pass

    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return r.json()


def procesar_datos_api(data):
    """Toma los primeros 5 anuncios y normaliza los campos."""
    if not data.get("data") or not isinstance(data["data"], list):
        raise ValueError("Estructura de datos inválida")

    anuncios = data["data"][:5]
    resultado = []
    for a in anuncios:
        adv = a.get("adv", {})
        advertiser = a.get("advertiser", {})
        metodos = adv.get("tradeMethods") or []
        resultado.append({
            "precio": float(adv.get("price", 0)),
            "min": adv.get("minSingleTransAmount"),
            "max": adv.get("maxSingleTransAmount"),
            "metodo": metodos[0].get("tradeMethodName", "Desconocido") if metodos else "Desconocido",
            "disponible": adv.get("surplusAmount", "0"),
            "advertiser": advertiser.get("nickName", "Anónimo"),
        })
    return resultado


def calcular_promedio(precios):
    if not precios:
        return 0
    return sum(p["precio"] for p in precios) / len(precios)


def fetch_rates(importe=IMPORTE_DEFAULT, pay_types=None):
    """Consulta BUY y SELL en paralelo y devuelve los promedios."""
    data_compra = obtener_tasas_binance(importe, "BUY", pay_types)
    data_venta = obtener_tasas_binance(importe, "SELL", pay_types)

    precios_compra = procesar_datos_api(data_compra)
    precios_venta = procesar_datos_api(data_venta)

    return {
        "compra": calcular_promedio(precios_compra),
        "venta": calcular_promedio(precios_venta),
        "detalles": {"compra": precios_compra, "venta": precios_venta},
    }


# ============================================================
#  SCRAPING BCV (fuente oficial)
# ============================================================
URL_BCV = "https://www.bcv.org.ve/"

HEADERS_BCV = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-VE,es;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
}


def _parsear_fecha_bcv(texto):
    """Convierte 'Viernes, 03 Octubre 2026' a 'YYYY-MM-DD'."""
    meses = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "setiembre": 9, "octubre": 10,
        "noviembre": 11, "diciembre": 12,
    }
    try:
        partes = texto.replace(",", "").split()
        dia = int(partes[1])
        mes = meses[partes[2].lower()]
        anio = int(partes[3])
        return f"{anio}-{mes:02d}-{dia:02d}"
    except Exception:
        return datetime.now().strftime("%Y-%m-%d")


def consultar_tasas_bcv():
    """
    Scrapea www.bcv.org.ve directamente.
    Retorna {'usd': float, 'eur': float, 'fecha': 'YYYY-MM-DD'} o None.
    """
    try:
        from bs4 import BeautifulSoup
        r = requests.get(URL_BCV, headers=HEADERS_BCV, timeout=15, verify=False)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        div_usd = soup.find("div", id="dolar")
        if not div_usd:
            raise ValueError("No se encontró el div#dolar")
        usd = float(
            div_usd.find("strong").get_text(strip=True)
            .replace(".", "").replace(",", ".")
        )

        div_eur = soup.find("div", id="euro")
        if not div_eur:
            raise ValueError("No se encontró el div#euro")
        eur = float(
            div_eur.find("strong").get_text(strip=True)
            .replace(".", "").replace(",", ".")
        )

        fecha_tag = soup.find("span", class_="date-display-single")
        fecha = (
            _parsear_fecha_bcv(fecha_tag.get_text(strip=True))
            if fecha_tag else datetime.now().strftime("%Y-%m-%d")
        )

        return {"usd": usd, "eur": eur, "fecha": fecha}

    except Exception as e:
        print(f"[BCV] Error consultando tasas: {e}")
        return None