"""Remote MCP wrapper: pykrx (daily/EOD) + KIS real-time quotes (read-only).

- Streamable HTTP transport behind a secret URL path (/<MCP_TOKEN>/mcp).
- DNS-rebinding/Host protection disabled (needed behind a cloud host).
- Adds KIS (한국투자증권) real-time price tools. READ-ONLY: no order tools.

Env vars:
    MCP_TOKEN       (required) secret URL path segment
    KIS_APP_KEY     (optional) 한국투자증권 Open API app key
    KIS_APP_SECRET  (optional) 한국투자증권 Open API app secret
    KIS_DOMAIN      "real" (default) or "virtual"
"""
import asyncio
import contextlib
import datetime
import os

import httpx
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Mount, Route

from mcp.server.transport_security import TransportSecuritySettings
from pykrx_mcp.server import mcp

TOKEN = os.environ.get("MCP_TOKEN", "").strip()
if not TOKEN:
    raise SystemExit("MCP_TOKEN environment variable is required")

mcp.settings.transport_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=False
)

# ---- KIS (한국투자증권) real-time quote tool, READ-ONLY ----
KIS_APP_KEY = os.environ.get("KIS_APP_KEY", "").strip()
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "").strip()
KIS_DOMAIN = os.environ.get("KIS_DOMAIN", "real").strip().lower()
KIS_BASE = (
    "https://openapivts.koreainvestment.com:29443"
    if KIS_DOMAIN in ("virtual", "vts", "mock", "paper")
    else "https://openapi.koreainvestment.com:9443"
)

_token = {"value": None, "exp": 0.0}
_token_lock = asyncio.Lock()


async def _kis_token() -> str:
    import time
    async with _token_lock:
        now = time.time()
        if _token["value"] and now < _token["exp"] - 120:
            return _token["value"]
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                f"{KIS_BASE}/oauth2/tokenP",
                json={
                    "grant_type": "client_credentials",
                    "appkey": KIS_APP_KEY,
                    "appsecret": KIS_APP_SECRET,
                },
            )
            r.raise_for_status()
            d = r.json()
        _token["value"] = d["access_token"]
        _token["exp"] = now + int(d.get("expires_in", 86400))
        return _token["value"]


@mcp.tool()
async def get_realtime_price(symbol: str) -> dict:
    """
    한국 주식의 실시간 현재가를 조회합니다 (한국투자증권 KIS Open API).

    pykrx는 일별(장 마감 후) 데이터지만, 이 도구는 장중 현재가를 실시간으로
    가져옵니다. 조회 전용이며 주문 기능은 없습니다.

    Args:
        symbol: 6자리 종목코드 (예: "005930" 삼성전자)

    Returns:
        현재가, 전일대비, 등락률, 시/고/저가, 누적거래량, PER/PBR 등
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"error": "KIS_APP_KEY/KIS_APP_SECRET 환경변수가 설정되지 않았습니다."}
    try:
        token = await _kis_token()
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHKST01010100",
            "custtype": "P",
        }
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{KIS_BASE}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers=headers,
                params=params,
            )
        d = r.json()
        o = d.get("output") or {}
        if not o:
            return {"error": d.get("msg1", "조회 실패"), "rt_cd": d.get("rt_cd"), "raw": d}

        def i(x):
            try:
                return int(x)
            except (TypeError, ValueError):
                return None

        def f(x):
            try:
                return float(x)
            except (TypeError, ValueError):
                return None

        sign = {"1": "상한", "2": "상승", "3": "보합", "4": "하한", "5": "하락"}
        kst = datetime.timezone(datetime.timedelta(hours=9))
        return {
            "symbol": symbol,
            "name": o.get("hts_kor_isnm"),
            "현재가": i(o.get("stck_prpr")),
            "전일대비": i(o.get("prdy_vrss")),
            "등락": sign.get(o.get("prdy_vrss_sign"), o.get("prdy_vrss_sign")),
            "등락률": f(o.get("prdy_ctrt")),
            "시가": i(o.get("stck_oprc")),
            "고가": i(o.get("stck_hgpr")),
            "저가": i(o.get("stck_lwpr")),
            "누적거래량": i(o.get("acml_vol")),
            "누적거래대금": i(o.get("acml_tr_pbmn")),
            "PER": o.get("per"),
            "PBR": o.get("pbr"),
            "52주최고": i(o.get("w52_hgpr")),
            "52주최저": i(o.get("w52_lwpr")),
            "조회시각_KST": datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S"),
            "데이터출처": "KIS (한국투자증권) " + ("모의" if KIS_BASE.find("vts") > 0 else "실전"),
        }
    except Exception as e:  # noqa: BLE001
        return {"error": f"{type(e).__name__}: {e}"}


async def health(request):
    return PlainTextResponse("ok")


streamable_app = mcp.streamable_http_app()


@contextlib.asynccontextmanager
async def lifespan(app):
    async with mcp.session_manager.run():
        yield


app = Starlette(
    routes=[Route("/", health), Mount(f"/{TOKEN}", app=streamable_app)],
    lifespan=lifespan,
)
