"""Remote MCP wrapper: pykrx (daily/EOD) + KIS real-time + 경제지표.

- Streamable HTTP transport behind a secret URL path (/<MCP_TOKEN>/mcp).
- DNS-rebinding/Host protection disabled (needed behind a cloud host).

Env vars:
  MCP_TOKEN       (required) secret URL path segment
  KIS_APP_KEY     (optional) 한국투자증권 Open API app key
  KIS_APP_SECRET  (optional) 한국투자증권 Open API app secret
  KIS_DOMAIN      "real" (default) or "virtual"
  FRED_API_KEY    (optional) FRED (미국 연준) API 키
  ECOS_API_KEY    (optional) 한국은행 ECOS API 키
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

# ---- 공통 설정 ----
KIS_APP_KEY    = os.environ.get("KIS_APP_KEY", "").strip()
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "").strip()
KIS_DOMAIN     = os.environ.get("KIS_DOMAIN", "real").strip().lower()
KIS_BASE = (
    "https://openapivts.koreainvestment.com:29443"
    if KIS_DOMAIN in ("virtual", "vts", "mock", "paper")
    else "https://openapi.koreainvestment.com:9443"
)

FRED_API_KEY = os.environ.get("FRED_API_KEY", "").strip()
ECOS_API_KEY = os.environ.get("ECOS_API_KEY", "").strip()

_token_cache = {"value": None, "exp": 0.0}
_token_lock  = asyncio.Lock()

KST = datetime.timezone(datetime.timedelta(hours=9))


def _now_kst() -> str:
    return datetime.datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def _i(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


async def _kis_token() -> str:
    import time
    async with _token_lock:
        now = time.time()
        if _token_cache["value"] and now < _token_cache["exp"] - 120:
            return _token_cache["value"]
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(
                f"{KIS_BASE}/oauth2/tokenP",
                headers={"content-type": "application/json"},
                json={
                    "grant_type": "client_credentials",
                    "appkey": KIS_APP_KEY,
                    "appsecret": KIS_APP_SECRET,
                },
            )
            if r.status_code != 200:
                raise RuntimeError(
                    f"KIS token {r.status_code} (domain={KIS_DOMAIN}): {r.text[:400]}"
                )
            d = r.json()
            _token_cache["value"] = d["access_token"]
            _token_cache["exp"]   = now + int(d.get("expires_in", 86400))
            return _token_cache["value"]


# ====================================================================
# 기존 도구 (현재가·미국현재가·TradingView) — 원본 유지
# ====================================================================

@mcp.tool()
async def get_realtime_price(symbol: str) -> dict:
    """
    한국 주식의 실시간 현재가를 조회합니다 (KIS Open API).

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
            return {"error": d.get("msg1", "조회 실패"), "rt_cd": d.get("rt_cd")}
        sign = {"1": "상한", "2": "상승", "3": "보합", "4": "하한", "5": "하락"}
        return {
            "symbol":     symbol,
            "name":       o.get("hts_kor_isnm"),
            "현재가":     _i(o.get("stck_prpr")),
            "전일대비":   _i(o.get("prdy_vrss")),
            "등락":       sign.get(o.get("prdy_vrss_sign"), o.get("prdy_vrss_sign")),
            "등락률":     _f(o.get("prdy_ctrt")),
            "시가":       _i(o.get("stck_oprc")),
            "고가":       _i(o.get("stck_hgpr")),
            "저가":       _i(o.get("stck_lwpr")),
            "누적거래량": _i(o.get("acml_vol")),
            "누적거래대금": _i(o.get("acml_tr_pbmn")),
            "PER":        o.get("per"),
            "PBR":        o.get("pbr"),
            "52주최고":   _i(o.get("w52_hgpr")),
            "52주최저":   _i(o.get("w52_lwpr")),
            "조회시각_KST": _now_kst(),
            "데이터출처": "KIS (한국투자증권)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@mcp.tool()
async def get_us_realtime_price(symbol: str, exchange: str = "auto") -> dict:
    """
    미국 주식의 현재가를 조회합니다 (KIS 해외주식 시세 API).

    Args:
        symbol: 미국 종목 티커 (예: "AAPL", "TSLA", "NVDA")
        exchange: "NAS"(나스닥), "NYS"(뉴욕), "AMS"(아멕스), "auto"(기본, 자동탐색)

    Returns:
        현재가(USD), 전일대비, 등락률, 거래량 등. 약 15분 지연 가능.
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"error": "KIS_APP_KEY/KIS_APP_SECRET 환경변수가 설정되지 않았습니다."}
    sym   = symbol.strip().upper()
    excd  = exchange.strip().upper()
    exchanges = [excd] if excd in ("NAS", "NYS", "AMS") else ["NAS", "NYS", "AMS"]
    try:
        token = await _kis_token()
        last_raw = None
        async with httpx.AsyncClient(timeout=15) as client:
            for ex in exchanges:
                headers = {
                    "authorization": f"Bearer {token}",
                    "appkey": KIS_APP_KEY,
                    "appsecret": KIS_APP_SECRET,
                    "tr_id": "HHDFS00000300",
                    "custtype": "P",
                }
                params = {"AUTH": "", "EXCD": ex, "SYMB": sym}
                r = await client.get(
                    f"{KIS_BASE}/uapi/overseas-price/v1/quotations/price",
                    headers=headers,
                    params=params,
                )
                d = r.json()
                last_raw = d
                o = d.get("output") or {}
                if d.get("rt_cd") == "0" and o.get("last") not in (None, "", "0", "0.0000"):
                    sign = {"1": "상한", "2": "상승", "3": "보합", "4": "하락", "5": "하한"}
                    excd_name = {"NAS": "나스닥", "NYS": "뉴욕(NYSE)", "AMS": "아멕스"}
                    return {
                        "symbol":          sym,
                        "거래소":          excd_name.get(ex, ex),
                        "현재가_USD":      _f(o.get("last")),
                        "전일종가_USD":    _f(o.get("base")),
                        "전일대비":        _f(o.get("diff")),
                        "등락":            sign.get(o.get("sign"), o.get("sign")),
                        "등락률":          _f(o.get("rate")),
                        "거래량":          _f(o.get("tvol")),
                        "거래대금_USD":    _f(o.get("tamt")),
                        "조회시각_KST":    _now_kst(),
                        "데이터출처":      "KIS 해외주식 (실시간 미신청 시 약 15분 지연 가능)",
                    }
        return {
            "error": f"'{sym}' 종목을 NAS/NYS/AMS에서 찾지 못했거나 데이터가 없습니다.",
            "rt_cd": (last_raw or {}).get("rt_cd"),
            "msg":   (last_raw or {}).get("msg1"),
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@mcp.tool()
async def get_tradingview_analysis(symbol: str, exchange: str = "auto", interval: str = "1D") -> dict:
    """
    TradingView 기술적 분석(RSI·MACD·이동평균·매수/매도 시그널)을 조회합니다.

    Args:
        symbol:   심볼. 미국주식 "AAPL", 한국주식 "005930", 선물 "NQ1!", 외환 "EURUSD" 등
        exchange: "auto"(기본), "NASDAQ"/"NYSE"/"KRX"/"CME"/"BINANCE"/"FX_IDC" 등
        interval: "1m","5m","15m","30m","1h","2h","4h","1D"(기본),"1W","1M"

    Returns:
        추천(매수/매도/중립), RSI, MACD, 이동평균 등
    """
    try:
        from tradingview_ta import TA_Handler, Interval
    except Exception as e:
        return {"error": f"tradingview_ta 로드 실패: {e}"}

    imap = {
        "1m":  Interval.INTERVAL_1_MINUTE,  "5m":  Interval.INTERVAL_5_MINUTES,
        "15m": Interval.INTERVAL_15_MINUTES, "30m": Interval.INTERVAL_30_MINUTES,
        "1h":  Interval.INTERVAL_1_HOUR,     "2h":  Interval.INTERVAL_2_HOURS,
        "4h":  Interval.INTERVAL_4_HOURS,    "1d":  Interval.INTERVAL_1_DAY,
        "1w":  Interval.INTERVAL_1_WEEK,     "1mo": Interval.INTERVAL_1_MONTH,
        "1month": Interval.INTERVAL_1_MONTH,
    }
    iv  = imap.get(interval.lower(), Interval.INTERVAL_1_DAY)
    sym = symbol.strip().upper()
    ex  = exchange.strip().upper()

    if ex not in ("AUTO", ""):
        if ex == "KRX":
            cands = [("korea", ex)]
        elif ex in ("BINANCE", "BYBIT", "KUCOIN", "COINBASE", "OKX"):
            cands = [("crypto", ex)]
        elif ex in ("FX_IDC", "OANDA", "FOREXCOM"):
            cands = [("forex", ex)]
        elif ex in ("CME", "CME_MINI", "CBOT", "NYMEX", "COMEX", "EUREX", "ICEUS"):
            cands = [("futures", ex)]
        else:
            cands = [("america", ex)]
    elif sym.isdigit() and len(sym) == 6:
        cands = [("korea", "KRX")]
    elif sym.endswith("!"):
        cands = [("futures", "CME")]
    elif sym.endswith(("USDT", "USDC", "BTC", "ETH")) and len(sym) > 5:
        cands = [("crypto", "BINANCE")]
    elif len(sym) == 6 and sym.isalpha():
        cands = [("forex", "FX_IDC"), ("america", "NASDAQ")]
    else:
        cands = [("america", "NASDAQ"), ("america", "NYSE"), ("america", "AMEX")]

    def run():
        last = None
        for scr, exc in cands:
            try:
                h = TA_Handler(symbol=sym, screener=scr, exchange=exc, interval=iv)
                return scr, exc, h.get_analysis()
            except Exception as e:
                last = e
        raise last or RuntimeError("no analysis")

    try:
        scr, exc, a = await asyncio.to_thread(run)
    except Exception as e:
        return {"error": f"분석 실패: {type(e).__name__}: {e}", "tried": cands}

    ind = a.indicators or {}

    def g(k):
        v = ind.get(k)
        return round(v, 4) if isinstance(v, (int, float)) else v

    return {
        "symbol":      sym,
        "거래소":      exc,
        "시장":        scr,
        "간격":        interval,
        "추천":        a.summary.get("RECOMMENDATION"),
        "매수신호수":  a.summary.get("BUY"),
        "매도신호수":  a.summary.get("SELL"),
        "중립":        a.summary.get("NEUTRAL"),
        "오실레이터_추천":  a.oscillators.get("RECOMMENDATION"),
        "이동평균_추천":    a.moving_averages.get("RECOMMENDATION"),
        "종가":        g("close"),
        "RSI":         g("RSI"),
        "MACD":        g("MACD.macd"),
        "MACD_signal": g("MACD.signal"),
        "Stoch_K":     g("Stoch.K"),
        "ADX":         g("ADX"),
        "EMA20":       g("EMA20"),
        "SMA50":       g("SMA50"),
        "SMA200":      g("SMA200"),
        "조회시각_KST": _now_kst(),
        "데이터출처":  "TradingView (비공식, tradingview-ta)",
    }


# ====================================================================
# A-1: 국내 호가창
# ====================================================================

@mcp.tool()
async def get_orderbook(symbol: str) -> dict:
    """
    한국 주식의 실시간 호가창(매수·매도 10단계)을 조회합니다 (KIS Open API).

    Args:
        symbol: 6자리 종목코드 (예: "005930" 삼성전자)

    Returns:
        매수/매도 각 10호가 가격·잔량, 총매수잔량, 총매도잔량
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"error": "KIS_APP_KEY/KIS_APP_SECRET 환경변수가 설정되지 않았습니다."}
    try:
        token = await _kis_token()
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHKST01010200",
            "custtype": "P",
        }
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": symbol}
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{KIS_BASE}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
                headers=headers,
                params=params,
            )
        d = r.json()
        o = d.get("output1") or {}
        if not o:
            return {"error": d.get("msg1", "조회 실패"), "rt_cd": d.get("rt_cd")}

        asks = []
        bids = []
        for i in range(1, 11):
            asks.append({
                "호가":  _i(o.get(f"askp{i}")),
                "잔량":  _i(o.get(f"askp_rsqn{i}")),
            })
            bids.append({
                "호가":  _i(o.get(f"bidp{i}")),
                "잔량":  _i(o.get(f"bidp_rsqn{i}")),
            })
        return {
            "symbol":       symbol,
            "매도호가":     asks,   # asks[0]이 최우선매도호가
            "매수호가":     bids,   # bids[0]이 최우선매수호가
            "총매도잔량":   _i(o.get("total_askp_rsqn")),
            "총매수잔량":   _i(o.get("total_bidp_rsqn")),
            "조회시각_KST": _now_kst(),
            "데이터출처":   "KIS (한국투자증권)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# A-2: 국내 분봉 데이터
# ====================================================================

@mcp.tool()
async def get_minute_candles(symbol: str, interval: int = 1, count: int = 30) -> dict:
    """
    한국 주식의 장중 분봉 데이터를 조회합니다 (KIS Open API).
    pykrx가 제공하는 일봉과 달리 장중 실시간 분봉을 가져옵니다.

    Args:
        symbol:   6자리 종목코드 (예: "005930" 삼성전자)
        interval: 분 단위. 1(기본), 3, 5, 10, 15, 30, 60, 120 중 하나
        count:    반환할 캔들 개수 (최대 30, KIS 1회 호출 제한)

    Returns:
        시각, 시/고/저/종가, 거래량 리스트 (최신순)
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"error": "KIS_APP_KEY/KIS_APP_SECRET 환경변수가 설정되지 않았습니다."}
    valid_intervals = {1, 3, 5, 10, 15, 30, 60, 120}
    if interval not in valid_intervals:
        return {"error": f"interval은 {sorted(valid_intervals)} 중 하나여야 합니다."}
    count = min(max(1, count), 30)
    try:
        token = await _kis_token()
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHKST03010200",
            "custtype": "P",
        }
        params = {
            "FID_ETC_CLS_CODE":     "",
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD":       symbol,
            "FID_INPUT_HOUR_1":     str(interval),
            "FID_PW_DATA_INCU_YN":  "Y",
        }
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{KIS_BASE}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
                headers=headers,
                params=params,
            )
        d = r.json()
        outputs = d.get("output2") or []
        if not outputs:
            return {"error": d.get("msg1", "조회 실패"), "rt_cd": d.get("rt_cd")}

        candles = []
        for row in outputs[:count]:
            candles.append({
                "시각":   row.get("stck_bsop_date", "") + " " + row.get("stck_cntg_hour", ""),
                "시가":   _i(row.get("stck_oprc")),
                "고가":   _i(row.get("stck_hgpr")),
                "저가":   _i(row.get("stck_lwpr")),
                "종가":   _i(row.get("stck_prpr")),
                "거래량": _i(row.get("cntg_vol")),
            })
        return {
            "symbol":       symbol,
            "interval_분":  interval,
            "캔들수":       len(candles),
            "캔들":         candles,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "KIS (한국투자증권)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# A-3: 지수 시세 (코스피·코스닥·나스닥 등)
# ====================================================================

@mcp.tool()
async def get_index(index_code: str = "0001") -> dict:
    """
    국내 주요 지수의 현재 시세를 조회합니다 (KIS Open API).

    Args:
        index_code: 지수 코드
            - "0001" : 코스피 (기본값)
            - "1001" : 코스닥
            - "2001" : 코스피200
            - "0003" : KRX100

    Returns:
        현재지수, 전일대비, 등락률, 거래량, 거래대금 등
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"error": "KIS_APP_KEY/KIS_APP_SECRET 환경변수가 설정되지 않았습니다."}
    index_names = {
        "0001": "코스피",
        "1001": "코스닥",
        "2001": "코스피200",
        "0003": "KRX100",
    }
    try:
        token = await _kis_token()
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHPUP02100000",
            "custtype": "P",
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "U",
            "FID_INPUT_ISCD": index_code,
        }
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{KIS_BASE}/uapi/domestic-stock/v1/quotations/inquire-index-price",
                headers=headers,
                params=params,
            )
        d = r.json()
        o = d.get("output") or {}
        if not o:
            return {"error": d.get("msg1", "조회 실패"), "rt_cd": d.get("rt_cd")}

        sign = {"1": "상한", "2": "상승", "3": "보합", "4": "하한", "5": "하락"}
        return {
            "지수코드":     index_code,
            "지수명":       index_names.get(index_code, index_code),
            "현재지수":     _f(o.get("bstp_nmix_prpr")),
            "전일대비":     _f(o.get("bstp_nmix_prdy_vrss")),
            "등락":         sign.get(o.get("prdy_vrss_sign"), o.get("prdy_vrss_sign")),
            "등락률":       _f(o.get("bstp_nmix_prdy_ctrt")),
            "시가":         _f(o.get("bstp_nmix_oprc")),
            "고가":         _f(o.get("bstp_nmix_hgpr")),
            "저가":         _f(o.get("bstp_nmix_lwpr")),
            "거래량":       _i(o.get("acml_vol")),
            "거래대금":     _i(o.get("acml_tr_pbmn")),
            "상승종목수":   _i(o.get("ntby_ascn_issu_cnt")),
            "하락종목수":   _i(o.get("ntby_dscn_issu_cnt")),
            "조회시각_KST": _now_kst(),
            "데이터출처":   "KIS (한국투자증권)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# A-4: pykrx 확장 — 공매도 현황
# ====================================================================

@mcp.tool()
async def get_short_selling(symbol: str, fromdate: str = "", todate: str = "") -> dict:
    """
    특정 종목의 공매도 현황(공매도량·비율·잔고)을 조회합니다 (pykrx).

    Args:
        symbol:   6자리 종목코드 (예: "005930" 삼성전자)
        fromdate: 시작일 "YYYYMMDD" (기본: 최근 20 거래일 전)
        todate:   종료일 "YYYYMMDD" (기본: 오늘)

    Returns:
        날짜별 공매도량, 공매도대금, 공매도비율, 공매도잔고 리스트
    """
    import pykrx.stock as stock

    today = datetime.datetime.now(KST).strftime("%Y%m%d")
    if not todate:
        todate = today
    if not fromdate:
        dt = datetime.datetime.now(KST) - datetime.timedelta(days=30)
        fromdate = dt.strftime("%Y%m%d")

    try:
        df = await asyncio.to_thread(
            stock.get_shorting_volume_by_date, fromdate, todate, symbol
        )
        if df is None or df.empty:
            return {"error": f"{symbol} 공매도 데이터가 없습니다."}

        df = df.reset_index()
        rows = []
        for _, row in df.iterrows():
            entry = {}
            for col in df.columns:
                v = row[col]
                if hasattr(v, "item"):
                    v = v.item()
                entry[str(col)] = v
            rows.append(entry)

        return {
            "symbol":       symbol,
            "기간":         f"{fromdate} ~ {todate}",
            "데이터":       rows,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "pykrx (한국거래소)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# A-5: pykrx 확장 — 투자자별 순매수 (외인·기관·개인)
# ====================================================================

@mcp.tool()
async def get_investor_trading(symbol: str, fromdate: str = "", todate: str = "") -> dict:
    """
    특정 종목의 투자자별 순매수 현황(외국인·기관·개인 등)을 조회합니다 (pykrx).

    Args:
        symbol:   6자리 종목코드 (예: "005930" 삼성전자)
        fromdate: 시작일 "YYYYMMDD" (기본: 최근 20 거래일 전)
        todate:   종료일 "YYYYMMDD" (기본: 오늘)

    Returns:
        날짜별 외국인·기관·개인 등 투자자 유형별 순매수량·순매수금액
    """
    import pykrx.stock as stock

    today = datetime.datetime.now(KST).strftime("%Y%m%d")
    if not todate:
        todate = today
    if not fromdate:
        dt = datetime.datetime.now(KST) - datetime.timedelta(days=30)
        fromdate = dt.strftime("%Y%m%d")

    try:
        df = await asyncio.to_thread(
            stock.get_market_trading_volume_by_date, fromdate, todate, symbol
        )
        if df is None or df.empty:
            return {"error": f"{symbol} 투자자별 거래 데이터가 없습니다."}

        df = df.reset_index()
        rows = []
        for _, row in df.iterrows():
            entry = {}
            for col in df.columns:
                v = row[col]
                if hasattr(v, "item"):
                    v = v.item()
                entry[str(col)] = v
            rows.append(entry)

        return {
            "symbol":       symbol,
            "기간":         f"{fromdate} ~ {todate}",
            "데이터":       rows,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "pykrx (한국거래소)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# A-6: pykrx 확장 — 시가총액 상위 종목
# ====================================================================

@mcp.tool()
async def get_top_market_cap(date: str = "", top_n: int = 20, market: str = "KOSPI") -> dict:
    """
    시가총액 상위 종목 순위를 조회합니다 (pykrx).

    Args:
        date:   기준일 "YYYYMMDD" (기본: 가장 최근 거래일)
        top_n:  조회할 상위 종목 수 (기본 20, 최대 100)
        market: "KOSPI"(기본), "KOSDAQ", "KONEX"

    Returns:
        종목코드, 종목명, 시가총액, 현재가, 등락률, 거래량 등 상위 N개
    """
    import pykrx.stock as stock

    if not date:
        date = datetime.datetime.now(KST).strftime("%Y%m%d")
    top_n = min(max(1, top_n), 100)

    try:
        df = await asyncio.to_thread(
            stock.get_market_cap_by_ticker, date, market=market
        )
        if df is None or df.empty:
            return {"error": f"{date} {market} 시가총액 데이터가 없습니다."}

        df = df.sort_values("시가총액", ascending=False).head(top_n).reset_index()
        rows = []
        for _, row in df.iterrows():
            entry = {}
            for col in df.columns:
                v = row[col]
                if hasattr(v, "item"):
                    v = v.item()
                entry[str(col)] = v
            rows.append(entry)

        return {
            "기준일":       date,
            "시장":         market,
            "상위N":        top_n,
            "데이터":       rows,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "pykrx (한국거래소)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# A-7: pykrx 확장 — 등락률 상위/하위 종목
# ====================================================================

@mcp.tool()
async def get_top_movers(date: str = "", top_n: int = 20, market: str = "KOSPI", direction: str = "up") -> dict:
    """
    당일 등락률 상위(상승) 또는 하위(하락) 종목을 조회합니다 (pykrx).

    Args:
        date:      기준일 "YYYYMMDD" (기본: 가장 최근 거래일)
        top_n:     조회할 종목 수 (기본 20, 최대 50)
        market:    "KOSPI"(기본), "KOSDAQ", "KONEX"
        direction: "up"(상승, 기본) 또는 "down"(하락)

    Returns:
        종목코드, 종목명, 현재가, 등락률, 거래량 등
    """
    import pykrx.stock as stock

    if not date:
        date = datetime.datetime.now(KST).strftime("%Y%m%d")
    top_n = min(max(1, top_n), 50)

    try:
        df = await asyncio.to_thread(
            stock.get_market_ohlcv_by_ticker, date, market=market
        )
        if df is None or df.empty:
            return {"error": f"{date} {market} 데이터가 없습니다."}

        ascending = direction.lower() == "down"
        df = df.sort_values("등락률", ascending=ascending).head(top_n).reset_index()

        rows = []
        for _, row in df.iterrows():
            entry = {}
            for col in df.columns:
                v = row[col]
                if hasattr(v, "item"):
                    v = v.item()
                entry[str(col)] = v
            rows.append(entry)

        return {
            "기준일":       date,
            "시장":         market,
            "방향":         "상승" if direction.lower() == "up" else "하락",
            "상위N":        top_n,
            "데이터":       rows,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "pykrx (한국거래소)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# A-8: pykrx 확장 — 외국인 보유 현황
# ====================================================================

@mcp.tool()
async def get_foreign_holding(symbol: str, fromdate: str = "", todate: str = "") -> dict:
    """
    특정 종목의 외국인 보유 현황(보유량·보유율·한도소진율)을 조회합니다 (pykrx).

    Args:
        symbol:   6자리 종목코드 (예: "005930" 삼성전자)
        fromdate: 시작일 "YYYYMMDD" (기본: 최근 30일 전)
        todate:   종료일 "YYYYMMDD" (기본: 오늘)

    Returns:
        날짜별 외국인 보유주수, 보유율, 한도소진율 등
    """
    import pykrx.stock as stock

    today = datetime.datetime.now(KST).strftime("%Y%m%d")
    if not todate:
        todate = today
    if not fromdate:
        dt = datetime.datetime.now(KST) - datetime.timedelta(days=30)
        fromdate = dt.strftime("%Y%m%d")

    try:
        df = await asyncio.to_thread(
            stock.get_exhaustion_rates_of_foreign_investment_by_date,
            fromdate, todate, symbol
        )
        if df is None or df.empty:
            return {"error": f"{symbol} 외국인 보유 데이터가 없습니다."}

        df = df.reset_index()
        rows = []
        for _, row in df.iterrows():
            entry = {}
            for col in df.columns:
                v = row[col]
                if hasattr(v, "item"):
                    v = v.item()
                entry[str(col)] = v
            rows.append(entry)

        return {
            "symbol":       symbol,
            "기간":         f"{fromdate} ~ {todate}",
            "데이터":       rows,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "pykrx (한국거래소)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# B: 경제지표 — FRED (미국 연준)
# ====================================================================

# 자주 쓰는 FRED 시리즈 사전
FRED_SERIES_MAP = {
    "fed_rate":    ("FEDFUNDS",   "미국 연방기금금리 (%)"),
    "cpi":         ("CPIAUCSL",   "미국 소비자물가지수 (CPI, 2015=100)"),
    "core_cpi":    ("CPILFESL",   "미국 근원CPI (에너지·식품 제외)"),
    "pce":         ("PCEPI",      "미국 PCE 물가지수"),
    "core_pce":    ("PCEPILFE",   "미국 근원PCE 물가지수"),
    "gdp":         ("GDP",        "미국 GDP (십억달러, 연율)"),
    "unemployment":("UNRATE",     "미국 실업률 (%)"),
    "nonfarm":     ("PAYEMS",     "미국 비농업 고용 (천명)"),
    "10y_yield":   ("DGS10",      "미국 10년물 국채금리 (%)"),
    "2y_yield":    ("DGS2",       "미국 2년물 국채금리 (%)"),
    "dxy":         ("DTWEXBGS",   "달러 인덱스 (광의, 무역가중)"),
    "m2":          ("M2SL",       "미국 M2 통화량 (십억달러)"),
    "vix":         ("VIXCLS",     "VIX 변동성 지수"),
    "wti":         ("DCOILWTICO", "WTI 원유가 (달러/배럴)"),
    "sp500":       ("SP500",      "S&P 500 지수"),
    "retail_sales":("RSAFS",      "미국 소매판매 (백만달러)"),
    "ism_mfg":     ("MANEMP",     "미국 제조업 고용"),
    "housing":     ("HOUST",      "미국 주택착공 (천채)"),
    "yield_spread":("T10Y2Y",     "미국 10년-2년 국채 스프레드 (%)"),
}


@mcp.tool()
async def get_fred_indicator(series_id: str, count: int = 12) -> dict:
    """
    미국 연준(FRED)에서 거시경제 지표를 조회합니다.

    Args:
        series_id: FRED 시리즈 ID 또는 단축명.
            단축명: fed_rate, cpi, core_cpi, pce, core_pce, gdp, unemployment,
                   nonfarm, 10y_yield, 2y_yield, dxy, m2, vix, wti, sp500,
                   retail_sales, housing, yield_spread
            직접 FRED ID도 가능 (예: "FEDFUNDS", "DGS10")
        count: 반환할 최근 데이터 개수 (기본 12)

    Returns:
        지표명, 단위, 최근 값 리스트 (날짜·값)

    주의: FRED_API_KEY 환경변수가 필요합니다.
    """
    if not FRED_API_KEY:
        return {
            "error": "FRED_API_KEY 환경변수가 설정되지 않았습니다.",
            "안내": "https://fredaccount.stlouisfed.org/apikeys 에서 무료 발급 후 Render 환경변수에 추가하세요.",
        }

    sid    = series_id.strip()
    label  = sid
    if sid in FRED_SERIES_MAP:
        fred_id, label = FRED_SERIES_MAP[sid]
    else:
        fred_id = sid.upper()

    count = min(max(1, count), 100)

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            # 시리즈 메타
            meta_r = await client.get(
                "https://api.stlouisfed.org/fred/series",
                params={"series_id": fred_id, "api_key": FRED_API_KEY, "file_type": "json"},
            )
            meta = meta_r.json().get("seriess", [{}])[0] if meta_r.status_code == 200 else {}

            # 관측값
            obs_r = await client.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id":    fred_id,
                    "api_key":      FRED_API_KEY,
                    "file_type":    "json",
                    "sort_order":   "desc",
                    "limit":        count,
                },
            )

        if obs_r.status_code != 200:
            return {"error": f"FRED API 오류 {obs_r.status_code}: {obs_r.text[:300]}"}

        obs = obs_r.json().get("observations", [])
        data = []
        for o in obs:
            val = o.get("value", ".")
            data.append({
                "날짜": o.get("date"),
                "값":   _f(val) if val != "." else None,
            })

        return {
            "series_id":    fred_id,
            "지표명":       label,
            "단위":         meta.get("units_short") or meta.get("units", ""),
            "빈도":         meta.get("frequency_short") or meta.get("frequency", ""),
            "최근_업데이트": meta.get("last_updated", ""),
            "데이터":       data,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "FRED (Federal Reserve Bank of St. Louis)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# B: 경제지표 — ECOS (한국은행)
# ====================================================================

ECOS_SERIES_MAP = {
    "base_rate":   ("722Y001", "0101000",  "한국 기준금리 (%)"),
    "cpi":         ("901Y009", "0",        "한국 소비자물가지수 (CPI, 전년동기비)"),
    "gdp":         ("200Y002", "2111Q",    "한국 GDP 성장률 (전기비, %)"),
    "m2":          ("101Y004", "BBHA00",   "한국 M2 통화량 (십억원)"),
    "usd_krw":     ("731Y001", "0000001",  "원달러 환율 (종가, 원)"),
    "trade_balance":("403Y004","I50A",     "한국 무역수지 (백만달러)"),
    "unemployment":("901Y026", "L1100",    "한국 실업률 (%)"),
    "production":  ("901Y019", "I16A",     "한국 산업생산지수 (전년비)"),
    "10y_yield":   ("817Y002", "010200000","한국 국고채 10년 금리 (%)"),
    "3y_yield":    ("817Y002", "010190000","한국 국고채 3년 금리 (%)"),
}


@mcp.tool()
async def get_ecos_indicator(series_id: str, count: int = 12) -> dict:
    """
    한국은행 경제통계시스템(ECOS)에서 거시경제 지표를 조회합니다.

    Args:
        series_id: ECOS 단축명 또는 "통계코드/항목코드" 형식.
            단축명: base_rate, cpi, gdp, m2, usd_krw, trade_balance,
                   unemployment, production, 10y_yield, 3y_yield
            직접 입력: "722Y001/0101000" (통계코드/항목코드)
        count: 반환할 최근 데이터 개수 (기본 12)

    Returns:
        지표명, 단위, 최근 값 리스트 (날짜·값)

    주의: ECOS_API_KEY 환경변수가 필요합니다.
    """
    if not ECOS_API_KEY:
        return {
            "error": "ECOS_API_KEY 환경변수가 설정되지 않았습니다.",
            "안내": "https://ecos.bok.or.kr → 오픈API → 인증키 신청 (무료) 후 Render 환경변수에 추가하세요.",
        }

    sid = series_id.strip()
    label = sid

    if sid in ECOS_SERIES_MAP:
        stat_code, item_code, label = ECOS_SERIES_MAP[sid]
    elif "/" in sid:
        parts = sid.split("/", 1)
        stat_code, item_code = parts[0].strip(), parts[1].strip()
    else:
        return {
            "error": f"알 수 없는 series_id: '{sid}'",
            "사용가능_단축명": list(ECOS_SERIES_MAP.keys()),
            "직접입력_예시": "722Y001/0101000",
        }

    count = min(max(1, count), 100)
    # ECOS는 기간을 YYYYMM 형식으로 요청
    end_date   = datetime.datetime.now(KST).strftime("%Y%m")
    start_year = datetime.datetime.now(KST).year - max(5, count // 2)
    start_date = f"{start_year}01"

    try:
        url = (
            f"https://ecos.bok.or.kr/api/StatisticSearch"
            f"/{ECOS_API_KEY}/json/kr/1/{count + 10}"
            f"/{stat_code}/M/{start_date}/{end_date}/{item_code}"
        )
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url)

        if r.status_code != 200:
            return {"error": f"ECOS API 오류 {r.status_code}: {r.text[:300]}"}

        body = r.json()
        stat_search = body.get("StatisticSearch") or {}
        err = stat_search.get("RESULT")
        if err and err.get("CODE", "").startswith("INFO-"):
            # 데이터 없으면 연간 주기로 재시도
            url2 = (
                f"https://ecos.bok.or.kr/api/StatisticSearch"
                f"/{ECOS_API_KEY}/json/kr/1/{count + 10}"
                f"/{stat_code}/A/{start_date[:4]}/{end_date[:4]}/{item_code}"
            )
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(url2)
            body = r.json()
            stat_search = body.get("StatisticSearch") or {}

        rows = stat_search.get("row") or []
        if not rows:
            return {
                "error":  "데이터가 없습니다. 통계코드·항목코드·기간을 확인하세요.",
                "raw":    body,
            }

        data = []
        for row in rows[-count:]:
            data.append({
                "날짜": row.get("TIME"),
                "값":   _f(row.get("DATA_VALUE")),
                "단위": row.get("UNIT_NAME"),
            })
        data.reverse()  # 최신순

        unit = rows[0].get("UNIT_NAME", "") if rows else ""
        stat_name = rows[0].get("STAT_NAME", label) if rows else label
        item_name = rows[0].get("ITEM_NAME1", "") if rows else ""

        return {
            "series_id":    f"{stat_code}/{item_code}",
            "지표명":       label if label != sid else f"{stat_name} - {item_name}",
            "단위":         unit,
            "데이터":       data,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "ECOS (한국은행 경제통계시스템)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# Starlette 앱 조립
# ====================================================================

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
