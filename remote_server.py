"""Remote MCP wrapper: pykrx (daily/EOD) + KIS real-time + 경제지표 + 신규 6개 도구.

Env vars:
  MCP_TOKEN       (required) secret URL path segment
  KIS_APP_KEY     (optional) 한국투자증권 Open API app key
  KIS_APP_SECRET  (optional) 한국투자증권 Open API app secret
  KIS_DOMAIN      "real" (default) or "virtual"
  FRED_API_KEY    (optional) FRED (미국 연준) API 키
  ECOS_API_KEY    (optional) 한국은행 ECOS API 키
  DART_API_KEY    (optional) DART 전자공시 API 키
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
DART_API_KEY = os.environ.get("DART_API_KEY", "").strip()

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


def _kospi200_near_month_code() -> str:
    """코스피200 선물 근월물 종목코드 생성 (101WYYMM 형식)"""
    now = datetime.datetime.now(KST)
    year  = now.year % 100
    month = now.month
    # 매달 두 번째 목요일 만기 → 당월 만기 전이면 당월, 이후면 익월
    # 간단히: 현재 날짜 기준으로 당월 두 번째 목요일 계산
    first_day = now.replace(day=1)
    # 첫 번째 목요일 찾기
    days_until_thu = (3 - first_day.weekday()) % 7
    first_thu = first_day + datetime.timedelta(days=days_until_thu)
    second_thu = first_thu + datetime.timedelta(days=7)
    if now.date() > second_thu.date():
        # 익월
        if month == 12:
            year = (year + 1) % 100
            month = 1
        else:
            month += 1
    return f"101W{year:02d}{month:02d}"


async def _dart_corp_code(ticker: str) -> str | None:
    """DART 기업코드를 종목코드로 조회"""
    if not DART_API_KEY:
        return None
    try:
        import io, zipfile
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                "https://opendart.fss.or.kr/api/corpCode.xml",
                params={"crtfc_key": DART_API_KEY},
            )
        if r.status_code != 200:
            return None
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            xml_data = z.read("CORPCODE.xml").decode("utf-8")
        import re
        pattern = rf"<stock_code>{re.escape(ticker)}</stock_code>.*?<corp_code>(\d+)</corp_code>"
        m = re.search(pattern, xml_data, re.DOTALL)
        if m:
            return m.group(1)
        # 역순 패턴도 시도
        pattern2 = rf"<corp_code>(\d+)</corp_code>.*?<stock_code>{re.escape(ticker)}</stock_code>"
        m2 = re.search(pattern2, xml_data, re.DOTALL)
        return m2.group(1) if m2 else None
    except Exception:
        return None


# ====================================================================
# 기존 도구 (현재가·미국현재가·TradingView)
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
    TradingView 기술적 분석(RSI, MACD, 이동평균, 매수/매도 시그널)을 조회합니다.

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
    한국 주식의 실시간 호가창(매수/매도 10단계)을 조회합니다 (KIS Open API).

    Args:
        symbol: 6자리 종목코드 (예: "005930" 삼성전자)

    Returns:
        매수/매도 각 10호가 가격/잔량, 총매수잔량, 총매도잔량
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
            "매도호가":     asks,
            "매수호가":     bids,
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

    Args:
        symbol:   6자리 종목코드 (예: "005930" 삼성전자)
        interval: 분 단위. 1(기본), 3, 5, 10, 15, 30, 60, 120 중 하나
        count:    반환할 캔들 개수 (최대 30)

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
            "interval_min": interval,
            "캔들수":       len(candles),
            "캔들":         candles,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "KIS (한국투자증권)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# A-3: 지수 시세
# ====================================================================

@mcp.tool()
async def get_index(index_code: str = "0001") -> dict:
    """
    국내 주요 지수의 현재 시세를 조회합니다 (KIS Open API).

    Args:
        index_code: "0001"(코스피), "1001"(코스닥), "2001"(코스피200), "0003"(KRX100)

    Returns:
        현재지수, 전일대비, 등락률, 거래량, 거래대금 등
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"error": "KIS_APP_KEY/KIS_APP_SECRET 환경변수가 설정되지 않았습니다."}
    index_names = {"0001": "코스피", "1001": "코스닥", "2001": "코스피200", "0003": "KRX100"}
    try:
        token = await _kis_token()
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHPUP02100000",
            "custtype": "P",
        }
        params = {"FID_COND_MRKT_DIV_CODE": "U", "FID_INPUT_ISCD": index_code}
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
# A-4: pykrx 확장 - 공매도 현황
# ====================================================================

@mcp.tool()
async def get_short_selling(symbol: str, fromdate: str = "", todate: str = "") -> dict:
    """
    특정 종목의 공매도 현황을 조회합니다 (pykrx).

    Args:
        symbol:   6자리 종목코드 (예: "005930" 삼성전자)
        fromdate: 시작일 "YYYYMMDD" (기본: 최근 30일 전)
        todate:   종료일 "YYYYMMDD" (기본: 오늘)

    Returns:
        날짜별 공매도량, 공매도대금, 공매도비율 리스트
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
# A-5: pykrx 확장 - 투자자별 순매수
# ====================================================================

@mcp.tool()
async def get_investor_trading(symbol: str, fromdate: str = "", todate: str = "") -> dict:
    """
    특정 종목의 투자자별 순매수 현황(외국인/기관/개인 등)을 조회합니다 (pykrx).

    Args:
        symbol:   6자리 종목코드 (예: "005930" 삼성전자)
        fromdate: 시작일 "YYYYMMDD" (기본: 최근 30일 전)
        todate:   종료일 "YYYYMMDD" (기본: 오늘)

    Returns:
        날짜별 외국인/기관/개인 등 투자자 유형별 순매수량/순매수금액
    """
    import pykrx.stock as stock

    today = datetime.datetime.now(KST).strftime("%Y%m%d")
    if not todate:
        todate = today
    if not fromdate:
        dt = datetime.datetime.now(KST) - datetime.timedelta(days=30)
        fromdate = dt.strftime("%Y%m%d")

    try:
        # 투자자별 순매수 금액 (기관/외국인/개인 등 구분)
        df = await asyncio.to_thread(
            stock.get_market_trading_value_by_investor, fromdate, todate, symbol
        )
        if df is None or df.empty:
            # 폴백: 투자자별 거래량 시도
            df = await asyncio.to_thread(
                stock.get_market_trading_volume_by_investor, fromdate, todate, symbol
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
# A-6: pykrx 확장 - 시가총액 상위 종목
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
        try:
            _bd = await asyncio.to_thread(stock.get_nearest_business_day_in_a_week, date)
            date = _bd or date
        except Exception:
            pass
        df = await asyncio.to_thread(
            stock.get_market_cap_by_ticker, date, market=market
        )
        if df is None or df.empty:
            return {"error": f"{date} {market} 시가총액 스냅샷이 비어 있습니다. (KRX가 클라우드 IP에 시장 전체 스냅샷을 제공하지 않는 제약 — 종목별 시세/OHLCV 조회는 정상입니다)"}

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
# A-7: pykrx 확장 - 등락률 상위/하위 종목
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
        try:
            _bd = await asyncio.to_thread(stock.get_nearest_business_day_in_a_week, date)
            date = _bd or date
        except Exception:
            pass
        df = await asyncio.to_thread(
            stock.get_market_ohlcv_by_ticker, date, market=market
        )
        if df is None or df.empty:
            return {"error": f"{date} {market} 등락률 스냅샷이 비어 있습니다. (KRX가 클라우드 IP에 시장 전체 스냅샷을 제공하지 않는 제약 — 종목별 시세/OHLCV 조회는 정상입니다)"}

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
# A-8: pykrx 확장 - 외국인 보유 현황
# ====================================================================

@mcp.tool()
async def get_foreign_holding(symbol: str, fromdate: str = "", todate: str = "") -> dict:
    """
    특정 종목의 외국인 보유 현황(보유량/보유율/한도소진율)을 조회합니다 (pykrx).

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
        # JSONDecodeError 등 KRX API 파싱 오류 → 단일 날짜 폴백
        try:
            today_str = datetime.datetime.now(KST).strftime("%Y%m%d")
            df_tick = await asyncio.to_thread(
                stock.get_exhaustion_rates_of_foreign_investment_by_ticker,
                today_str
            )
            if df_tick is not None and not df_tick.empty and symbol in df_tick.index:
                row = df_tick.loc[[symbol]].reset_index()
                rows = [
                    {str(c): (r[c].item() if hasattr(r[c], "item") else r[c])
                     for c in row.columns}
                    for _, r in row.iterrows()
                ]
                return {
                    "symbol":       symbol,
                    "기간":         today_str,
                    "note":         f"날짜범위 조회 오류({type(e).__name__}) — 오늘 기준 스냅샷으로 대체",
                    "데이터":       rows,
                    "조회시각_KST": _now_kst(),
                    "데이터출처":   "pykrx (한국거래소)",
                }
        except Exception:
            pass
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# B: 경제지표 - FRED (미국 연준)
# ====================================================================

FRED_SERIES_MAP = {
    "fed_rate":    ("FEDFUNDS",   "미국 연방기금금리 (%)"),
    "cpi":         ("CPIAUCSL",   "미국 소비자물가지수 (CPI, 2015=100)"),
    "core_cpi":    ("CPILFESL",   "미국 근원CPI (에너지/식품 제외)"),
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
        지표명, 단위, 최근 값 리스트 (날짜/값)

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
            meta_r = await client.get(
                "https://api.stlouisfed.org/fred/series",
                params={"series_id": fred_id, "api_key": FRED_API_KEY, "file_type": "json"},
            )
            meta = meta_r.json().get("seriess", [{}])[0] if meta_r.status_code == 200 else {}

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
# B: 경제지표 - ECOS (한국은행)
# ====================================================================

ECOS_SERIES_MAP = {
    # shortname:   (stat_code,  item_code,   label,                         cycle)
    "base_rate":    ("722Y001", "0101000",  "한국 기준금리 (%)",            "M"),
    "cpi":          ("901Y009", "0",        "한국 소비자물가지수 (CPI)",     "M"),
    "gdp":          ("200Y002", "1400",     "한국 실질 GDP 성장률 (전기대비, %)", "Q"),
    "m2":           ("101Y003", "BBHA00",   "한국 M2 광의통화 (평잔, 십억원)",   "M"),
    "usd_krw":      ("731Y001", "0000001",  "원/달러 환율 (종가, 원)",       "D"),
    "trade_balance":("301Y013", "000000",   "한국 경상수지 (백만달러)",       "M"),
    "unemployment": ("901Y027", "I61BC",    "한국 실업률 (%)",              "M"),
    "production":   ("901Y019", "I16A",     "한국 산업생산지수",            "M"),
    "10y_yield":    ("817Y002", "010200000","한국 국고채 10년 금리 (%)",      "D"),
    "3y_yield":     ("817Y002", "010190000","한국 국고채 3년 금리 (%)",       "D"),
}


def _ecos_period(cycle: str, count: int):
    """주기별 (검색시작, 검색종료) 문자열을 ECOS 포맷으로 반환."""
    now = datetime.datetime.now(KST)
    if cycle == "D":
        end   = now.strftime("%Y%m%d")
        start = (now - datetime.timedelta(days=count * 2 + 40)).strftime("%Y%m%d")
    elif cycle == "Q":
        q = (now.month - 1) // 3 + 1
        end = f"{now.year}Q{q}"
        tot = now.year * 4 + (q - 1) - (count + 2)
        start = f"{tot // 4}Q{tot % 4 + 1}"
    elif cycle == "A":
        end   = f"{now.year}"
        start = f"{now.year - (count + 1)}"
    else:  # M
        end = now.strftime("%Y%m")
        tot = now.year * 12 + (now.month - 1) - (count + 6)
        start = f"{tot // 12}{tot % 12 + 1:02d}"
    return start, end


async def _ecos_fetch(stat_code: str, item_code: str, cycle: str, count: int):
    """단일 (통계코드/항목/주기) 조회 → (rows, body)."""
    start, end = _ecos_period(cycle, count)
    url = (
        f"https://ecos.bok.or.kr/api/StatisticSearch"
        f"/{ECOS_API_KEY}/json/kr/1/1000"
        f"/{stat_code}/{cycle}/{start}/{end}/{item_code}"
    )
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url)
    if r.status_code != 200:
        return [], {"error": f"ECOS API 오류 {r.status_code}: {r.text[:200]}"}
    body = r.json()
    ss = body.get("StatisticSearch") or {}
    return (ss.get("row") or []), body


@mcp.tool()
async def get_ecos_indicator(series_id: str, count: int = 12) -> dict:
    """
    한국은행 경제통계시스템(ECOS)에서 거시경제 지표를 조회합니다.

    Args:
        series_id: ECOS 단축명 또는 "통계코드/항목코드[/주기]" 형식.
            단축명: base_rate, cpi, gdp, m2, usd_krw, trade_balance,
                   unemployment, production, 10y_yield, 3y_yield
            직접 입력: "722Y001/0101000" 또는 "731Y001/0000001/D"
                       (주기 D=일 M=월 Q=분기 A=년; 생략 시 자동 탐색)
        count: 반환할 최근 데이터 개수 (기본 12)

    Returns:
        지표명, 단위, 최근 값 리스트 (날짜/값)

    주의: ECOS_API_KEY 환경변수가 필요합니다.
    """
    if not ECOS_API_KEY:
        return {
            "error": "ECOS_API_KEY 환경변수가 설정되지 않았습니다.",
            "안내": "https://ecos.bok.or.kr -> 오픈API -> 인증키 신청 (무료) 후 Render 환경변수에 추가하세요.",
        }

    sid = series_id.strip()
    label = sid
    cycle = None

    if sid in ECOS_SERIES_MAP:
        stat_code, item_code, label, cycle = ECOS_SERIES_MAP[sid]
    elif "/" in sid:
        parts = [x.strip() for x in sid.split("/")]
        stat_code, item_code = parts[0], parts[1]
        if len(parts) >= 3 and parts[2]:
            cycle = parts[2].upper()
    else:
        return {
            "error": f"알 수 없는 series_id: '{sid}'",
            "사용가능_단축명": list(ECOS_SERIES_MAP.keys()),
            "직접입력_예시": "722Y001/0101000 또는 731Y001/0000001/D",
        }

    count = min(max(1, count), 100)

    # 지정 주기 우선, 실패 시 나머지 주기 자동 탐색
    order = [cycle] if cycle else []
    for c in ("M", "D", "Q", "A"):
        if c not in order:
            order.append(c)

    rows, last_body, used_cycle = [], None, None
    try:
        for c in order:
            try:
                rows, last_body = await _ecos_fetch(stat_code, item_code, c, count)
            except Exception:
                rows = []
            if rows:
                used_cycle = c
                break
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}

    if not rows:
        return {
            "error": "데이터가 없습니다. 통계코드/항목코드/주기를 확인하세요.",
            "raw":   last_body,
        }

    data = []
    for row in rows[-count:]:
        data.append({
            "날짜": row.get("TIME"),
            "값":   _f(row.get("DATA_VALUE")),
            "단위": row.get("UNIT_NAME"),
        })
    data.reverse()

    unit      = rows[0].get("UNIT_NAME", "")
    stat_name = rows[0].get("STAT_NAME", label)
    item_name = rows[0].get("ITEM_NAME1", "")

    return {
        "series_id":    f"{stat_code}/{item_code}",
        "주기":         used_cycle,
        "지표명":       label if label != sid else f"{stat_name} - {item_name}",
        "단위":         unit,
        "데이터":       data,
        "조회시각_KST": _now_kst(),
        "데이터출처":   "ECOS (한국은행 경제통계시스템)",
    }


# ====================================================================
# C: DART 전자공시
# ====================================================================

@mcp.tool()
async def get_dart_disclosure(ticker: str, count: int = 10) -> dict:
    """
    DART 전자공시에서 특정 상장사의 최근 공시 목록을 조회합니다.

    Args:
        ticker: 6자리 종목코드 (예: "005930" 삼성전자)
        count:  반환할 최근 공시 수 (기본 10, 최대 40)

    Returns:
        공시 제목, 공시일자, 공시 유형 리스트

    주의: DART_API_KEY 환경변수가 필요합니다.
    """
    if not DART_API_KEY:
        return {
            "error": "DART_API_KEY 환경변수가 설정되지 않았습니다.",
            "안내": "https://opendart.fss.or.kr 에서 무료 발급 후 Render 환경변수에 추가하세요.",
        }

    corp_code = await _dart_corp_code(ticker)
    if not corp_code:
        return {"error": f"'{ticker}' 종목의 DART 기업코드를 찾을 수 없습니다."}

    count = min(max(1, count), 40)

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(
                "https://opendart.fss.or.kr/api/list.json",
                params={
                    "crtfc_key": DART_API_KEY,
                    "corp_code": corp_code,
                    "bgn_de": (datetime.datetime.now(KST) - datetime.timedelta(days=180)).strftime("%Y%m%d"),
                    "end_de": datetime.datetime.now(KST).strftime("%Y%m%d"),
                    "page_no":   "1",
                    "page_count": str(count),
                },
            )
        if r.status_code != 200:
            return {"error": f"DART API 오류 {r.status_code}: {r.text[:300]}"}

        body = r.json()
        if body.get("status") != "000":
            return {"error": body.get("message", "DART API 오류"), "status": body.get("status")}

        items = body.get("list", [])
        disclosures = []
        for item in items:
            disclosures.append({
                "공시일자":   item.get("rcept_dt"),
                "공시 유형":  item.get("pblntf_ty"),
                "공시 제목":  item.get("report_nm"),
                "공시번호":   item.get("rcept_no"),
                "제출인명":   item.get("flr_nm"),
            })

        return {
            "symbol":       ticker,
            "corp_code":    corp_code,
            "공시수":       len(disclosures),
            "공시목록":     disclosures,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "DART (금융감독원 전자공시시스템)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@mcp.tool()
async def get_dart_financial(ticker: str, year: int = 0, report_type: str = "11011") -> dict:
    """
    DART에서 특정 상장사의 재무제표 데이터를 조회합니다.

    Args:
        ticker:      6자리 종목코드 (예: "005930" 삼성전자)
        year:        사업연도 (기본: 전년도, 예: 2023)
        report_type: "11011"(사업보고서, 기본), "11012"(반기보고서), "11013"(1분기), "11014"(3분기)

    Returns:
        매출액, 영업이익, 당기순이익 등 주요 재무지표

    주의: DART_API_KEY 환경변수가 필요합니다.
    """
    if not DART_API_KEY:
        return {
            "error": "DART_API_KEY 환경변수가 설정되지 않았습니다.",
            "안내": "https://opendart.fss.or.kr 에서 무료 발급 후 Render 환경변수에 추가하세요.",
        }

    if year == 0:
        year = datetime.datetime.now(KST).year - 1

    corp_code = await _dart_corp_code(ticker)
    if not corp_code:
        return {"error": f"'{ticker}' 종목의 DART 기업코드를 찾을 수 없습니다."}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(
                "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json",
                params={
                    "crtfc_key": DART_API_KEY,
                    "corp_code": corp_code,
                    "bsns_year": str(year),
                    "reprt_code": report_type,
                    "fs_div": "CFS",  # 연결재무제표
                },
            )
        if r.status_code != 200:
            return {"error": f"DART API 오류 {r.status_code}: {r.text[:300]}"}

        body = r.json()
        if body.get("status") != "000":
            # 연결재무제표 없으면 개별재무제표 시도
            async with httpx.AsyncClient(timeout=20) as client:
                r2 = await client.get(
                    "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json",
                    params={
                        "crtfc_key": DART_API_KEY,
                        "corp_code": corp_code,
                        "bsns_year": str(year),
                        "reprt_code": report_type,
                        "fs_div": "OFS",  # 개별재무제표
                    },
                )
            body = r2.json()
            if body.get("status") != "000":
                return {"error": body.get("message", "DART 재무제표 조회 오류"), "status": body.get("status")}

        items = body.get("list", [])

        # 주요 계정과목만 필터링
        key_accounts = {
            "매출액", "영업이익", "당기순이익", "자산총계", "부채총계",
            "자본총계", "영업활동으로인한현금흐름", "기본주당순이익(손실)",
        }
        result = {}
        for item in items:
            acct = item.get("account_nm", "")
            if any(k in acct for k in key_accounts) or acct in key_accounts:
                val = item.get("thstrm_amount")
                result[acct] = {
                    "당기금액": val,
                    "전기금액": item.get("frmtrm_amount"),
                    "단위": "원",
                }

        return {
            "symbol":       ticker,
            "corp_code":    corp_code,
            "사업연도":     year,
            "보고서유형":   {"11011": "사업보고서", "11012": "반기보고서", "11013": "1분기", "11014": "3분기"}.get(report_type, report_type),
            "재무제표구분": "연결재무제표",
            "주요재무지표": result,
            "전체항목수":   len(items),
            "조회시각_KST": _now_kst(),
            "데이터출처":   "DART (금융감독원 전자공시시스템)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# D: yfinance - 미국 주식 정보
# ====================================================================

@mcp.tool()
async def get_us_stock_info(symbol: str) -> dict:
    """
    Yahoo Finance에서 미국 주식의 기업 정보 및 재무 지표를 조회합니다.

    Args:
        symbol: 미국 종목 티커 (예: "AAPL", "TSLA", "NVDA", "MSFT")

    Returns:
        기업명, 시가총액, PER, PBR, EPS, 배당수익률, 52주 최고/최저, 애널리스트 목표가 등
    """
    try:
        import yfinance as yf
    except ImportError:
        return {"error": "yfinance가 설치되지 않았습니다. requirements.txt에 yfinance를 추가하세요."}

    sym = symbol.strip().upper()

    def _fetch():
        ticker = yf.Ticker(sym)
        return ticker.info

    try:
        info = await asyncio.to_thread(_fetch)
        if not info or info.get("regularMarketPrice") is None:
            return {"error": f"'{sym}' 종목 정보를 가져올 수 없습니다."}

        return {
            "symbol":          sym,
            "기업명":          info.get("longName") or info.get("shortName"),
            "섹터":            info.get("sector"),
            "업종":            info.get("industry"),
            "현재가_USD":      info.get("regularMarketPrice") or info.get("currentPrice"),
            "시가총액_USD":    info.get("marketCap"),
            "PER":             info.get("trailingPE"),
            "Forward_PER":     info.get("forwardPE"),
            "PBR":             info.get("priceToBook"),
            "EPS":             info.get("trailingEps"),
            "52주_최고":       info.get("fiftyTwoWeekHigh"),
            "52주_최저":       info.get("fiftyTwoWeekLow"),
            "배당수익률":      info.get("dividendYield"),
            "목표가_애널리스트": info.get("targetMeanPrice"),
            "권고의견":        info.get("recommendationKey"),
            "베타":            info.get("beta"),
            "직원수":          info.get("fullTimeEmployees"),
            "조회시각_KST":    _now_kst(),
            "데이터출처":      "Yahoo Finance (yfinance)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@mcp.tool()
async def get_us_stock_ohlcv(symbol: str, period: str = "3mo", interval: str = "1d") -> dict:
    """
    Yahoo Finance에서 미국 주식의 OHLCV(시고저종거래량) 데이터를 조회합니다.

    Args:
        symbol:   미국 종목 티커 (예: "AAPL", "SPY", "QQQ")
        period:   기간. "1d","5d","1mo","3mo"(기본),"6mo","1y","2y","5y","10y","ytd","max"
        interval: 봉 간격. "1m","2m","5m","15m","30m","60m","90m","1h","1d"(기본),"5d","1wk","1mo","3mo"

    Returns:
        날짜별 시가, 고가, 저가, 종가, 거래량 리스트
    """
    try:
        import yfinance as yf
    except ImportError:
        return {"error": "yfinance가 설치되지 않았습니다. requirements.txt에 yfinance를 추가하세요."}

    sym = symbol.strip().upper()

    def _fetch():
        ticker = yf.Ticker(sym)
        df = ticker.history(period=period, interval=interval)
        return df

    try:
        df = await asyncio.to_thread(_fetch)
        if df is None or df.empty:
            return {"error": f"'{sym}' 종목의 가격 데이터를 가져올 수 없습니다."}

        df = df.reset_index()
        rows = []
        for _, row in df.iterrows():
            rows.append({
                "날짜":   str(row.get("Date", row.get("Datetime", ""))),
                "시가":   round(float(row["Open"]), 4) if row.get("Open") is not None else None,
                "고가":   round(float(row["High"]), 4) if row.get("High") is not None else None,
                "저가":   round(float(row["Low"]), 4) if row.get("Low") is not None else None,
                "종가":   round(float(row["Close"]), 4) if row.get("Close") is not None else None,
                "거래량": int(row["Volume"]) if row.get("Volume") is not None else None,
            })

        return {
            "symbol":       sym,
            "period":       period,
            "interval":     interval,
            "데이터수":     len(rows),
            "데이터":       rows,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "Yahoo Finance (yfinance)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# E: 채권/국채 수익률 (pykrx bond)
# ====================================================================

@mcp.tool()
async def get_bond_yield(fromdate: str = "", todate: str = "") -> dict:
    """
    한국 국채 OTC 수익률(3년/5년/10년/20년/30년)을 조회합니다 (pykrx bond).

    Args:
        fromdate: 시작일 "YYYYMMDD" (기본: 최근 30일 전)
        todate:   종료일 "YYYYMMDD" (기본: 오늘)

    Returns:
        날짜별 국채 만기별 수익률 리스트
    """
    today = datetime.datetime.now(KST).strftime("%Y%m%d")
    if not todate:
        todate = today
    if not fromdate:
        dt = datetime.datetime.now(KST) - datetime.timedelta(days=30)
        fromdate = dt.strftime("%Y%m%d")

    try:
        from pykrx import bond

        df = await asyncio.to_thread(
            bond.get_otc_treasury_yields, todate
        )
        if df is None or df.empty:
            return {"error": "국채 수익률 데이터가 없습니다."}

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
            "기준일":       todate,
            "데이터":       rows,
            "설명":         "한국 국채 OTC 수익률 (3/5/10/20/30년, 단일일 스냅샷)",
            "조회시각_KST": _now_kst(),
            "데이터출처":   "pykrx bond (한국거래소)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# F: 기술적 지표 (yfinance 기반)
# ====================================================================

@mcp.tool()
async def get_technical_indicators(symbol: str, period: str = "6mo") -> dict:
    """
    미국/글로벌 주식의 주요 기술적 지표를 계산합니다 (yfinance 데이터 기반).
    RSI, MACD, 볼린저밴드, 이동평균선 등을 반환합니다.

    Args:
        symbol: 미국 종목 티커 (예: "AAPL", "SPY", "QQQ")
        period: 계산에 사용할 데이터 기간. "3mo","6mo"(기본),"1y","2y"

    Returns:
        RSI, MACD, 볼린저밴드, SMA/EMA, 현재가 등
    """
    try:
        import yfinance as yf
    except ImportError:
        return {"error": "yfinance가 설치되지 않았습니다."}

    sym = symbol.strip().upper()

    def _fetch():
        ticker = yf.Ticker(sym)
        df = ticker.history(period=period, interval="1d")
        return df

    try:
        df = await asyncio.to_thread(_fetch)
        if df is None or df.empty:
            return {"error": f"'{sym}' 데이터를 가져올 수 없습니다."}

        close = df["Close"]
        n = len(close)
        if n < 20:
            return {"error": f"데이터가 부족합니다. 최소 20일 필요, 현재 {n}일"}

        # RSI (14일)
        delta = close.diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        # MACD
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - signal_line

        # 볼린저밴드 (20일)
        sma20 = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        bb_upper = sma20 + 2 * std20
        bb_lower = sma20 - 2 * std20

        # 이동평균
        sma50 = close.rolling(50).mean() if n >= 50 else None
        sma200 = close.rolling(200).mean() if n >= 200 else None
        ema20 = close.ewm(span=20, adjust=False).mean()

        def last(series):
            if series is None:
                return None
            v = series.iloc[-1]
            return round(float(v), 4) if not (v != v) else None  # NaN check

        current_price = last(close)
        bb_up = last(bb_upper)
        bb_lo = last(bb_lower)
        bb_mid = last(sma20)

        return {
            "symbol":       sym,
            "현재가":       current_price,
            "RSI_14":       last(rsi),
            "MACD":         last(macd_line),
            "MACD_signal":  last(signal_line),
            "MACD_hist":    last(macd_hist),
            "볼린저_상단":  bb_up,
            "볼린저_중간":  bb_mid,
            "볼린저_하단":  bb_lo,
            "볼린저_%B":    round((current_price - bb_lo) / (bb_up - bb_lo), 4) if bb_up and bb_lo and current_price else None,
            "SMA20":        last(sma20),
            "EMA20":        last(ema20),
            "SMA50":        last(sma50) if n >= 50 else None,
            "SMA200":       last(sma200) if n >= 200 else None,
            "데이터기간":   period,
            "데이터수":     n,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "Yahoo Finance (yfinance) - 직접 계산",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# G: 선물/옵션 시세 (KIS)
# ====================================================================

@mcp.tool()
async def get_futures_price(symbol: str = "auto") -> dict:
    """
    코스피200 선물 근월물 또는 지정 선물 종목의 현재가를 조회합니다 (KIS Open API).

    Args:
        symbol: 선물 종목코드 (기본 "auto": 코스피200 근월물 자동). 예: "101W2506"

    Returns:
        현재가, 전일대비, 등락률, 이론가, 괴리율, 거래량 등
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"error": "KIS_APP_KEY/KIS_APP_SECRET 환경변수가 설정되지 않았습니다."}

    if symbol.strip().lower() == "auto":
        futs_code = _kospi200_near_month_code()
    else:
        futs_code = symbol.strip()

    try:
        token = await _kis_token()
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHMIF10000000",
            "custtype": "P",
        }
        params = {"FID_COND_MRKT_DIV_CODE": "F", "FID_INPUT_ISCD": futs_code}
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(
                f"{KIS_BASE}/uapi/domestic-futureoption/v1/quotations/inquire-price",
                headers=headers,
                params=params,
            )
        d = r.json()
        o = d.get("output") or {}
        if not o:
            return {"error": d.get("msg1", "조회 실패"), "rt_cd": d.get("rt_cd"), "종목코드": futs_code}

        sign = {"1": "상한", "2": "상승", "3": "보합", "4": "하한", "5": "하락"}
        return {
            "종목코드":     futs_code,
            "현재가":       _f(o.get("futs_prpr")),
            "전일대비":     _f(o.get("prdy_vrss")),
            "등락":         sign.get(o.get("prdy_vrss_sign"), o.get("prdy_vrss_sign")),
            "등락률":       _f(o.get("prdy_ctrt")),
            "시가":         _f(o.get("futs_oprc")),
            "고가":         _f(o.get("futs_hgpr")),
            "저가":         _f(o.get("futs_lwpr")),
            "누적거래량":   _i(o.get("acml_vol")),
            "이론가":       _f(o.get("theo_pric")),
            "기초지수":     _f(o.get("bsic_val")),
            "조회시각_KST": _now_kst(),
            "데이터출처":   "KIS (한국투자증권)",
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@mcp.tool()
async def get_options_price(
    expiry: str = "",
    option_type: str = "C",
    strike: float = 0.0,
) -> dict:
    """
    코스피200 옵션 시세를 조회합니다 (KIS Open API).

    Args:
        expiry:      만기월 "YYMM" (기본: 현재 근월물, 예: "2506")
        option_type: "C"(콜, 기본) 또는 "P"(풋)
        strike:      행사가격 (0이면 근방 행사가 상위 5개 반환)

    Returns:
        옵션 현재가, 이론가, 델타, 감마, 세타, 베가, 내재변동성 등
    """
    if not KIS_APP_KEY or not KIS_APP_SECRET:
        return {"error": "KIS_APP_KEY/KIS_APP_SECRET 환경변수가 설정되지 않았습니다."}

    if not expiry:
        now = datetime.datetime.now(KST)
        expiry = f"{now.year % 100:02d}{now.month:02d}"

    opt_type = option_type.strip().upper()
    if opt_type not in ("C", "P"):
        return {"error": "option_type은 'C'(콜) 또는 'P'(풋)이어야 합니다."}

    # 옵션 종목코드: 2(콜)/3(풋) + YYMM + strike(XXXXX, 소수점 제거, 5자리)
    def make_opt_code(strike_val: float) -> str:
        t = "2" if opt_type == "C" else "3"
        s = f"{int(strike_val * 100):05d}"
        return f"{t}{expiry}{s}"

    try:
        token = await _kis_token()

        async def _query(opt_code: str) -> dict:
            headers = {
                "authorization": f"Bearer {token}",
                "appkey": KIS_APP_KEY,
                "appsecret": KIS_APP_SECRET,
                "tr_id": "FHMIF10000000",
                "custtype": "P",
            }
            params = {"FID_COND_MRKT_DIV_CODE": "O", "FID_INPUT_ISCD": opt_code}
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.get(
                    f"{KIS_BASE}/uapi/domestic-futureoption/v1/quotations/inquire-price",
                    headers=headers,
                    params=params,
                )
            return r.json()

        if strike > 0:
            opt_code = make_opt_code(strike)
            d = await _query(opt_code)
            o = d.get("output") or {}
            if not o:
                return {"error": d.get("msg1", "조회 실패"), "종목코드": opt_code}
            return {
                "종목코드":     opt_code,
                "만기월":       expiry,
                "옵션유형":     "콜" if opt_type == "C" else "풋",
                "행사가":       strike,
                "현재가":       _f(o.get("futs_prpr")),
                "전일대비":     _f(o.get("prdy_vrss")),
                "이론가":       _f(o.get("theo_pric")),
                "내재변동성":   _f(o.get("impv")),
                "델타":         _f(o.get("delta")),
                "감마":         _f(o.get("gama")),
                "세타":         _f(o.get("theta")),
                "베가":         _f(o.get("vega")),
                "조회시각_KST": _now_kst(),
                "데이터출처":   "KIS (한국투자증권)",
            }
        else:
            return {
                "error": "행사가(strike)를 지정해야 합니다.",
                "안내": "예: strike=360.0, option_type='C', expiry='2506'",
            }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ====================================================================
# H: 주식 뉴스 (네이버 금융)
# ====================================================================

@mcp.tool()
async def get_stock_news(symbol: str, count: int = 10) -> dict:
    """
    네이버 금융에서 특정 주식 종목의 최신 뉴스를 가져옵니다.

    Args:
        symbol: 6자리 종목코드 (예: "005930" 삼성전자)
        count:  반환할 뉴스 개수 (기본 10, 최대 20)

    Returns:
        뉴스 제목, 날짜, 언론사, 링크 리스트
    """
    count = min(max(1, count), 20)
    url = f"https://finance.naver.com/item/news_news.naver?code={symbol}&page=1&sm=title_entity_id.basic&clusterId="

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://finance.naver.com/",
            "Accept-Language": "ko-KR,ko;q=0.9",
        }
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            r = await client.get(url, headers=headers)

        if r.status_code != 200:
            return {"error": f"네이버 금융 응답 오류: {r.status_code}"}

        # 간단한 파싱 (BeautifulSoup 없이 정규식 사용)
        import re
        content = r.text

        # 뉴스 제목과 링크 추출
        pattern = r'<a[^>]+href="(/item/news_read\.(?:nhn|naver)\?[^"]+)"[^>]*>\s*([^<]+?)\s*</a>'
        matches = re.findall(pattern, content)

        # 날짜 및 언론사 추출
        date_pattern = r'<td class="date">([^<]+)</td>'
        office_pattern = r'<td class="info">([^<]+)</td>'
        dates = re.findall(date_pattern, content)
        offices = re.findall(office_pattern, content)

        news_list = []
        for i, (link, title) in enumerate(matches[:count]):
            title = title.strip()
            if not title or len(title) < 5:
                continue
            news_list.append({
                "제목":   title,
                "날짜":   dates[i].strip() if i < len(dates) else "",
                "언론사": offices[i].strip() if i < len(offices) else "",
                "링크":   f"https://finance.naver.com{link}",
            })

        if not news_list:
            return {
                "error": "뉴스를 파싱할 수 없습니다.",
                "안내": "네이버 금융 페이지 구조가 변경되었을 수 있습니다.",
                "symbol": symbol,
            }

        return {
            "symbol":       symbol,
            "뉴스수":       len(news_list),
            "뉴스":         news_list,
            "조회시각_KST": _now_kst(),
            "데이터출처":   "네이버 금융",
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
