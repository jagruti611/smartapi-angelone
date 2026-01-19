import time
from typing import Dict, Any, List, Optional

from SmartApi.smartWebSocketV2 import SmartWebSocketV2

from .config import (
    WS_WARMUP_SEC, STRIKES_AROUND, MAX_WS_SUBS, SUBSCRIBE_MODE,
    STREAM_EQ, STREAM_OPT,
    STREAM_MAXLEN_EQ, STREAM_MAXLEN_OPT,
)
from .utils import now_ms, paise_to_rupees
from .redis_store import RedisStore
from .scripmaster import load_scripmaster, resolve_eq_tokens, build_atm_option_tokens


class MarketDataProducer:
    def __init__(self, auth_token: str, feed_token: str, client_code: str, api_key: str, symbols: list[str]):
        self.auth_token = auth_token
        self.feed_token = feed_token
        self.client_code = client_code
        self.api_key = api_key
        self.symbols = symbols

        self.rs = RedisStore()
        self.df = load_scripmaster()

        self.eq_map = resolve_eq_tokens(self.df, symbols)
        self.eq_token_to_symbol = {v["token"]: k for k, v in self.eq_map.items()}

        # option token -> meta
        self.opt_meta: Dict[str, dict] = {}

        # ✅ This will be published to Redis for the greeks poller
        self.active_expiry_by_underlying: Dict[str, str] = {}

        self.spot_ltp: Dict[str, float] = {}
        self.ws_open_t: Optional[float] = None
        self.options_subscribed = False

        self.sws = SmartWebSocketV2(
            auth_token=self.auth_token,
            api_key=self.api_key,
            client_code=self.client_code,
            feed_token=self.feed_token,
            max_retry_attempt=10,
            retry_strategy=0,
            retry_delay=2,
            retry_multiplier=2,
            retry_duration=60,
        )

        self.EXCH_NSE = getattr(SmartWebSocketV2, "NSE_CM", 1)
        self.EXCH_NFO = getattr(SmartWebSocketV2, "NSE_FO", 2)

        # mode: SNAP_QUOTE recommended for OHLC/vol/OI
        self.MODE_LTP = getattr(SmartWebSocketV2, "LTP", 1)
        self.MODE_QUOTE = getattr(SmartWebSocketV2, "QUOTE", 2)
        self.MODE_SNAP = getattr(SmartWebSocketV2, "SNAP_QUOTE", 3)

        if SUBSCRIBE_MODE == "LTP":
            self.mode_eq = self.MODE_LTP
            self.mode_opt = self.MODE_LTP
        elif SUBSCRIBE_MODE == "QUOTE":
            self.mode_eq = self.MODE_QUOTE
            self.mode_opt = self.MODE_QUOTE
        else:
            self.mode_eq = self.MODE_SNAP
            self.mode_opt = self.MODE_SNAP

        self.sws.on_open = self.on_open
        self.sws.on_data = self.on_data
        self.sws.on_error = self.on_error
        self.sws.on_close = self.on_close

    def start(self):
        if not self.eq_map:
            raise RuntimeError("No NSE EQ tokens resolved from ScripMaster.")
        print(f"[WS] EQ tokens resolved: {len(self.eq_map)}")
        self.sws.connect()

    def on_open(self, wsapp):
        self.ws_open_t = time.time()
        eq_tokens = [info["token"] for info in self.eq_map.values()]

        # store meta in redis
        for sym, info in self.eq_map.items():
            self.rs.hset_meta(f"meta:eq:{info['token']}", {
                "symbol": sym,
                "tradingsymbol": info["tradingsymbol"],
                "exchange": "NSE",
            })

        token_list = [{"exchangeType": self.EXCH_NSE, "tokens": eq_tokens}]
        self.sws.subscribe(correlation_id="EQ01", mode=self.mode_eq, token_list=token_list)
        print(f"[WS] opened; subscribed EQ={len(eq_tokens)} mode={SUBSCRIBE_MODE}")

    def on_error(self, wsapp, error):
        print("[WS] error:", error)

    def on_close(self, wsapp):
        print("[WS] closed")

    def _publish_active_expiry(self):
        """
        ✅ Publish active expiries for greeks poller to Redis.
        """
        self.rs.hset_meta("md:active_expiry", self.active_expiry_by_underlying)
        self.rs.set_latest("md:active_expiry:ts_ms", str(now_ms()), ex_sec=3600)

    def _maybe_subscribe_options(self):
        if self.options_subscribed:
            return
        if self.ws_open_t is None:
            return

        elapsed = time.time() - self.ws_open_t
        enough = (len(self.spot_ltp) >= max(10, int(0.5 * len(self.eq_map)))) or (elapsed >= WS_WARMUP_SEC)
        if not enough:
            return

        # build option token plan
        planned: List[dict] = []
        for sym in self.symbols:
            if sym not in self.spot_ltp:
                continue

            contracts, expiry_iso = build_atm_option_tokens(self.df, sym, self.spot_ltp[sym], STRIKES_AROUND)
            if not contracts:
                continue

            if expiry_iso:
                self.active_expiry_by_underlying[sym] = expiry_iso

            planned.extend(contracts)

        # dedupe by token
        seen = set()
        unique = []
        for c in planned:
            t = c["token"]
            if t not in seen:
                seen.add(t)
                unique.append(c)

        eq_count = len(self.eq_map)
        total = eq_count + len(unique)

        if total > MAX_WS_SUBS:
            cap = max(0, MAX_WS_SUBS - eq_count)
            unique = unique[:cap]
            print(f"[WS] capped option tokens to {len(unique)} to stay under MAX_WS_SUBS={MAX_WS_SUBS}")

        if not unique:
            print("[WS] no option contracts planned (many symbols may not have options)")
            self.options_subscribed = True

            # ✅ publish active expiries (even if partial/empty)
            self._publish_active_expiry()
            return

        # store opt meta in redis + memory
        tokens = []
        for c in unique:
            tok = c["token"]
            self.opt_meta[tok] = c
            tokens.append(tok)
            self.rs.hset_meta(f"meta:opt:{tok}", {
                "underlying": c["underlying"],
                "tradingsymbol": c["tradingsymbol"],
                "expiry": c["expiry"],
                "strike": str(c["strike"]),
                "cp": c["cp"],
                "exchange": "NFO",
            })

        # subscribe in batches
        BATCH = 50
        for i in range(0, len(tokens), BATCH):
            batch = tokens[i:i + BATCH]
            token_list = [{"exchangeType": self.EXCH_NFO, "tokens": batch}]
            self.sws.subscribe(correlation_id=f"OPT{i//BATCH:02d}", mode=self.mode_opt, token_list=token_list)

        self.options_subscribed = True

        # ✅ publish active expiries for greeks poller
        self._publish_active_expiry()

        print(f"[WS] subscribed OPT={len(tokens)} mode={SUBSCRIBE_MODE} (EQ={eq_count}, total={eq_count+len(tokens)})")

    def _emit_eq(self, sym: str, tok: str, data: Dict[str, Any]):
        ltp = paise_to_rupees(data.get("last_traded_price"))
        if ltp is not None:
            self.spot_ltp[sym] = ltp

        payload = {
            "ts_recv": str(now_ms()),
            "ts_exch": str(data.get("exchange_timestamp") or ""),
            "token": tok,
            "symbol": sym,
            "ltp": str(ltp if ltp is not None else ""),
            "o": str(paise_to_rupees(data.get("open_price_of_the_day")) or ""),
            "h": str(paise_to_rupees(data.get("high_price_of_the_day")) or ""),
            "l": str(paise_to_rupees(data.get("low_price_of_the_day")) or ""),
            "c": str(paise_to_rupees(data.get("closed_price")) or ""),
            "vol": str(data.get("volume_trade_for_the_day") or ""),
            "tbq": str(data.get("total_buy_quantity") or ""),
            "tsq": str(data.get("total_sell_quantity") or ""),
        }
        self.rs.xadd(STREAM_EQ, payload, maxlen=STREAM_MAXLEN_EQ)

    def _emit_opt(self, tok: str, data: Dict[str, Any]):
        meta = self.opt_meta.get(tok)
        if not meta:
            return

        payload = {
            "ts_recv": str(now_ms()),
            "ts_exch": str(data.get("exchange_timestamp") or ""),
            "token": tok,
            "underlying": meta["underlying"],
            "tradingsymbol": meta["tradingsymbol"],
            "expiry": meta["expiry"],
            "strike": str(meta["strike"]),
            "cp": meta["cp"],
            "ltp": str(paise_to_rupees(data.get("last_traded_price")) or ""),
            "oi": str(data.get("open_interest") or ""),
            "vol": str(data.get("volume_trade_for_the_day") or ""),
            "o": str(paise_to_rupees(data.get("open_price_of_the_day")) or ""),
            "h": str(paise_to_rupees(data.get("high_price_of_the_day")) or ""),
            "l": str(paise_to_rupees(data.get("low_price_of_the_day")) or ""),
            "c": str(paise_to_rupees(data.get("closed_price")) or ""),
            "tbq": str(data.get("total_buy_quantity") or ""),
            "tsq": str(data.get("total_sell_quantity") or ""),
        }
        self.rs.xadd(STREAM_OPT, payload, maxlen=STREAM_MAXLEN_OPT)

    def on_data(self, wsapp, data: Dict[str, Any]):
        tok = str(data.get("token", ""))

        # equity tick
        if tok in self.eq_token_to_symbol:
            sym = self.eq_token_to_symbol[tok]
            self._emit_eq(sym, tok, data)
            self._maybe_subscribe_options()
            return

        # option tick
        if tok in self.opt_meta:
            self._emit_opt(tok, data)
            return
