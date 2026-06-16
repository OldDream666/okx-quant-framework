"""Unit tests for Module 2: Exchange Gateway.

Covers:
  - Data models: from_okx() parsing, field defaults, error types
  - _okx_bar(): timeframe conversion
  - RESTClient: request construction, signing, 429 retry, error parsing
  - WebSocketClient: message dispatch, callback invocation, parse_push
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from okx_quant.config.auth import OKXAuth
from okx_quant.config.settings import OKXConfig
from okx_quant.gateway.rest_client import RESTClient, _okx_bar
from okx_quant.gateway.ws_client import WebSocketClient
from okx_quant.models.market import (
    AccountData,
    BarData,
    InstrumentData,
    InstrumentType,
    OKXAPIError,
    OrderData,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionData,
    PositionSide,
    TickData,
)


# ======================================================================
# Helpers
# ======================================================================

def _make_config() -> OKXConfig:
    return OKXConfig(api_key="ak", secret_key="sk", passphrase="pp")


def _make_auth() -> OKXAuth:
    return OKXAuth(_make_config())


# ======================================================================
# _okx_bar helper
# ======================================================================


class TestOkxBar:
    """Test timeframe string conversion."""

    def test_minutes_lowercase_kept(self):
        assert _okx_bar("1m") == "1m"
        assert _okx_bar("5m") == "5m"
        assert _okx_bar("15m") == "15m"
        assert _okx_bar("30m") == "30m"

    def test_hours_uppercased(self):
        assert _okx_bar("1h") == "1H"
        assert _okx_bar("4h") == "4H"

    def test_days_uppercased(self):
        assert _okx_bar("1d") == "1D"
        assert _okx_bar("1w") == "1W"
        assert _okx_bar("1M") == "1M"

    def test_already_uppercase_unchanged(self):
        assert _okx_bar("1H") == "1H"
        assert _okx_bar("4H") == "4H"

    def test_whitespace_stripped(self):
        assert _okx_bar(" 1H ") == "1H"
        assert _okx_bar(" 5m ") == "5m"


# ======================================================================
# Data Models — from_okx parsing
# ======================================================================


class TestTickData:

    RAW = {
        "instId": "BTC-USDT",
        "last": "63000.5",
        "bidPx": "62999.0",
        "askPx": "63001.0",
        "vol24h": "1234.56",
        "high24h": "63500.0",
        "low24h": "62000.0",
        "sodUtc8": "62500.0",
        "ts": "1718448000000",
    }

    def test_from_okx(self):
        tick = TickData.from_okx(self.RAW)
        assert tick.symbol == "BTC-USDT"
        assert tick.last == 63000.5
        assert tick.bid == 62999.0
        assert tick.ask == 63001.0
        assert tick.volume24h == 1234.56
        assert tick.high24h == 63500.0
        assert tick.low24h == 62000.0
        assert tick.timestamp == 1718448000000

    def test_change24h_calculation(self):
        tick = TickData.from_okx(self.RAW)
        # (63000.5 - 62500) / 62500 = 0.008008
        assert abs(tick.change24h - 0.008008) < 1e-5

    def test_frozen(self):
        tick = TickData.from_okx(self.RAW)
        with pytest.raises(AttributeError):
            tick.last = 0  # type: ignore[misc]


class TestBarData:

    def test_from_okx(self):
        # OKX format: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        raw = ["1718448000000", "63000", "63100", "62900", "63050", "100", "6305000", "6305000", "1"]
        bar = BarData.from_okx(raw, "BTC-USDT")
        assert bar.symbol == "BTC-USDT"
        assert bar.open == 63000.0
        assert bar.high == 63100.0
        assert bar.low == 62900.0
        assert bar.close == 63050.0
        assert bar.volume == 100.0
        assert bar.timestamp == 1718448000000
        assert bar.confirmed is True

    def test_unconfirmed_bar(self):
        raw = ["1718448000000", "63000", "63100", "62900", "63050", "100", "0", "0", "0"]
        bar = BarData.from_okx(raw, "BTC-USDT")
        assert bar.confirmed is False


class TestOrderData:

    RAW = {
        "ordId": "12345",
        "clOrdId": "my_order_1",
        "instId": "ETH-USDT",
        "side": "buy",
        "ordType": "limit",
        "px": "3000.5",
        "sz": "1.5",
        "accFillSz": "0.5",
        "avgPx": "3000.0",
        "state": "partially_filled",
        "fee": "-0.15",
        "feeCcy": "USDT",
        "cTime": "1718448000000",
        "uTime": "1718448100000",
    }

    def test_from_okx(self):
        order = OrderData.from_okx(self.RAW)
        assert order.order_id == "12345"
        assert order.symbol == "ETH-USDT"
        assert order.side == OrderSide.BUY
        assert order.order_type == OrderType.LIMIT
        assert order.price == 3000.5
        assert order.quantity == 1.5
        assert order.filled_qty == 0.5
        assert order.status == OrderStatus.PARTIALLY_FILLED
        assert order.fee == -0.15

    def test_market_order(self):
        raw = {**self.RAW, "ordType": "market", "px": "0"}
        order = OrderData.from_okx(raw)
        assert order.order_type == OrderType.MARKET
        assert order.price == 0.0


class TestPositionData:

    RAW = {
        "instId": "BTC-USDT-SWAP",
        "posSide": "long",
        "pos": "0.1",
        "avgPx": "62000",
        "upl": "100.5",
        "lever": "10",
        "mgnMode": "cross",
        "liqPx": "55000",
        "margin": "620",
        "cTime": "1718448000000",
    }

    def test_from_okx_long(self):
        pos = PositionData.from_okx(self.RAW)
        assert pos.symbol == "BTC-USDT-SWAP"
        assert pos.side == PositionSide.LONG
        assert pos.quantity == 0.1
        assert pos.unrealized_pnl == 100.5
        assert pos.leverage == 10.0

    def test_net_negative_is_short(self):
        raw = {**self.RAW, "posSide": "net", "pos": "-0.05"}
        pos = PositionData.from_okx(raw)
        assert pos.side == PositionSide.SHORT
        assert pos.quantity == -0.05

    def test_net_positive_is_long(self):
        raw = {**self.RAW, "posSide": "net", "pos": "0.05"}
        pos = PositionData.from_okx(raw)
        assert pos.side == PositionSide.LONG


class TestAccountData:

    RAW = {
        "totalEq": "10000.5",
        "availBal": "8000.0",
        "mgnRatio": "1.5",
        "details": [],
    }

    def test_from_okx_balance(self):
        acc = AccountData.from_okx_balance(self.RAW)
        assert acc.total_equity == 10000.5
        assert acc.available_balance == 8000.0
        assert acc.margin_ratio == 1.5
        assert acc.positions == []


class TestInstrumentData:

    RAW = {
        "instId": "BTC-USDT",
        "instType": "SPOT",
        "tickSz": "0.1",
        "lotSz": "0.00000001",
        "minSz": "0.00001",
        "ctMult": "",
        "state": "live",
    }

    def test_from_okx(self):
        inst = InstrumentData.from_okx(self.RAW)
        assert inst.symbol == "BTC-USDT"
        assert inst.inst_type == InstrumentType.SPOT
        assert inst.tick_size == 0.1
        assert inst.lot_size == 0.00000001
        assert inst.min_size == 0.00001
        assert inst.contract_multiplier == 1.0  # empty ctMult → default 1
        assert inst.state == "live"


# ======================================================================
# OKXAPIError
# ======================================================================


class TestOKXAPIError:

    def test_message_format(self):
        err = OKXAPIError("50001", "Service temporarily unavailable")
        assert "50001" in str(err)
        assert "Service temporarily unavailable" in str(err)

    def test_attributes(self):
        err = OKXAPIError("50101", "APIKey error", data=[{"hint": "check key"}])
        assert err.code == "50101"
        assert err.okx_message == "APIKey error"
        assert err.data == [{"hint": "check key"}]


# ======================================================================
# RESTClient — mock httpx
# ======================================================================


def _mock_response(code: str = "0", data: Any = None, msg: str = "") -> httpx.Response:
    """Create a mock httpx.Response with OKX JSON structure."""
    body = json.dumps({"code": code, "msg": msg, "data": data or []}).encode()
    return httpx.Response(
        status_code=200,
        content=body,
        request=httpx.Request("GET", "https://www.okx.com/test"),
    )


class TestRESTClient:

    @pytest.fixture
    def client(self) -> RESTClient:
        cfg = _make_config()
        auth = OKXAuth(cfg)
        return RESTClient(cfg, auth)

    @pytest.mark.asyncio
    async def test_connect_creates_httpx_client(self, client: RESTClient):
        await client.connect()
        assert client._client is not None
        await client.close()
        assert client._client is None

    @pytest.mark.asyncio
    async def test_context_manager(self, client: RESTClient):
        async with client:
            assert client._client is not None
        assert client._client is None

    @pytest.mark.asyncio
    async def test_request_without_connect_raises(self, client: RESTClient):
        with pytest.raises(RuntimeError, match="not connected"):
            await client._request("GET", "/api/v5/market/ticker")

    @pytest.mark.asyncio
    async def test_get_ticker_parsing(self, client: RESTClient):
        """Verify get_ticker() parses OKX response into TickData."""
        tick_raw = {
            "instId": "BTC-USDT", "last": "63000", "bidPx": "62999",
            "askPx": "63001", "vol24h": "100", "high24h": "63500",
            "low24h": "62000", "sodUtc8": "62500", "ts": "1718448000000",
        }
        mock_resp = _mock_response("0", [tick_raw])

        await client.connect()
        with patch.object(client._client, "get", new_callable=AsyncMock, return_value=mock_resp):
            tick = await client.get_ticker("BTC-USDT")

        assert isinstance(tick, TickData)
        assert tick.symbol == "BTC-USDT"
        assert tick.last == 63000.0
        await client.close()

    @pytest.mark.asyncio
    async def test_get_candles_parsing(self, client: RESTClient):
        candles_raw = [
            ["1718448000000", "63000", "63100", "62900", "63050", "100", "0", "0", "1"],
            ["1718451600000", "63050", "63200", "63000", "63150", "200", "0", "0", "0"],
        ]
        mock_resp = _mock_response("0", candles_raw)

        await client.connect()
        with patch.object(client._client, "get", new_callable=AsyncMock, return_value=mock_resp):
            bars = await client.get_candles("BTC-USDT", "1H", limit=2)

        assert len(bars) == 2
        assert isinstance(bars[0], BarData)
        assert bars[0].confirmed is True
        assert bars[1].confirmed is False
        await client.close()

    @pytest.mark.asyncio
    async def test_get_history_candles_reversed(self, client: RESTClient):
        """History candles should be reversed to ascending order."""
        candles_raw = [
            ["1718451600000", "63050", "63200", "63000", "63150", "200", "0", "0", "1"],
            ["1718448000000", "63000", "63100", "62900", "63050", "100", "0", "0", "1"],
        ]
        mock_resp = _mock_response("0", candles_raw)

        await client.connect()
        with patch.object(client._client, "get", new_callable=AsyncMock, return_value=mock_resp):
            bars = await client.get_history_candles("BTC-USDT", "1H", after=999999)

        # Should be ascending (oldest first)
        assert bars[0].timestamp < bars[1].timestamp
        assert bars[0].open == 63000.0
        await client.close()

    @pytest.mark.asyncio
    async def test_api_error_raises(self, client: RESTClient):
        mock_resp = _mock_response("50001", [], msg="Service unavailable")

        await client.connect()
        with patch.object(client._client, "get", new_callable=AsyncMock, return_value=mock_resp):
            with pytest.raises(OKXAPIError) as exc_info:
                await client.get_ticker("BTC-USDT")
            assert exc_info.value.code == "50001"
        await client.close()

    @pytest.mark.asyncio
    async def test_429_retry(self, client: RESTClient):
        """Verify 429 triggers retry with backoff."""
        ok_resp = _mock_response("0", [{
            "instId": "BTC-USDT", "last": "63000", "bidPx": "62999",
            "askPx": "63001", "vol24h": "100", "high24h": "63500",
            "low24h": "62000", "sodUtc8": "62500", "ts": "1718448000000",
        }])
        rate_limit_resp = httpx.Response(
            status_code=429,
            content=b"",
            headers={"Retry-After": "0.1"},
            request=httpx.Request("GET", "https://www.okx.com/test"),
        )

        await client.connect()
        with patch.object(
            client._client, "get", new_callable=AsyncMock,
            side_effect=[
                httpx.HTTPStatusError("429", request=rate_limit_resp.request, response=rate_limit_resp),
                ok_resp,
            ],
        ):
            with patch("okx_quant.gateway.rest_client.asyncio.sleep", new_callable=AsyncMock):
                tick = await client.get_ticker("BTC-USDT")
            assert tick.symbol == "BTC-USDT"
        await client.close()

    @pytest.mark.asyncio
    async def test_place_order(self, client: RESTClient):
        order_result = [{"ordId": "12345", "sCode": "0", "sMsg": ""}]
        mock_resp = _mock_response("0", order_result)

        await client.connect()
        with patch.object(client._client, "post", new_callable=AsyncMock, return_value=mock_resp):
            result = await client.place_order("BTC-USDT", "buy", "limit", "0.001", price="63000")
        assert result["ordId"] == "12345"
        await client.close()

    @pytest.mark.asyncio
    async def test_signed_request_includes_auth_headers(self, client: RESTClient):
        """Verify private endpoints include OK-ACCESS-* headers."""
        mock_resp = _mock_response("0", [{"totalEq": "10000", "availBal": "8000", "mgnRatio": "1.5"}])

        await client.connect()
        with patch.object(client._client, "get", new_callable=AsyncMock, return_value=mock_resp) as mock_get:
            await client.get_balance()

        # Check headers were passed
        call_kwargs = mock_get.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers", {})
        assert "OK-ACCESS-KEY" in headers
        assert "OK-ACCESS-SIGN" in headers
        assert "OK-ACCESS-TIMESTAMP" in headers
        assert "OK-ACCESS-PASSPHRASE" in headers
        await client.close()


# ======================================================================
# WebSocketClient — dispatch and parsing
# ======================================================================


class TestWebSocketClient:

    def test_parse_ticker(self):
        raw = {
            "instId": "BTC-USDT", "last": "63000", "bidPx": "62999",
            "askPx": "63001", "vol24h": "100", "high24h": "63500",
            "low24h": "62000", "sodUtc8": "62500", "ts": "1718448000000",
        }
        result = WebSocketClient._parse_push("tickers", raw)
        assert isinstance(result, TickData)
        assert result.symbol == "BTC-USDT"

    def test_parse_candle(self):
        raw = ["1718448000000", "63000", "63100", "62900", "63050", "100", "0", "0", "1"]
        raw_dict = {"instId": "BTC-USDT", **{f"field{i}": v for i, v in enumerate(raw)}}
        # Candle data comes as a list in the WS push, but _parse_push gets a dict
        # OKX sends candle data as array-like dicts with instId
        raw_candle = {
            "instId": "BTC-USDT",
            0: raw[0], 1: raw[1], 2: raw[2], 3: raw[3], 4: raw[4],
            5: raw[5], 6: raw[6], 7: raw[7], 8: raw[8],
        }
        result = WebSocketClient._parse_push("candle1H", raw_candle)
        assert isinstance(result, BarData)

    def test_parse_order(self):
        raw = {
            "ordId": "12345", "clOrdId": "my_order", "instId": "BTC-USDT",
            "side": "buy", "ordType": "limit", "px": "63000", "sz": "0.001",
            "accFillSz": "0", "avgPx": "0", "state": "live",
            "fee": "0", "feeCcy": "", "cTime": "1718448000000", "uTime": "1718448000000",
        }
        result = WebSocketClient._parse_push("orders", raw)
        assert isinstance(result, OrderData)
        assert result.order_id == "12345"

    def test_parse_unknown_channel_returns_dict(self):
        raw = {"foo": "bar"}
        result = WebSocketClient._parse_push("unknown_channel", raw)
        assert result == raw

    def test_add_subscription(self):
        client = WebSocketClient("wss://test")
        cb = AsyncMock()
        client.subscribe_ticker(["BTC-USDT", "ETH-USDT"], cb)
        assert "tickers" in client._subscriptions
        assert len(client._subscriptions["tickers"]) == 2

    def test_add_multiple_channels(self):
        client = WebSocketClient("wss://test")
        client.subscribe_ticker(["BTC-USDT"], AsyncMock())
        client.subscribe_candles("BTC-USDT", "1H", AsyncMock())
        assert "tickers" in client._subscriptions
        assert "candle1H" in client._subscriptions

    @pytest.mark.asyncio
    async def test_dispatch_ticker_callback(self):
        """Verify dispatch() invokes the registered callback with parsed data."""
        client = WebSocketClient("wss://test")
        received: list[TickData] = []

        async def on_tick(tick: TickData):
            received.append(tick)

        client.subscribe_ticker(["BTC-USDT"], on_tick)
        client._connected.set()  # pretend connected

        # Simulate OKX push message
        push_msg = {
            "arg": {"channel": "tickers", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT", "last": "63000", "bidPx": "62999",
                "askPx": "63001", "vol24h": "100", "high24h": "63500",
                "low24h": "62000", "sodUtc8": "62500", "ts": "1718448000000",
            }],
        }
        client._dispatch(push_msg)

        # Let the created tasks complete
        await asyncio.sleep(0.05)
        assert len(received) == 1
        assert isinstance(received[0], TickData)
        assert received[0].last == 63000.0

    @pytest.mark.asyncio
    async def test_dispatch_filters_by_inst_id(self):
        """Callback should only fire for subscribed symbols."""
        client = WebSocketClient("wss://test")
        received: list[TickData] = []

        async def on_tick(tick: TickData):
            received.append(tick)

        client.subscribe_ticker(["BTC-USDT"], on_tick)
        client._connected.set()

        # Push for ETH-USDT — should NOT trigger
        push_eth = {
            "arg": {"channel": "tickers", "instId": "ETH-USDT"},
            "data": [{
                "instId": "ETH-USDT", "last": "3000", "bidPx": "2999",
                "askPx": "3001", "vol24h": "500", "high24h": "3100",
                "low24h": "2900", "sodUtc8": "2950", "ts": "1718448000000",
            }],
        }
        client._dispatch(push_eth)
        await asyncio.sleep(0.05)
        assert len(received) == 0  # filtered out

    @pytest.mark.asyncio
    async def test_dispatch_pong_resets_timer(self):
        client = WebSocketClient("wss://test")
        old = client._last_pong
        client._dispatch("pong")
        assert client._last_pong >= old

    @pytest.mark.asyncio
    async def test_dispatch_event_message_ignored(self):
        """Subscribe/unsubscribe events should not crash."""
        client = WebSocketClient("wss://test")
        client._dispatch({"event": "subscribe", "arg": {"channel": "tickers"}})
        client._dispatch({"event": "error", "msg": "bad arg"})
        # Should not raise

    @pytest.mark.asyncio
    async def test_disconnect_cancels_tasks(self):
        client = WebSocketClient("wss://test")
        # Create mock tasks that can be cancelled
        async def dummy():
            while True:
                await asyncio.sleep(1)

        client._recv_task = asyncio.create_task(dummy())
        client._ping_task = asyncio.create_task(dummy())
        client._running = True

        await client.disconnect()
        assert client._running is False
        assert client._recv_task.cancelled()
        assert client._ping_task.cancelled()
