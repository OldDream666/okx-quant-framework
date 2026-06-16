"""统一数据模型：行情、订单、持仓、账户。

这些 dataclass 作为 OKX API 响应的内部表示，
将框架的其余部分与原始 JSON 格式解耦。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    POST_ONLY = "post_only"
    FOK = "fok"
    IOC = "ioc"


class OrderStatus(str, Enum):
    LIVE = "live"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelling"  # OKX uses "cancelling" for cancel-pending
    CANCELLED_DONE = "canceled"  # OKX terminal cancelled state
    FAILED = "failed"


class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"
    NET = "net"


class InstrumentType(str, Enum):
    SPOT = "SPOT"
    SWAP = "SWAP"
    FUTURES = "FUTURES"
    MARGIN = "MARGIN"
    OPTION = "OPTION"


# ---------------------------------------------------------------------------
# Market Data
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class TickData:
    """实时行情快照。"""

    symbol: str            # e.g. "BTC-USDT"
    last: float            # last traded price
    bid: float             # best bid price
    ask: float             # best ask price
    volume24h: float       # 24h base currency volume
    high24h: float         # 24h high
    low24h: float          # 24h low
    change24h: float       # 24h price change percentage (e.g. 0.02 = 2%)
    timestamp: int         # Unix ms

    @classmethod
    def from_okx(cls, data: dict[str, Any]) -> TickData:
        """从 OKX ``/api/v5/market/ticker`` 响应项解析。"""
        return cls(
            symbol=data["instId"],
            last=float(data["last"]),
            bid=float(data.get("bidPx", 0)),
            ask=float(data.get("askPx", 0)),
            volume24h=float(data.get("vol24h", 0)),
            high24h=float(data.get("high24h", 0)),
            low24h=float(data.get("low24h", 0)),
            change24h=float(data.get("sodUtc8", 0) and
                            (float(data["last"]) - float(data["sodUtc8"]))
                            / float(data["sodUtc8"]) if float(data.get("sodUtc8", 0)) else 0),
            timestamp=int(data.get("ts", 0)),
        )


@dataclass(slots=True, frozen=True)
class BarData:
    """OHLCV K 线数据。"""

    symbol: str            # e.g. "BTC-USDT"
    open: float
    high: float
    low: float
    close: float
    volume: float          # base currency volume
    timestamp: int         # Unix ms (bar open time)
    confirmed: bool        # True if bar is closed/final

    @classmethod
    def from_okx(cls, data: list[Any], symbol: str) -> BarData:
        """从 OKX K 线数组解析。

        OKX 格式: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
        """
        return cls(
            symbol=symbol,
            open=float(data[1]),
            high=float(data[2]),
            low=float(data[3]),
            close=float(data[4]),
            volume=float(data[5]),
            timestamp=int(data[0]),
            confirmed=data[8] == "1",
        )


# ---------------------------------------------------------------------------
# Order Data
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class OrderData:
    """订单状态表示。"""

    order_id: str          # OKX ordId
    client_order_id: str   # OKX clOrdId (client-assigned)
    symbol: str            # instId
    side: OrderSide
    order_type: OrderType
    price: float           # limit price (0 for market)
    quantity: float        # original order size
    filled_qty: float      # cumulative filled quantity
    avg_price: float       # average fill price
    status: OrderStatus
    fee: float             # accumulated fee (negative = rebate)
    fee_currency: str
    timestamp: int         # Unix ms (creation time)
    update_time: int       # Unix ms (last update)

    @classmethod
    def from_okx(cls, data: dict[str, Any]) -> OrderData:
        """从 OKX 订单数据解析（``/api/v5/trade/orders`` 或 WebSocket 推送）。"""
        return cls(
            order_id=data.get("ordId", ""),
            client_order_id=data.get("clOrdId", ""),
            symbol=data["instId"],
            side=OrderSide(data["side"]),
            order_type=OrderType(data.get("ordType", "limit")),
            price=float(data.get("px", 0)),
            quantity=float(data.get("sz", 0)),
            filled_qty=float(data.get("accFillSz", 0)),
            avg_price=float(data.get("avgPx", 0)),
            status=OrderStatus(data.get("state", "live")),
            fee=float(data.get("fee", 0)),
            fee_currency=data.get("feeCcy", ""),
            timestamp=int(data.get("cTime", 0)),
            update_time=int(data.get("uTime", 0)),
        )


# ---------------------------------------------------------------------------
# Position Data
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class PositionData:
    """账户持仓表示。"""

    symbol: str
    side: PositionSide
    quantity: float        # signed: +long, -short
    avg_price: float
    unrealized_pnl: float
    leverage: float
    margin_mode: str       # "cross" or "isolated"
    liquidation_price: float
    margin: float          # margin occupied
    timestamp: int         # Unix ms

    @classmethod
    def from_okx(cls, data: dict[str, Any]) -> PositionData:
        """从 OKX ``/api/v5/account/positions`` 响应项解析。"""
        pos = float(data.get("pos", 0))
        side_str = data.get("posSide", "net")
        if side_str == "net":
            side = PositionSide.LONG if pos >= 0 else PositionSide.SHORT
        else:
            side = PositionSide(side_str)
        return cls(
            symbol=data["instId"],
            side=side,
            quantity=pos,
            avg_price=float(data.get("avgPx", 0)),
            unrealized_pnl=float(data.get("upl", 0)),
            leverage=float(data.get("lever", 0)),
            margin_mode=data.get("mgnMode", "cross"),
            liquidation_price=float(data.get("liqPx", 0)),
            margin=float(data.get("margin", 0)),
            timestamp=int(data.get("cTime", 0)),
        )


# ---------------------------------------------------------------------------
# Account Data
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AccountData:
    """账户余额和持仓快照。"""

    total_equity: float          # total equity in USD
    available_balance: float     # available balance
    margin_ratio: float          # maintenance margin ratio
    positions: list[PositionData] = field(default_factory=list)

    @classmethod
    def from_okx_balance(cls, data: dict[str, Any]) -> AccountData:
        """从 OKX ``/api/v5/account/balance`` 响应解析。

        取第一个 ``details`` 条目作为聚合的账户级别字段。
        """
        details = data.get("details", [])
        # Account-level totals
        return cls(
            total_equity=float(data.get("totalEq", 0)),
            available_balance=float(data.get("availBal", 0) or 0),
            margin_ratio=float(data.get("mgnRatio", 0) or 0),
            positions=[],
        )


# ---------------------------------------------------------------------------
# Instrument Data
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class InstrumentData:
    """交易品种规格。"""

    symbol: str                  # instId, e.g. "BTC-USDT"
    inst_type: InstrumentType
    tick_size: float             # minimum price increment
    lot_size: float              # minimum quantity step
    min_size: float              # minimum order quantity
    contract_multiplier: float   # 1.0 for spot
    state: str                   # "live", "suspend", etc.

    @classmethod
    def from_okx(cls, data: dict[str, Any]) -> InstrumentData:
        """从 OKX ``/api/v5/public/instruments`` 响应项解析。"""
        return cls(
            symbol=data["instId"],
            inst_type=InstrumentType(data.get("instType", "SPOT")),
            tick_size=float(data.get("tickSz", 0.01)),
            lot_size=float(data.get("lotSz", 0.00000001)),
            min_size=float(data.get("minSz", 0)),
            contract_multiplier=float(data.get("ctMult", 1) or 1),
            state=data.get("state", "live"),
        )


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class OKXAPIError(Exception):
    """当 OKX 返回非零 ``code`` 时抛出。"""

    def __init__(self, code: str, message: str, data: Any = None) -> None:
        self.code = code
        self.okx_message = message
        self.data = data
        super().__init__(f"OKX API Error [{code}]: {message}")


class OKXConnectionError(Exception):
    """WebSocket 连接在重试耗尽后失败时抛出。"""
