"""OKX V5 API 请求签名（HMAC-SHA256 → Base64）。

本模块是 **无状态** 的——每次调用 :meth:`OKXAuth.sign` 都会生成新的时间戳，
因此可以安全地从多个 async 任务并发使用。

签名算法（来自 `OKX V5 文档 <https://www.okx.com/docs-v5/en/#rest-api-authentication-sign>`_）：

    1. timestamp = ``datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')`` (ISO 8601, 无毫秒, UTC)
    2. prehash   = ``timestamp + method.upper() + requestPath + body``
    3. signature  = ``base64(hmac_sha256(secret_key, prehash))``

生成的 headers 字典包含 OKX 要求的四个键：

    - ``OK-ACCESS-KEY``
    - ``OK-ACCESS-SIGN``
    - ``OK-ACCESS-TIMESTAMP``
    - ``OK-ACCESS-PASSPHRASE``

对于 **私有** WebSocket 登录，可以使用相同的 ``sign()`` 方法——
只需传入 ``method="GET"`` 和 ``path="/users/self/verify"`` 并使用空 body。
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
from typing import Final

from okx_quant.config.settings import OKXConfig

# Prehash string format: timestamp + method + path + body
_PREHASH_FMT: Final[str] = "{ts}{method}{path}{body}"


class OKXAuth:
    """无状态的 OKX V5 API 请求签名器。

    参数:
        config: 提供凭据的 :class:`OKXConfig` 实例。
    """

    __slots__ = ("_key", "_secret", "_passphrase")

    def __init__(self, config: OKXConfig) -> None:
        self._key: str = config.api_key
        self._secret: str = config.secret_key
        self._passphrase: str = config.passphrase

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def sign(self, method: str, path: str, body: str = "") -> dict[str, str]:
        """生成 OKX V5 要求的四个认证 header。

        参数:
            method: HTTP 方法（``GET``、``POST``、``PUT``、``DELETE``）。
            path:   请求路径 **包含 query string**，
                    例如 ``/api/v5/account/balance?ccy=BTC``。
            body:   JSON 编码的请求体（GET 请求为空字符串）。

        返回:
            包含 ``OK-ACCESS-KEY``、``OK-ACCESS-SIGN``、
            ``OK-ACCESS-TIMESTAMP``、``OK-ACCESS-PASSPHRASE`` 键的字典。
        """
        timestamp = self._make_timestamp()
        signature = self._compute_signature(timestamp, method, path, body)
        return {
            "OK-ACCESS-KEY": self._key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self._passphrase,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _make_timestamp() -> str:
        """UTC ISO-8601 时间戳，含毫秒 + Z 后缀（OKX V5 格式）。
        示例输出: ``2024-01-01T12:00:00.123Z``
        """
        return datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )[:-3] + "Z"

    def _compute_signature(
        self, timestamp: str, method: str, path: str, body: str
    ) -> str:
        """HMAC-SHA256 → Base64 签名。"""
        prehash = _PREHASH_FMT.format(
            ts=timestamp,
            method=method.upper(),
            path=path,
            body=body,
        )
        mac = hmac.new(
            self._secret.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        )
        return base64.b64encode(mac.digest()).decode("utf-8")
