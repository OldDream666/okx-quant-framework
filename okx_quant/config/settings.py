"""基于 Pydantic 的配置管理。

从环境变量（.env 文件）加载配置，并使用 Pydantic v2 模型验证所有字段。
根据 ``is_demo`` 标志自动切换 demo 和 production 的 OKX 端点。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# OKX connection configuration
# ---------------------------------------------------------------------------

class OKXConfig(BaseModel):
    """OKX V5 API 连接参数。

    属性:
        api_key:    OKX API key。
        secret_key: OKX API secret。
        passphrase: OKX API passphrase。
        is_demo:    ``True`` → demo/sandbox (flag='1')，``False`` → production (flag='0')。
        base_url:   REST API 基础 URL。
        ws_public:  公共 WebSocket URL（tickers、candles、orderbook）。
        ws_private: 私有 WebSocket URL（account、orders、positions）。
    """

    api_key: str = Field(..., min_length=1, description="OKX API key")
    secret_key: str = Field(..., min_length=1, description="OKX API secret")
    passphrase: str = Field(..., min_length=1, description="OKX API passphrase")

    is_demo: bool = Field(default=True, description="True=demo, False=production")

    base_url: str = Field(default="https://www.okx.com")
    ws_public: str = Field(default="wss://ws.okx.com:8443/ws/v5/public")
    ws_private: str = Field(default="wss://ws.okx.com:8443/ws/v5/private")

    # Convenience properties -------------------------------------------------

    @property
    def flag(self) -> str:
        """OKX SDK 标志：'0' = production，'1' = demo。"""
        return "1" if self.is_demo else "0"

    # Validators -------------------------------------------------------------

    @model_validator(mode="after")
    def _set_demo_urls(self) -> "OKXConfig":
        """当 ``is_demo=True`` 时，将 WebSocket URL 切换为 demo 端点。"""
        if self.is_demo:
            self.ws_public = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
            self.ws_private = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
        return self


# ---------------------------------------------------------------------------
# Application-level configuration
# ---------------------------------------------------------------------------

class AppConfig(BaseModel):
    """顶层应用配置。

    属性:
        okx:        OKX 连接设置。
        log_level:  日志级别（DEBUG / INFO / WARNING / ERROR）。
        max_workers: 最大并发工作任务数（由 gateway 层使用）。
    """

    okx: OKXConfig
    log_level: str = Field(default="INFO", pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    max_workers: int = Field(default=4, ge=1, le=64)


# ---------------------------------------------------------------------------
# Loader helpers
# ---------------------------------------------------------------------------

_ENV_KEY_MAP: dict[str, str] = {
    "OKX_API_KEY": "api_key",
    "OKX_SECRET_KEY": "secret_key",
    "OKX_PASSPHRASE": "passphrase",
    "OKX_IS_DEMO": "is_demo",
    "OKX_BASE_URL": "base_url",
    "OKX_WS_PUBLIC": "ws_public",
    "OKX_WS_PRIVATE": "ws_private",
    "LOG_LEVEL": "log_level",
    "MAX_WORKERS": "max_workers",
}

_BOOL_TRUE = {"1", "true", "yes", "on"}


def _parse_bool(val: str) -> bool:
    return val.strip().lower() in _BOOL_TRUE


def load_config(env_file: str | Path | None = None) -> AppConfig:
    """从环境变量构建 :class:`AppConfig`。

    参数:
        env_file: ``.env`` 文件路径。为 *None* 时，``python-dotenv``
                  会在当前目录及父目录中搜索 ``.env``。

    返回:
        经过完整验证的 :class:`AppConfig` 实例。

    异常:
        pydantic.ValidationError: 必需变量缺失或无效时抛出。
    """
    if env_file is not None:
        load_dotenv(env_file, override=True)
    else:
        load_dotenv(override=True)

    # Collect OKX sub-config --------------------------------------------------
    okx_kwargs: dict[str, Any] = {}
    for env_key, field_name in _ENV_KEY_MAP.items():
        value = os.getenv(env_key)
        if value is None:
            continue
        if field_name == "is_demo":
            okx_kwargs[field_name] = _parse_bool(value)
        elif field_name in ("log_level", "max_workers"):
            continue  # handled below
        else:
            okx_kwargs[field_name] = value

    okx = OKXConfig(**okx_kwargs)

    # App-level fields --------------------------------------------------------
    app_kwargs: dict[str, Any] = {"okx": okx}
    log_level = os.getenv("LOG_LEVEL")
    if log_level is not None:
        app_kwargs["log_level"] = log_level.upper()
    max_workers = os.getenv("MAX_WORKERS")
    if max_workers is not None:
        app_kwargs["max_workers"] = int(max_workers)

    return AppConfig(**app_kwargs)


def env_example(path: str | Path = ".env.example") -> Path:
    """生成 ``.env.example`` 模板文件。

    参数:
        path: 目标路径。

    返回:
        写入的已解析 :class:`Path` 对象。
    """
    content = """\
# OKX API credentials (required)
OKX_API_KEY=your_api_key_here
OKX_SECRET_KEY=your_secret_key_here
OKX_PASSPHRASE=your_passphrase_here

# Trading environment: true = demo/sandbox, false = production
OKX_IS_DEMO=true

# Override default endpoints (optional)
# OKX_BASE_URL=https://www.okx.com
# OKX_WS_PUBLIC=wss://ws.okx.com:8443/ws/v5/public
# OKX_WS_PRIVATE=wss://ws.okx.com:8443/ws/v5/private

# Application settings (optional)
LOG_LEVEL=INFO
MAX_WORKERS=4
"""
    p = Path(path).resolve()
    p.write_text(content, encoding="utf-8")
    return p
