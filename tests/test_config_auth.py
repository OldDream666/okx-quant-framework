"""Unit tests for Module 1: Config & Auth.

Covers:
  - OKXConfig: field validation, demo flag URL switching, flag property
  - AppConfig: nested config, validation constraints
  - OKXAuth: signature format, headers completeness, deterministic output
  - load_config: .env file loading
"""

from __future__ import annotations

import base64
import datetime
import hashlib
import hmac
import os
import re
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from okx_quant.config.auth import OKXAuth
from okx_quant.config.settings import AppConfig, OKXConfig, load_config


# ======================================================================
# OKXConfig
# ======================================================================


class TestOKXConfig:
    """Tests for OKXConfig pydantic model."""

    _VALID = dict(api_key="ak", secret_key="sk", passphrase="pp")

    # --- basic construction ---

    def test_minimal_config(self):
        cfg = OKXConfig(**self._VALID)
        assert cfg.api_key == "ak"
        assert cfg.secret_key == "sk"
        assert cfg.passphrase == "pp"
        assert cfg.is_demo is True  # default

    def test_production_config(self):
        cfg = OKXConfig(**self._VALID, is_demo=False)
        assert cfg.is_demo is False
        assert cfg.flag == "0"

    def test_demo_flag_value(self):
        cfg = OKXConfig(**self._VALID, is_demo=True)
        assert cfg.flag == "1"

    # --- demo URL auto-switching ---

    def test_demo_urls(self):
        cfg = OKXConfig(**self._VALID, is_demo=True)
        assert "brokerId=9999" in cfg.ws_public
        assert "brokerId=9999" in cfg.ws_private
        assert "wspap.okx.com" in cfg.ws_public

    def test_production_urls(self):
        cfg = OKXConfig(**self._VALID, is_demo=False)
        assert "brokerId" not in cfg.ws_public
        assert cfg.ws_public == "wss://ws.okx.com:8443/ws/v5/public"
        assert cfg.ws_private == "wss://ws.okx.com:8443/ws/v5/private"

    # --- custom endpoints ---

    def test_custom_base_url(self):
        cfg = OKXConfig(**self._VALID, base_url="https://custom.okx.com")
        assert cfg.base_url == "https://custom.okx.com"

    # --- validation errors ---

    def test_missing_api_key(self):
        with pytest.raises(ValidationError) as exc_info:
            OKXConfig(secret_key="sk", passphrase="pp")
        assert "api_key" in str(exc_info.value).lower() or "api_key" in str(exc_info.value)

    def test_empty_secret_key(self):
        with pytest.raises(ValidationError):
            OKXConfig(api_key="ak", secret_key="", passphrase="pp")

    def test_missing_passphrase(self):
        with pytest.raises(ValidationError):
            OKXConfig(api_key="ak", secret_key="sk")


# ======================================================================
# AppConfig
# ======================================================================


class TestAppConfig:
    """Tests for the top-level AppConfig model."""

    _OKX = OKXConfig(api_key="ak", secret_key="sk", passphrase="pp")

    def test_defaults(self):
        cfg = AppConfig(okx=self._OKX)
        assert cfg.log_level == "INFO"
        assert cfg.max_workers == 4

    def test_custom_log_level(self):
        cfg = AppConfig(okx=self._OKX, log_level="DEBUG")
        assert cfg.log_level == "DEBUG"

    def test_invalid_log_level(self):
        with pytest.raises(ValidationError):
            AppConfig(okx=self._OKX, log_level="VERBOSE")

    def test_max_workers_too_low(self):
        with pytest.raises(ValidationError):
            AppConfig(okx=self._OKX, max_workers=0)

    def test_max_workers_too_high(self):
        with pytest.raises(ValidationError):
            AppConfig(okx=self._OKX, max_workers=100)

    def test_nested_okx_access(self):
        cfg = AppConfig(okx=self._OKX)
        assert cfg.okx.flag == "1"


# ======================================================================
# load_config
# ======================================================================


class TestLoadConfig:
    """Tests for load_config() .env file loader."""

    def test_load_from_dotenv(self, tmp_path: Path):
        env = tmp_path / ".env"
        env.write_text(textwrap.dedent("""\
            OKX_API_KEY=test_key
            OKX_SECRET_KEY=test_secret
            OKX_PASSPHRASE=test_pp
            OKX_IS_DEMO=false
            LOG_LEVEL=WARNING
            MAX_WORKERS=8
        """))
        cfg = load_config(env)
        assert cfg.okx.api_key == "test_key"
        assert cfg.okx.secret_key == "test_secret"
        assert cfg.okx.passphrase == "test_pp"
        assert cfg.okx.is_demo is False
        assert cfg.okx.flag == "0"
        assert cfg.log_level == "WARNING"
        assert cfg.max_workers == 8

    def test_demo_true_values(self, tmp_path: Path):
        """Verify various truthy strings for IS_DEMO."""
        for val in ["true", "True", "1", "yes", "YES", "on", "ON"]:
            env = tmp_path / ".env"
            env.write_text(
                f"OKX_API_KEY=k\nOKX_SECRET_KEY=s\nOKX_PASSPHRASE=p\nOKX_IS_DEMO={val}\n"
            )
            cfg = load_config(env)
            assert cfg.okx.is_demo is True, f"'{val}' should be True"

    def test_demo_false_values(self, tmp_path: Path):
        """Verify various falsy strings for IS_DEMO."""
        for val in ["false", "False", "0", "no", "NO", "off", "OFF", ""]:
            env = tmp_path / ".env"
            env.write_text(
                f"OKX_API_KEY=k\nOKX_SECRET_KEY=s\nOKX_PASSPHRASE=p\nOKX_IS_DEMO={val}\n"
            )
            cfg = load_config(env)
            assert cfg.okx.is_demo is False, f"'{val}' should be False"

    def test_missing_required_raises(self, tmp_path: Path):
        """Missing OKX_API_KEY should cause a ValidationError."""
        env = tmp_path / ".env"
        env.write_text("OKX_SECRET_KEY=s\nOKX_PASSPHRASE=p\n")
        # Clear any env vars that might leak from the host
        with patch.dict(os.environ, {}, clear=False):
            # Remove keys if present in real env
            for k in ["OKX_API_KEY", "OKX_SECRET_KEY", "OKX_PASSPHRASE"]:
                os.environ.pop(k, None)
            with pytest.raises(ValidationError):
                load_config(env)

    def test_custom_endpoints(self, tmp_path: Path):
        """Custom WS URLs are overridden by demo validator when is_demo=True."""
        env = tmp_path / ".env"
        env.write_text(textwrap.dedent("""\
            OKX_API_KEY=k
            OKX_SECRET_KEY=s
            OKX_PASSPHRASE=p
            OKX_IS_DEMO=true
            OKX_WS_PUBLIC=wss://custom.public
            OKX_WS_PRIVATE=wss://custom.private
        """))
        # Isolate from leaked env vars by prior load_dotenv calls
        with patch.dict(os.environ, {}, clear=False):
            for k in ["OKX_API_KEY", "OKX_SECRET_KEY", "OKX_PASSPHRASE",
                       "OKX_IS_DEMO", "OKX_WS_PUBLIC", "OKX_WS_PRIVATE",
                       "LOG_LEVEL", "MAX_WORKERS"]:
                os.environ.pop(k, None)
            cfg = load_config(env)
        # is_demo=True → demo validator overrides custom URLs
        assert cfg.okx.is_demo is True
        assert "brokerId=9999" in cfg.okx.ws_public

    def test_custom_endpoints_not_overridden_when_production(self, tmp_path: Path):
        env = tmp_path / ".env"
        env.write_text(textwrap.dedent("""\
            OKX_API_KEY=k
            OKX_SECRET_KEY=s
            OKX_PASSPHRASE=p
            OKX_IS_DEMO=false
            OKX_WS_PUBLIC=wss://my.public
            OKX_WS_PRIVATE=wss://my.private
        """))
        cfg = load_config(env)
        # Production mode: validator doesn't override
        # But the model_validator sets demo URLs only when is_demo=True.
        # With is_demo=False, the fields keep their values from env.
        assert cfg.okx.ws_public == "wss://my.public"
        assert cfg.okx.ws_private == "wss://my.private"


# ======================================================================
# OKXAuth
# ======================================================================


class TestOKXAuth:
    """Tests for OKX V5 API signature generation."""

    def _make_auth(self) -> OKXAuth:
        cfg = OKXConfig(api_key="test_key", secret_key="test_secret", passphrase="test_pp")
        return OKXAuth(cfg)

    # --- headers completeness ---

    def test_sign_returns_four_keys(self):
        auth = self._make_auth()
        headers = auth.sign("GET", "/api/v5/account/balance")
        assert set(headers.keys()) == {
            "OK-ACCESS-KEY",
            "OK-ACCESS-SIGN",
            "OK-ACCESS-TIMESTAMP",
            "OK-ACCESS-PASSPHRASE",
        }

    def test_sign_key_value(self):
        auth = self._make_auth()
        headers = auth.sign("GET", "/api/v5/account/balance")
        assert headers["OK-ACCESS-KEY"] == "test_key"
        assert headers["OK-ACCESS-PASSPHRASE"] == "test_pp"

    # --- timestamp format ---

    def test_timestamp_is_iso_utc_with_milliseconds(self):
        auth = self._make_auth()
        headers = auth.sign("GET", "/api/v5/account/balance")
        ts = headers["OK-ACCESS-TIMESTAMP"]
        # Should match ISO 8601 with milliseconds and Z suffix
        # e.g. "2026-06-15T12:00:00.123Z"
        assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", ts), (
            f"Timestamp '{ts}' does not match ISO 8601 with ms (e.g. 2026-06-15T12:00:00.123Z)"
        )

    # --- signature correctness ---

    def test_signature_is_base64(self):
        auth = self._make_auth()
        headers = auth.sign("GET", "/api/v5/account/balance")
        sig = headers["OK-ACCESS-SIGN"]
        # Should be valid base64
        decoded = base64.b64decode(sig)
        assert len(decoded) == 32  # SHA256 digest = 32 bytes

    def test_signature_deterministic_with_same_timestamp(self):
        """Same timestamp + method + path + body → same signature."""
        auth = self._make_auth()
        fixed_ts = "2026-01-01T00:00:00"
        with patch.object(OKXAuth, "_make_timestamp", return_value=fixed_ts):
            h1 = auth.sign("POST", "/api/v5/trade/order", '{"instId":"BTC-USDT"}')
            h2 = auth.sign("POST", "/api/v5/trade/order", '{"instId":"BTC-USDT"}')
        assert h1["OK-ACCESS-SIGN"] == h2["OK-ACCESS-SIGN"]

    def test_signature_differs_with_different_body(self):
        auth = self._make_auth()
        fixed_ts = "2026-01-01T00:00:00"
        with patch.object(OKXAuth, "_make_timestamp", return_value=fixed_ts):
            h1 = auth.sign("POST", "/api/v5/trade/order", '{"instId":"BTC-USDT"}')
            h2 = auth.sign("POST", "/api/v5/trade/order", '{"instId":"ETH-USDT"}')
        assert h1["OK-ACCESS-SIGN"] != h2["OK-ACCESS-SIGN"]

    def test_signature_differs_with_different_method(self):
        auth = self._make_auth()
        fixed_ts = "2026-01-01T00:00:00"
        with patch.object(OKXAuth, "_make_timestamp", return_value=fixed_ts):
            h1 = auth.sign("GET", "/api/v5/account/balance")
            h2 = auth.sign("POST", "/api/v5/account/balance")
        assert h1["OK-ACCESS-SIGN"] != h2["OK-ACCESS-SIGN"]

    def test_signature_matches_manual_computation(self):
        """Verify against a hand-computed HMAC-SHA256."""
        auth = self._make_auth()
        fixed_ts = "2026-06-15T12:00:00"
        method = "GET"
        path = "/api/v5/account/balance"
        body = ""

        with patch.object(OKXAuth, "_make_timestamp", return_value=fixed_ts):
            headers = auth.sign(method, path, body)

        # Manual computation
        prehash = f"{fixed_ts}{method}{path}{body}"
        mac = hmac.new(
            b"test_secret", prehash.encode("utf-8"), hashlib.sha256
        )
        expected_sig = base64.b64encode(mac.digest()).decode("utf-8")
        assert headers["OK-ACCESS-SIGN"] == expected_sig

    # --- method uppercasing ---

    def test_method_is_uppercased(self):
        """Prehash should use uppercased method regardless of input case."""
        auth = self._make_auth()
        fixed_ts = "2026-06-15T12:00:00"
        with patch.object(OKXAuth, "_make_timestamp", return_value=fixed_ts):
            h_lower = auth.sign("get", "/api/v5/account/balance")
            h_upper = auth.sign("GET", "/api/v5/account/balance")
        assert h_lower["OK-ACCESS-SIGN"] == h_upper["OK-ACCESS-SIGN"]

    # --- different configs produce different signatures ---

    def test_different_secrets_produce_different_signatures(self):
        cfg1 = OKXConfig(api_key="k1", secret_key="s1", passphrase="p1")
        cfg2 = OKXConfig(api_key="k2", secret_key="s2", passphrase="p2")
        auth1 = OKXAuth(cfg1)
        auth2 = OKXAuth(cfg2)
        fixed_ts = "2026-06-15T12:00:00"
        with patch.object(OKXAuth, "_make_timestamp", return_value=fixed_ts):
            h1 = auth1.sign("GET", "/api/v5/account/balance")
            h2 = auth2.sign("GET", "/api/v5/account/balance")
        assert h1["OK-ACCESS-SIGN"] != h2["OK-ACCESS-SIGN"]
