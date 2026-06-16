"""实盘交易引擎。"""

__all__ = ["LiveRunner"]


def __getattr__(name: str):
    """Lazy import to avoid circular dependencies."""
    if name == "LiveRunner":
        from okx_quant.live.runner import LiveRunner
        return LiveRunner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
