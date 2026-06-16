"""基于 loguru 的日志配置。

配置结构化、轮转、压缩日志，并拦截标准 ``logging`` 模块，
使框架中所有现有的 ``logging.getLogger()`` 调用自动通过 loguru 处理。

用法::

    from okx_quant.monitoring.logger import setup_logger
    setup_logger(log_dir="logs", level="INFO")
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

from loguru import logger as _loguru_logger


# ---------------------------------------------------------------------------
# InterceptHandler — bridges stdlib logging → loguru
# ---------------------------------------------------------------------------


class InterceptHandler(logging.Handler):
    """将所有 ``logging`` 日志记录重定向到 loguru。

    确保使用 ``logging.getLogger(__name__)`` 的模块（REST client、
    WebSocket client、OMS 等）的输出被 loguru 的 formatter 和 sink 捕获。
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = _loguru_logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find the originating frame (skip stdlib internals)
        frame, depth = logging.currentframe(), 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back  # type: ignore[assignment]
            depth += 1

        _loguru_logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


# ---------------------------------------------------------------------------
# setup_logger
# ---------------------------------------------------------------------------


def setup_logger(
    log_dir: str | Path = "logs",
    level: str = "INFO",
    rotation: str = "00:00",
    retention: str = "30 days",
    compression: str = "gz",
    console: bool = True,
    format_console: str | None = None,
    format_file: str | None = None,
) -> None:
    """配置 loguru 的文件和控制台 sink 以及 stdlib 拦截。

    Parameters:
        log_dir:     日志文件目录（自动创建）。
        level:       最低日志级别（DEBUG / INFO / WARNING / ERROR / CRITICAL）。
        rotation:    日志轮转时机（``"00:00"`` = 每日午夜，
                     ``"100 MB"`` = 按大小等）。
        retention:   旧日志保留时长（``"30 days"``、``"10 files"``）。
        compression: 轮转文件压缩格式（``"gz"``、``"zip"``、``"tar.gz"``）。
        console:     启用带颜色的控制台（stderr）输出。
        format_console: 覆盖控制台格式字符串。
        format_file: 覆盖文件格式字符串。
    """
    # Remove loguru's default stderr sink
    _loguru_logger.remove()

    # Formats
    _fmt_console = format_console or (
        "<green>{time:HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    )
    _fmt_file = format_file or (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
        "{level: <8} | "
        "{name}:{function}:{line} - "
        "{message}"
    )

    # Console sink
    if console:
        _loguru_logger.add(
            sys.stderr,
            format=_fmt_console,
            level=level,
            colorize=True,
        )

    # File sink — daily rotation + compression
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    log_file = log_path / "trading_{time:YYYY-MM-DD}.log"

    _loguru_logger.add(
        str(log_file),
        format=_fmt_file,
        level=level,
        rotation=rotation,
        retention=retention,
        compression=compression,
        encoding="utf-8",
    )

    # Intercept stdlib logging
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # Silence noisy libraries
    for name in ("httpx", "httpcore", "websockets"):
        logging.getLogger(name).setLevel(logging.WARNING)

    _loguru_logger.info(
        "Logger initialized — level={}, dir={}, rotation={}, retention={}",
        level, log_path.resolve(), rotation, retention,
    )


# ---------------------------------------------------------------------------
# Convenience: get a contextualized logger
# ---------------------------------------------------------------------------


def get_logger(**context: Any) -> Any:
    """返回绑定 *context* 字段的 loguru logger。

    用法::

        log = get_logger(strategy="ma_crossover", symbol="BTC-USDT")
        log.info("Signal generated")  # 输出中包含上下文信息

    Returns:
        绑定了上下文的 loguru ``Logger``。
    """
    return _loguru_logger.bind(**context)
