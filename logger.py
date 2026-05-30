from __future__ import annotations

import logging
import sys
from pathlib import Path

_LOG_DIR = Path(__file__).parent / "logs"


def setup_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    _LOG_DIR.mkdir(exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(level)

    if not root.handlers:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        root.addHandler(sh)

        fh = logging.FileHandler(_LOG_DIR / "binance.log")
        fh.setFormatter(fmt)
        root.addHandler(fh)

    return logging.getLogger(name)
