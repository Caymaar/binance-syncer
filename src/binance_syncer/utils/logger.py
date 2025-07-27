import logging.config
from rich.console import Console
from datetime import datetime
import functools
from pathlib import Path

console = Console()

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "file": {
            "format": "%(asctime)s [%(levelname)s] %(name)s:%(funcName)s:%(lineno)d – %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "rich": {
            "format": "[%(asctime)s] %(name)s:%(funcName)s:%(lineno)d – %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }
    },
    "handlers": {
        "rich": {
            "()": "rich.logging.RichHandler",
            "console": console,
            "show_time": True,
            "show_level": True,
            "show_path": False,
            "level": "INFO",
            "formatter": "rich",  # Utiliser le formateur rich
        },
        "file": {
            "class": "logging.FileHandler",
            "formatter": "file",
            "encoding": "utf-8",
            "level": "INFO",
        },
    },
    "root": {
        "level": "DEBUG",
        "handlers": ["rich", "file"],
    },
}

def configure_logger():
    log_dir = Path.home() / "logs" / "binance_syncer"
    log_dir.mkdir(parents=True, exist_ok=True)
    logs = sorted(log_dir.glob("log_*.log"), key=lambda p: p.stat().st_mtime, reverse=True)

    LOGGING_CONFIG["handlers"]["file"]["filename"] = log_dir / f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    for old_log in logs[5:]:
        try:
            old_log.unlink()
        except OSError:
            pass
    logging.config.dictConfig(LOGGING_CONFIG)


def with_spinner(text: str, spinner: str = "simpleDotsScrolling"):    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Merge args→kwargs via les noms de signature
            import inspect
            sig = inspect.signature(func)
            bound = sig.bind_partial(*args, **kwargs)
            bound.apply_defaults()

            try:
                message = text.format(**bound.arguments)
            except Exception:
                message = text

            with console.status(f"[bold green]{message}", spinner=spinner):
                return func(*args, **kwargs)
        return wrapper
    return decorator