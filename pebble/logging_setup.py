from __future__ import annotations
import logging, json, sys, time


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(record.created)),
            "lvl": record.levelname,
            "msg": record.getMessage(),
            "logger": record.name,
        }
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            payload.update(record.extra)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    root = logging.getLogger("pebble")
    root.handlers.clear()
    root.setLevel(level)
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JsonFormatter())
    root.addHandler(h)
    return root
