import os
import json
import uuid
import logging
import threading
from datetime import datetime, timezone

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
_context = threading.local()

def set_log_context(correlation_id=None, event_source=None):
    _context.correlation_id = correlation_id or str(uuid.uuid4())
    _context.event_source = event_source or "UNKNOWN"

def get_log_context():
    return {
        "correlationId": getattr(_context, "correlation_id", str(uuid.uuid4())),
        "eventSource": getattr(_context, "event_source", "UNKNOWN"),
    }

class JSONFormatter(logging.Formatter):
    def format(self, record):
        context = get_log_context()
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "correlationId": context["correlationId"],
            "eventSource": context["eventSource"]
        }

        if record.exc_info:
            log_entry["stackTrace"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False)

def setup_global_logger():
    root_logger = logging.getLogger()
    root_logger.setLevel(LOG_LEVEL)  # <- Explicitly set root logger level

    # Remove all handlers (e.g. preconfigured by Lambda environment)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add JSON formatter
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    root_logger.addHandler(handler)

    # Optional: force log level on the handler too
    handler.setLevel(LOG_LEVEL)

    # Enrich log records with context
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        context = get_log_context()
        record.correlationId = context["correlationId"]
        record.eventSource = context["eventSource"]
        return record

    old_factory = logging.getLogRecordFactory()
    logging.setLogRecordFactory(record_factory)