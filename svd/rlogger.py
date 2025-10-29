import logging
import threading
from datetime import datetime

__all__ = ["get_logger", "get_adapter"]

LOG_TIME_FMT = "%H:%M:%S"

LOG_COLORS = {
    "DEBUG": "\033[96m",
    "INFO": "",
    "OK": "\033[92m",
    "WARNING": "\033[93m",
    "ERROR": "\033[91m",
    "CRITICAL": "\033[95m",
}
LOG_RESET = "\033[0m"

_log_lock = threading.Lock()

OK_LEVEL = 25
logging.addLevelName(OK_LEVEL, "OK")


def _logger_ok(self, msg, *args, **kwargs):
    """Add .ok() method to base Logger."""
    if self.isEnabledFor(OK_LEVEL):
        self._log(OK_LEVEL, msg, args, **kwargs)


logging.Logger.ok = _logger_ok


class ColoredFormatter(logging.Formatter):
    """Thread-safe formatter adding colors and hierarchy labels."""

    def format(self, record):
        with _log_lock:
            color = LOG_COLORS.get(record.levelname, "")
            created = datetime.fromtimestamp(record.created).strftime(LOG_TIME_FMT)
            hierarchy_list = getattr(record, "hierarchy_list", [record.name])
            hierarchy = "".join(f"[{name}]" for name in hierarchy_list)
            msg = f"[{created}] {hierarchy} {record.levelname}: {record.getMessage()}"
            return f"{color}{msg}{LOG_RESET}"


def get_logger(name=__name__) -> logging.Logger:
    """Return a base logger configured with ColoredFormatter."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(ColoredFormatter())
        logger.addHandler(handler)
    logger.propagate = False
    return logger


def get_adapter(logger_or_adapter, name: str) -> logging.LoggerAdapter:
    """Return a LoggerAdapter that adds hierarchical context and supports .ok()."""
    if isinstance(logger_or_adapter, logging.LoggerAdapter):
        hierarchy_list = list(logger_or_adapter.extra.get("hierarchy_list", []))
        base_logger = logger_or_adapter.logger
    else:
        hierarchy_list = [logger_or_adapter.name]
        base_logger = logger_or_adapter

    hierarchy_list.append(name)
    extra = {"hierarchy_list": hierarchy_list}

    class OKLoggerAdapter(logging.LoggerAdapter):
        """LoggerAdapter supporting the custom OK level."""

        def ok(self, msg, *args, **kwargs):
            self.log(OK_LEVEL, msg, *args, **kwargs)

    return OKLoggerAdapter(base_logger, extra)


if __name__ == "__main__":
    import concurrent.futures
    import time
    import random

    root = get_logger("rlogger")
    root.setLevel(logging.DEBUG)

    app_logger = get_adapter(root, "app")
    service_logger = get_adapter(app_logger, "service1")
    worker_logger = get_adapter(service_logger, "worker")

    def worker(i):
        """Simulated worker task using hierarchical logging."""
        task_logger = get_adapter(worker_logger, f"task{i}")
        task_logger.info("Starting task")
        sec = random.randint(1, 4)
        if random.choice([True, False]):
            task_logger.warning("A warning occurred")
        else:
            task_logger.ok("Task succeeded")
        task_logger.debug(f"Sleeping for {sec}s")
        time.sleep(sec)
        task_logger.info("Task done")

    root.info("Main started")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="thread") as executor:
        executor.map(worker, range(1, 9))

    root.critical("Service shutting down")
