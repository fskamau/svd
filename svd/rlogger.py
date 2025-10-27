import logging
import threading
from datetime import datetime

__all__ = ["get_logger", "get_adapter", "acquire", "release"]

LOG_TIME_FMT = "%H:%M:%S"

LOG_COLORS = {
    "DEBUG": "\033[96m",
    "INFO": "",
    "WARNING": "\033[93m",
    "ERROR": "\033[91m",
    "CRITICAL": "\033[95m",
}
LOG_RESET = "\033[0m"

_log_lock = threading.Lock()


def acquire():
    _log_lock.acquire()


def release():
    _log_lock.release()


class ColoredFormatter(logging.Formatter):
    def format(self, record):
        with _log_lock:
            color = LOG_COLORS.get(record.levelname, "")
            created = datetime.fromtimestamp(record.created).strftime(LOG_TIME_FMT)
            hierarchy_list = getattr(record, "hierarchy_list", [record.name])
            hierarchy = "".join(f"[{name}]" for name in hierarchy_list)
            msg = f"[{created}] {hierarchy} {record.levelname}: {record.getMessage()}"
            return f"{color}{msg}{LOG_RESET}"


def get_logger(name=__name__) -> logging.Logger:
    """
    Returns a base logger with ColoredFormatter.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(ColoredFormatter())
        logger.addHandler(handler)
    logger.propagate = False
    return logger


def get_adapter(logger_or_adapter, name: str) -> logging.LoggerAdapter:
    """
    Returns a LoggerAdapter that adds 'name' to the hierarchy.
    Hierarchy is stored internally as a list to allow clean chaining.
    """
    if isinstance(logger_or_adapter, logging.LoggerAdapter):
        hierarchy_list = list(logger_or_adapter.extra.get("hierarchy_list", []))
        base_logger = logger_or_adapter.logger
    else:
        hierarchy_list = [logger_or_adapter.name]
        base_logger = logger_or_adapter

    hierarchy_list.append(name)
    extra = {
        "hierarchy_list": hierarchy_list
    }
    return logging.LoggerAdapter(base_logger, extra)


if __name__ == "__main__":
    import concurrent.futures
    import time
    import random

    # Base logger
    root = get_logger("rlogger")
    root.setLevel(logging.DEBUG)

    # Add hierarchy dynamically
    app_logger = get_adapter(root, "app")
    service_logger = get_adapter(app_logger, "service1")
    worker_logger = get_adapter(service_logger, "worker")

    def worker(i):
        task_logger = get_adapter(worker_logger, f"task{i}")
        task_logger.info("Starting task")
        sec = random.randint(1, 4)
        if random.choice([True, False]):
            task_logger.warning("A warning occurred")
        task_logger.debug(f"Sleeping for {sec}s")
        time.sleep(sec)
        task_logger.info("Task done")

    root.info("Main started")

    with concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="thread") as executor:
        executor.map(worker, range(1, 9))

    root.critical("Service shutting down")
