import logging
import traceback


def log_failure(logger: logging.Logger, operation: str, error: BaseException) -> None:
    """Record an exception type and stack frames without sensitive values."""
    frames = traceback.extract_tb(tb=error.__traceback__)
    stack = "\n".join(
        f"  File {frame.filename!r}, line {frame.lineno}, in {frame.name}"
        for frame in frames
    )

    logger.error("%s failed (%s)\n%s", operation, type(error).__name__, stack)
