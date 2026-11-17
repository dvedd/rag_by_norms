import logging
import traceback


def log_failure(logger: logging.Logger, operation: str, error: BaseException) -> None:
    """Log the failure type and stack locations without sensitive values."""
    frames = traceback.extract_tb(tb=error.__traceback__)
    locations = "\n".join(
        f"  {frame.filename}:{frame.lineno} in {frame.name}" for frame in frames
    )
    logger.error("%s failed (%s)\n%s", operation, type(error).__name__, locations)
