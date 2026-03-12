"""
utils/logger.py
---------------
Centralized structured logger. All modules import from here.
All log files are written to PROJECT_ROOT/logs/ directory.
"""
import logging
import sys
from pathlib import Path
from datetime import datetime
from config.settings import settings


def get_logger(name: str) -> logging.Logger:
    """Return a named logger with consistent formatting."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)

    # Formatter shared by all handlers
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (always enabled)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (optional, controlled by LOG_TO_FILE setting)
    if settings.LOG_TO_FILE:
        # Ensure logs directory exists within codebase
        settings.LOGS_DIR.mkdir(exist_ok=True)
        
        # Create log file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = settings.LOGS_DIR / f"dsg_build_{timestamp}.log"
        
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger
