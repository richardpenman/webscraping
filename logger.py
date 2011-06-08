__doc__ = """Webscraping logger"""

import logging
import threading
from webscraping import settings

lock = threading.Lock()

def synchronous(f):
    def call(*args, **kwargs):
        lock.acquire()
        try:
            return f(*args, **kwargs)
        finally:
            lock.release()
    return call

@synchronous
def get_logger(output_file, stdout=True, level=logging.DEBUG):
    """Create a logger instance
    """
    logger = logging.getLogger(output_file)
    # void duplicate handlers
    if not logger.handlers:
        file_handler = logging.FileHandler(output_file)
        file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(file_handler)
        if stdout:
            logger.addHandler(logging.StreamHandler())
        logger.setLevel(level)
    return logger