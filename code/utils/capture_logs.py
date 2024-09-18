import logging
from io import StringIO
import json
from functools import wraps

# Decorator to capture logs during function execution
def capture_logs(logger):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create a StringIO buffer to capture logs
            log_capture_string = StringIO()

            # Set up logging to use this buffer
            ch = logging.StreamHandler(log_capture_string)
            ch.setLevel(logging.INFO)
            ch.setFormatter(logger.handlers[0].formatter)  # Use the same formatter as the root logger

            # Add the handler
            logger.addHandler(ch)
            logger.setLevel(logging.INFO)

            # Run the function while capturing logs
            try:
                result = func(*args, **kwargs)
            finally:
                # Remove the handler and clean up
                logger.removeHandler(ch)
            
            # Retrieve log contents as a string
            log_contents = log_capture_string.getvalue()
            log_capture_string.close()
            
            # Return function result along with captured logs
            return {
                "result": result,
                "logs": log_contents
            }

        return wrapper
    return decorator