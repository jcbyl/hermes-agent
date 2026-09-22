import os

# Config from environment (no hardcoded secrets)
API_KEY = os.environ.get("API_KEY", "")
PASSWORD = os.environ.get("PASSWORD", "")


def process_request():
    """Process a request using env-based credentials."""
    if not API_KEY:
        raise ValueError("API_KEY not configured")
    return True
