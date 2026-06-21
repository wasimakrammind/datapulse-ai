import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(__file__))

DB_PATH = os.getenv("DB_PATH", os.path.join(BASE_DIR, "data", "datapulse.db"))

API_KEY = os.getenv("API_KEY", "datapulse-dev-key-2024")
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")

SLA_RUNTIME_SECONDS = int(os.getenv("SLA_RUNTIME_SECONDS", "300"))
ANOMALY_VOLUME_THRESHOLD = float(os.getenv("ANOMALY_VOLUME_THRESHOLD", "0.3"))

DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
QUARANTINE_DIR = os.path.join(DATA_DIR, "quarantine")
