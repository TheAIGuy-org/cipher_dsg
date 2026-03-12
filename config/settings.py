"""
config/settings.py
------------------
Single source of truth for all configuration.
Reads from .env file. Never hardcode credentials anywhere else.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
_ROOT = Path(__file__).parent.parent
load_dotenv(_ROOT / ".env")


class Settings:
    # ── Neo4j ──────────────────────────────────────────────────────────
    # Use .get() so importing this module never raises KeyError.
    # Presence of these values is validated lazily in Neo4jClient.connect(),
    # which means run_local_test.py and dry-run modes work without any .env.
    NEO4J_URI: str = os.environ.get("NEO4J_URI", "")
    NEO4J_USERNAME: str = os.environ.get("NEO4J_USERNAME", "")
    NEO4J_PASSWORD: str = os.environ.get("NEO4J_PASSWORD", "")
    NEO4J_DATABASE: str = os.environ.get("NEO4J_DATABASE", "neo4j")

    # ── Embeddings ─────────────────────────────────────────────────────
    # "local" → deterministic hash-based vectors (works offline, no API key)
    # "openai" → text-embedding-3-small via OpenAI API
    EMBEDDING_BACKEND: str = os.environ.get("EMBEDDING_BACKEND", "local")
    OPENAI_API_KEY: str = os.environ.get("OPENAI_API_KEY", "")

    # Embedding dimension:
    # local → 384 (to match common SentenceTransformer dim for future swap)
    # openai text-embedding-3-small → 1536
    EMBEDDING_DIM: int = 384

    # ── Paths ──────────────────────────────────────────────────────────
    PROJECT_ROOT: Path = _ROOT
    DOSSIER_DIR: Path = _ROOT / "data" / "dossiers"
    LOGS_DIR: Path = _ROOT / "logs"

    # ── Logging ────────────────────────────────────────────────────────
    LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO")
    # Write logs to file in addition to console (True/False)
    LOG_TO_FILE: bool = os.environ.get("LOG_TO_FILE", "true").lower() == "true"


settings = Settings()
