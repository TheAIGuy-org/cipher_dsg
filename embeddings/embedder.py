"""
embeddings/embedder.py
-----------------------
Pluggable embedding generation.

Two backends:
  - "local": Deterministic hash-based pseudo-embeddings (384 dims).
    These are NOT semantically meaningful but allow the vector index
    to exist and be swapped out later for real embeddings.
    Use for local dev / POC where no API key is available.

  - "openai": Real embeddings via text-embedding-3-small (1536 dims).
    Requires OPENAI_API_KEY in .env. Semantically meaningful.

The interface is identical — just change EMBEDDING_BACKEND in .env.
The Neo4j vector index is created at the correct dimension automatically.

How to swap in SentenceTransformers later:
  1. pip install sentence-transformers
  2. Add "sentence_transformers" backend below
  3. Change EMBEDDING_BACKEND=sentence_transformers in .env
  No other code changes needed.
"""
from __future__ import annotations
import hashlib
import math
from typing import Protocol

from config.settings import settings
from utils.logger import get_logger

log = get_logger(__name__)


class EmbedderProtocol(Protocol):
    """Interface contract for all embedding backends."""
    def embed(self, text: str) -> list[float]: ...
    def embed_batch(self, texts: list[str]) -> list[list[float]]: ...
    @property
    def dimension(self) -> int: ...


# ── Local (hash-based) backend ───────────────────────────────────────────────

class LocalEmbedder:
    """
    Deterministic pseudo-embedder using SHA-256 hash.

    Produces 384-float vectors from text hashes. Not semantically meaningful
    but completely reproducible, offline, and zero-cost. Perfect for POC
    structural testing. Swap to OpenAI/SentenceTransformers for real semantic
    search capability.
    """
    _DIM = 384

    def embed(self, text: str) -> list[float]:
        if not text or not text.strip():
            return [0.0] * self._DIM

        # Use SHA-256 to get deterministic bytes from text
        hash_bytes = hashlib.sha256(text.encode('utf-8')).digest()

        # Extend to 384 dimensions by cycling through hash with different seeds
        values = []
        seed_idx = 0
        while len(values) < self._DIM:
            seed = f"{seed_idx}:{text[:32]}"
            chunk = hashlib.sha256((seed + text).encode('utf-8')).digest()
            for byte in chunk:
                values.append((byte - 128) / 128.0)  # normalize to [-1, 1]
                if len(values) == self._DIM:
                    break
            seed_idx += 1

        # L2-normalize the vector
        magnitude = math.sqrt(sum(v * v for v in values))
        if magnitude > 0:
            values = [v / magnitude for v in values]

        return values[:self._DIM]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [self.embed(t) for t in texts]

    @property
    def dimension(self) -> int:
        return self._DIM


# ── OpenAI backend ───────────────────────────────────────────────────────────

class OpenAIEmbedder:
    """Real semantic embeddings via OpenAI text-embedding-3-small."""
    _DIM = 1536
    _MODEL = "text-embedding-3-small"

    def __init__(self):
        try:
            from openai import OpenAI
            self._client = OpenAI(api_key=settings.OPENAI_API_KEY)
        except ImportError:
            raise ImportError("openai package required: pip install openai")

    def embed(self, text: str) -> list[float]:
        if not text or not text.strip():
            return [0.0] * self._DIM
        resp = self._client.embeddings.create(
            model=self._MODEL,
            input=text[:8000],  # token limit safety
        )
        return resp.data[0].embedding

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        # OpenAI supports batching natively
        resp = self._client.embeddings.create(
            model=self._MODEL,
            input=[t[:8000] for t in texts],
        )
        return [item.embedding for item in resp.data]

    @property
    def dimension(self) -> int:
        return self._DIM


# ── Azure OpenAI backend ─────────────────────────────────────────────────────

class AzureOpenAIEmbedder:
    """Real semantic embeddings via Azure OpenAI text-embedding-ada-002."""
    _DIM = 1536
    _MODEL = "text-embedding-ada-002"

    def __init__(self):
        try:
            from openai import AzureOpenAI
            import os
            self._client = AzureOpenAI(
                api_key=os.getenv("AZURE_OPENAI_KEY"),
                api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
            )
            # Use deployment name from env, fallback to model name
            self._deployment = os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT", self._MODEL)
        except ImportError:
            raise ImportError("openai package required: pip install openai")

    def embed(self, text: str) -> list[float]:
        if not text or not text.strip():
            return [0.0] * self._DIM
        resp = self._client.embeddings.create(
            model=self._deployment,
            input=text[:8000],  # token limit safety
        )
        return resp.data[0].embedding

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        # Azure OpenAI supports batching natively
        resp = self._client.embeddings.create(
            model=self._deployment,
            input=[t[:8000] for t in texts],
        )
        return [item.embedding for item in resp.data]

    @property
    def dimension(self) -> int:
        return self._DIM


# ── Factory ──────────────────────────────────────────────────────────────────

def get_embedder() -> EmbedderProtocol:
    """
    Factory function — returns the configured embedding backend.
    This is the ONLY place in the codebase that reads EMBEDDING_BACKEND.
    """
    backend = settings.EMBEDDING_BACKEND.lower()
    log.info(f"Embedding backend: {backend}")

    if backend == "openai":
        if not settings.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY must be set when EMBEDDING_BACKEND=openai")
        return OpenAIEmbedder()
    
    elif backend == "azure":
        import os
        if not os.getenv("AZURE_OPENAI_KEY"):
            raise ValueError("AZURE_OPENAI_KEY must be set when EMBEDDING_BACKEND=azure")
        return AzureOpenAIEmbedder()

    elif backend == "local":
        return LocalEmbedder()

    else:
        raise ValueError(
            f"Unknown EMBEDDING_BACKEND='{backend}'. "
            f"Valid options: 'local', 'openai', 'azure'"
        )
