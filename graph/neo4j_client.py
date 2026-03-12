"""
graph/neo4j_client.py
----------------------
Clean, context-manager-aware Neo4j connection wrapper.

Design:
  - Single connection pool shared across the process.
  - All query execution goes through this client.
  - Never expose raw driver objects outside this module.
  - Supports both read (query) and write (execute_write) transactions.
"""
from __future__ import annotations
from contextlib import contextmanager
from typing import Any, Generator

from neo4j import GraphDatabase, Driver, ManagedTransaction
from config.settings import settings
from utils.logger import get_logger

log = get_logger(__name__)


class Neo4jClient:
    """Thread-safe Neo4j client with connection pool."""

    def __init__(self):
        self._driver: Driver | None = None

    def connect(self) -> None:
        """Open the connection to Neo4j Aura."""
        # Validate credentials at connection time — settings.py intentionally
        # uses .get() defaults so the module can be imported without a .env
        if not settings.NEO4J_URI:
            raise ValueError(
                "NEO4J_URI is not set. Copy .env.example to .env and fill in your credentials."
            )
        if not settings.NEO4J_USERNAME or not settings.NEO4J_PASSWORD:
            raise ValueError(
                "NEO4J_USERNAME and NEO4J_PASSWORD must be set in .env before connecting."
            )

        log.info(f"Connecting to Neo4j: {settings.NEO4J_URI}")
        self._driver = GraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USERNAME, settings.NEO4J_PASSWORD),
        )
        # Verify connectivity immediately
        self._driver.verify_connectivity()
        log.info("Neo4j connection established ✓")

    def close(self) -> None:
        """Close the connection pool."""
        if self._driver:
            self._driver.close()
            self._driver = None
            log.info("Neo4j connection closed")

    @contextmanager
    def session(self) -> Generator:
        """Context manager that yields a Neo4j session."""
        if not self._driver:
            raise RuntimeError("Neo4jClient not connected. Call connect() first.")
        session = self._driver.session(database=settings.NEO4J_DATABASE)
        try:
            yield session
        finally:
            session.close()

    def run_query(self, cypher: str, params: dict | None = None) -> list[dict]:
        """
        Execute a read query and return results as list of dicts.
        Use for SELECT-style queries.
        """
        params = params or {}
        with self.session() as session:
            result = session.run(cypher, params)
            return [record.data() for record in result]

    def run_auto_commit(self, cypher: str, params: dict | None = None) -> list[dict]:
        """
        Execute a single write statement in an auto-commit (non-transactional) session.
        For batches or multi-statement atomicity, use run_write_batch() or run_write_transaction().
        Use for single CREATE/MERGE/SET statements.
        """
        params = params or {}
        with self.session() as session:
            result = session.run(cypher, params)
            return [record.data() for record in result]

    def run_write_batch(self, cypher: str, batch_params: list[dict]) -> None:
        """
        Execute a write query for each item in batch_params.
        Uses a single transaction for efficiency.
        """
        if not batch_params:
            return

        def _write_tx(tx: ManagedTransaction) -> None:
            for params in batch_params:
                tx.run(cypher, params)

        with self.session() as session:
            session.execute_write(_write_tx)

    def run_write_transaction(self, fn, *args, **kwargs) -> Any:
        """
        Execute a custom write function within a managed transaction.
        The function receives (tx, *args, **kwargs).
        """
        with self.session() as session:
            return session.execute_write(fn, *args, **kwargs)

    def clear_database(self) -> None:
        """
        Drop ALL nodes and relationships, then drop all constraints.
        USE WITH CAUTION — for POC/dev only.

        NOTE: Does NOT use APOC — AuraDB Free does not have APOC installed.
        Constraints are dropped individually via SHOW CONSTRAINTS + DROP CONSTRAINT.
        The schema (constraints/indexes) is rebuilt by build_schema() on next run.
        """
        log.warning("Clearing entire Neo4j database...")

        # Step 1: Delete all data first (constraints block DROP if nodes still violate them)
        with self.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        log.info("  All nodes and relationships deleted.")

        # Step 2: Drop all constraints individually (AuraDB-safe, no APOC required)
        with self.session() as session:
            constraints = session.run("SHOW CONSTRAINTS YIELD name RETURN name").data()

        for row in constraints:
            constraint_name = row["name"]
            try:
                with self.session() as session:
                    session.run(f"DROP CONSTRAINT {constraint_name} IF EXISTS")
                log.debug(f"  Dropped constraint: {constraint_name}")
            except Exception as e:
                log.warning(f"  Could not drop constraint '{constraint_name}': {e}")

        log.warning(f"Database cleared. Dropped {len(constraints)} constraint(s).")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


# Module-level singleton — use this throughout the app
client = Neo4jClient()
