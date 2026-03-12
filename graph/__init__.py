"""
graph module
------------
Simplified Neo4j graph operations for LLM-powered system.
"""
from graph.neo4j_client import client, Neo4jClient
from graph.neo4j_schema import build_schema, clear_all_data
from graph.graph_loader import load_dossier
import graph.neo4j_schema as neo4j_schema

__all__ = [
    "client",
    "Neo4jClient",
    "build_schema",
    "clear_all_data",
    "load_dossier",
    "neo4j_schema",
]
