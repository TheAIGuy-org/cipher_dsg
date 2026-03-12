"""
graph/simple_loader.py
-----------------------
SIMPLIFIED dossier loader for LLM-powered system.

REMOVED (200+ lines):
- _load_situation() - no SituationProfile node
- _load_clauses() - no TextClause nodes
- _load_db_table_edges() - no DatabaseTable mapping
- _make_situation_id() - no situation hashing

KEPT (100 lines):
- _load_product()
- _load_dossier_version()
- _load_section() - now includes embedding
- _load_parent_edges() - hierarchical relationships

This loader is SIMPLE - it just stores:
1. Product metadata
2. Dossier version info
3. Section with full_text + embedding
4. Hierarchical parent-child relationships

No metadata extraction, no classification - all that moved to LLM layer!
"""
from typing import List

from parsers.data_models import ParsedDossier, ParsedSection
from graph.neo4j_client import Neo4jClient
from graph import neo4j_schema
from utils.logger import get_logger

log = get_logger(__name__)


def load_dossier(
    dossier: ParsedDossier,
    client: Neo4jClient,
    skip_embeddings: bool = False
) -> None:
    """
    Load a parsed dossier into Neo4j.
    
    This is the SIMPLIFIED version - no complex metadata, just structure + text.
    
    Args:
        dossier: Parsed dossier from dossier_parser
        client: Neo4j client
        skip_embeddings: If True, don't include embeddings (for testing)
    """
    log.info(f"Loading dossier: {dossier.product_name} ({dossier.product_code})")
    
    # Step 1: Load Product node
    _load_product(dossier, client)
    
    # Step 2: Load DossierVersion node
    _load_dossier_version(dossier, client)
    
    # Step 3: Load Section nodes with embeddings
    for section in dossier.sections:
        _load_section(section, dossier, client, skip_embeddings)
    
    # Step 4: Create hierarchical relationships
    _load_parent_edges(dossier, client)
    
    log.info(f"✅ Loaded {len(dossier.sections)} sections for {dossier.product_name}")


def _load_product(dossier: ParsedDossier, client: Neo4jClient) -> None:
    """Create or update Product node."""
    client.run_auto_commit(
        neo4j_schema.MERGE_PRODUCT,
        {
            "product_code": dossier.product_code,
            "product_name": dossier.product_name,
        }
    )
    log.debug(f"  ✓ Product: {dossier.product_name}")


def _load_dossier_version(dossier: ParsedDossier, client: Neo4jClient) -> None:
    """Create or update DossierVersion node."""
    dossier_id = f"{dossier.product_code}_{dossier.version_code}"
    
    client.run_auto_commit(
        neo4j_schema.MERGE_DOSSIER_VERSION,
        {
            "dossier_id": dossier_id,
            "version_code": dossier.version_code,
            "regqual_code": dossier.regqual_code,
            "issue_date": dossier.issue_date,
            "product_code": dossier.product_code,
        }
    )
    
    # Create Product -> Dossier relationship
    client.run_auto_commit(
        neo4j_schema.CREATE_PRODUCT_DOSSIER,
        {
            "product_code": dossier.product_code,
            "dossier_id": dossier_id,
        }
    )
    
    log.debug(f"  ✓ Dossier: {dossier_id}")


def _load_section(
    section: ParsedSection,
    dossier: ParsedDossier,
    client: Neo4jClient,
    skip_embeddings: bool = False
) -> None:
    """
    Create or update Section node with embedding.
    
    This is where the magic happens - we store:
    - Full text (the template for generation)
    - Embedding (for semantic search)
    - Format metadata (for choosing generation strategy)
    """
    section_id = _make_section_id(dossier.product_code, section.section_number)
    dossier_id = f"{dossier.product_code}_{dossier.version_code}"
    
    # Prepare embedding (empty list if skipping)
    embedding = section.embedding if not skip_embeddings else []
    
    client.run_auto_commit(
        neo4j_schema.MERGE_SECTION,
        {
            "section_id": section_id,
            "section_number": section.section_number,
            "title": section.title,
            "parent_number": section.parent_number,
            "full_text": section.full_text,
            "content_format": section.content_format,
            "has_table": section.has_table,
            "has_bullets": section.has_bullets,
            "embedding": embedding,
            "product_code": dossier.product_code,
            "dossier_id": dossier_id,
        }
    )
    
    # Create Dossier -> Section relationship
    client.run_auto_commit(
        neo4j_schema.CREATE_DOSSIER_SECTION,
        {
            "dossier_id": dossier_id,
            "section_id": section_id,
        }
    )
    
    log.debug(f"  ✓ Section {section.section_number}: {section.title} "
              f"({"with" if not skip_embeddings else "without"} embedding)")


def _load_parent_edges(dossier: ParsedDossier, client: Neo4jClient) -> None:
    """
    Create hierarchical parent-child relationships between sections.
    
    This is CRITICAL for LLM to understand organizational structure.
    """
    for section in dossier.sections:
        if section.parent_number:  # Has a parent
            client.run_auto_commit(
                neo4j_schema.CREATE_PARENT_CHILD,
                {
                    "parent_number": section.parent_number,
                    "child_number": section.section_number,
                    "product_code": dossier.product_code,
                }
            )
    
    log.debug(f"  ✓ Created hierarchical relationships")


def _make_section_id(product_code: str, section_number: str) -> str:
    """Generate unique section ID."""
    return f"{product_code}__section__{section_number}"
