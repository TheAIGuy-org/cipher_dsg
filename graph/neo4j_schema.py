"""
graph/neo4j_schema.py
----------------------
SIMPLIFIED Neo4j schema for LLM-powered dynamic system.

REMOVED:
- SituationProfile node (no metadata node)
- TextClause node (no clause classification)
- DatabaseTable node (not needed for MVP)
- Complex constraints and indexes for removed nodes

ADDED:
- Vector embedding on Section node
- Enhanced fulltext search on Section

KEPT:
- Product, DossierVersion, Section nodes
- Hierarchical relationships (HAS_CHILD)
- Product->Dossier->Section containment

This gives us:
1. **Hierarchical discovery** via graph traversal
2. **Semantic search** via vector embeddings
3. **Keyword search** via fulltext index
4. **Pure templates** in Section.full_text

All intelligence moved to LLM layer - no hardcoded metadata!
"""
from graph.neo4j_client import Neo4jClient
from utils.logger import get_logger

log = get_logger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
#  SCHEMA: Constraints & Indexes
# ══════════════════════════════════════════════════════════════════════════════

CONSTRAINT_PRODUCT = """
CREATE CONSTRAINT constraint_product_code IF NOT EXISTS
FOR (p:Product) REQUIRE p.product_code IS UNIQUE
"""

CONSTRAINT_DOSSIER = """
CREATE CONSTRAINT constraint_dossier_id IF NOT EXISTS
FOR (d:DossierVersion) REQUIRE d.dossier_id IS UNIQUE
"""

CONSTRAINT_SECTION = """
CREATE CONSTRAINT constraint_section_id IF NOT EXISTS
FOR (s:Section) REQUIRE s.section_id IS UNIQUE
"""

# Fulltext index on Section (for keyword search)
INDEX_SECTION_FULLTEXT = """
CREATE FULLTEXT INDEX idx_section_fulltext IF NOT EXISTS
FOR (s:Section) ON EACH [s.title, s.full_text, s.section_number]
"""

# Range index on section_number (for efficient lookups)
INDEX_SECTION_NUMBER = """
CREATE INDEX idx_section_number IF NOT EXISTS
FOR (s:Section) ON (s.section_number)
"""

# Vector index on Section.embedding (for semantic search)
# Dimension = 1536 for Azure OpenAI text-embedding-ada-002
VECTOR_INDEX_SECTION = """
CREATE VECTOR INDEX idx_section_embedding IF NOT EXISTS
FOR (s:Section) ON s.embedding
OPTIONS {
  indexConfig: {
    `vector.dimensions`: 1536,
    `vector.similarity_function`: 'cosine'
  }
}
"""


# ══════════════════════════════════════════════════════════════════════════════
#  MERGE QUERIES: Nodes
# ══════════════════════════════════════════════════════════════════════════════

MERGE_PRODUCT = """
MERGE (p:Product {product_code: $product_code})
SET
  p.product_name = $product_name
RETURN p.product_code AS code
"""

MERGE_DOSSIER_VERSION = """
MERGE (d:DossierVersion {dossier_id: $dossier_id})
SET
  d.version_code  = $version_code,
  d.regqual_code  = $regqual_code,
  d.issue_date    = $issue_date,
  d.product_code  = $product_code
RETURN d.dossier_id AS id
"""

MERGE_SECTION = """
MERGE (s:Section {section_id: $section_id})
SET
  s.section_number  = $section_number,
  s.title           = $title,
  s.parent_number   = $parent_number,
  s.full_text       = $full_text,
  s.content_format  = $content_format,
  s.has_table       = $has_table,
  s.has_bullets     = $has_bullets,
  s.embedding       = $embedding,
  s.product_code    = $product_code,
  s.dossier_id      = $dossier_id
RETURN s.section_id AS id
"""


# ══════════════════════════════════════════════════════════════════════════════
#  RELATIONSHIPS
# ══════════════════════════════════════════════════════════════════════════════

CREATE_PRODUCT_DOSSIER = """
MATCH (p:Product {product_code: $product_code})
MATCH (d:DossierVersion {dossier_id: $dossier_id})
MERGE (p)-[:HAS_DOSSIER]->(d)
"""

CREATE_DOSSIER_SECTION = """
MATCH (d:DossierVersion {dossier_id: $dossier_id})
MATCH (s:Section {section_id: $section_id})
MERGE (d)-[:HAS_SECTION]->(s)
"""

CREATE_PARENT_CHILD = """
MATCH (parent:Section {section_number: $parent_number, product_code: $product_code})
MATCH (child:Section {section_number: $child_number, product_code: $product_code})
MERGE (parent)-[:HAS_CHILD]->(child)
"""


# ══════════════════════════════════════════════════════════════════════════════
#  QUERY TEMPLATES
# ══════════════════════════════════════════════════════════════════════════════

# Vector search for similar sections
Q_SEMANTIC_SEARCH = """
CALL db.index.vector.queryNodes('idx_section_embedding', $limit, $query_vector)
YIELD node, score
MATCH (node)<-[:HAS_SECTION]-(d:DossierVersion)<-[:HAS_DOSSIER]-(p:Product)
RETURN 
    node.section_id AS section_id,
    node.section_number AS section_number,
    node.title AS title,
    node.full_text AS full_text,
    node.content_format AS content_format,
    node.parent_number AS parent_number,
    p.product_name AS product_name,
    p.product_code AS product_code,
    d.version_code AS version_code,
    score
ORDER BY score DESC
LIMIT $limit
"""

# Keyword search fallback
Q_KEYWORD_SEARCH = """
CALL db.index.fulltext.queryNodes('idx_section_fulltext', $query)
YIELD node, score
MATCH (node)<-[:HAS_SECTION]-(d:DossierVersion)<-[:HAS_DOSSIER]-(p:Product)
RETURN 
    node.section_id AS section_id,
    node.section_number AS section_number,
    node.title AS title,
    node.full_text AS full_text,
    node.content_format AS content_format,
    node.parent_number AS parent_number,
    p.product_name AS product_name,
    p.product_code AS product_code,
    score
ORDER BY score DESC
LIMIT $limit
"""

# Get hierarchy context for a section
Q_SECTION_HIERARCHY = """
MATCH (section:Section {section_id: $section_id})
OPTIONAL MATCH (parent:Section {section_number: section.parent_number, product_code: section.product_code})
OPTIONAL MATCH (parent)-[:HAS_CHILD]->(sibling:Section)
WHERE sibling.section_id <> $section_id

RETURN 
    section.section_number AS section_number,
    section.title AS title,
    section.parent_number AS parent_number,
    parent.title AS parent_title,
    collect(DISTINCT sibling.section_number) AS sibling_numbers
"""

# Get all sections for a product (for placement decisions)
Q_PRODUCT_SECTIONS = """
MATCH (p:Product {product_code: $product_code})
      -[:HAS_DOSSIER]->(d:DossierVersion)
      -[:HAS_SECTION]->(s:Section)
WHERE s.section_number STARTS WITH '2.2'
RETURN s.section_number AS section_number, 
       s.title AS title,
       s.parent_number AS parent_number
ORDER BY s.section_number
"""

# Count sections (validation)
Q_COUNT_SECTIONS = """
MATCH (:DossierVersion)-[:HAS_SECTION]->(s:Section)
RETURN count(s) AS section_count
"""

# Get all products
Q_ALL_PRODUCTS = """
MATCH (p:Product)-[:HAS_DOSSIER]->(d:DossierVersion)
RETURN 
    p.product_code AS product_code,
    p.product_name AS product_name,
    d.version_code AS version_code,
    d.regqual_code AS regqual_code,
    d.issue_date AS issue_date
ORDER BY p.product_name
"""


# ══════════════════════════════════════════════════════════════════════════════
#  SCHEMA BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_schema(client: Neo4jClient) -> None:
    """
    Build the complete Neo4j schema.
    
    Creates:
    - Constraints (uniqueness)
    - Indexes (fulltext, range, vector)
    
    Idempotent - safe to run multiple times.
    """
    log.info("Building Neo4j schema...")
    
    # Constraints
    log.debug("  Creating constraints...")
    client.run_auto_commit(CONSTRAINT_PRODUCT)
    client.run_auto_commit(CONSTRAINT_DOSSIER)
    client.run_auto_commit(CONSTRAINT_SECTION)
    
    # Indexes
    log.debug("  Creating indexes...")
    client.run_auto_commit(INDEX_SECTION_FULLTEXT)
    client.run_auto_commit(INDEX_SECTION_NUMBER)
    client.run_auto_commit(VECTOR_INDEX_SECTION)
    
    log.info("✅ Schema built successfully")


def clear_all_data(client: Neo4jClient) -> None:
    """
    Delete ALL nodes and relationships.
    
    WARNING: This is destructive! Use only for testing/rebuilding.
    """
    log.warning("⚠️  Clearing ALL data from Neo4j...")
    
    cypher = """
    MATCH (n)
    DETACH DELETE n
    """
    
    client.run_auto_commit(cypher)
    log.info("✅ All data cleared")
