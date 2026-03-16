"""
Phase 7: Neo4j Update Storage
==============================

Stores generated dossier updates back to Neo4j graph.
Maintains version history and audit trail.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from graph.neo4j_client import client as neo4j_singleton, Neo4jClient
from llm.update_generator import SectionUpdate
from utils.logger import get_logger

log = get_logger(__name__)


class UpdateStorage:
    """
    Stores dossier updates in Neo4j with versioning.
    
    Features:
    - Version control (preserves old content)
    - Audit trail (who, when, why)
    - Rollback support
    - Change history queries
    """
    
    def __init__(self, neo4j_client: Optional[Neo4jClient] = None):
        """Initialize storage handler."""
        self.neo4j_client = neo4j_client or neo4j_singleton
        log.info("UpdateStorage initialized")
    
    def store_update(
        self,
        update: SectionUpdate,
        product_code: str,
        author: str = "system",
        comment: Optional[str] = None
    ) -> bool:
        """
        Store section update in Neo4j.
        
        Args:
            update: Generated section update
            product_code: Product identifier
            author: Who made the update
            comment: Optional comment for audit trail
        
        Returns:
            True if successful
        """
        log.info(f"Storing update for section {update.section_id}")
        
        # Build full section ID
        full_section_id = f"{product_code}__section__{update.section_id}"
        
        try:
            # Create version node
            version_query = """
            MATCH (s:Section {product_code: $product_code, section_id: $full_section_id})
            CREATE (v:SectionVersion {
                section_id: $section_id,
                version_number: coalesce(s.version, 0) + 1,
                content: $new_content,
                previous_content: s.full_text,
                updated_at: datetime($timestamp),
                updated_by: $author,
                comment: $comment,
                confidence_score: $confidence,
                changes_applied: $changes,
                strategy: $strategy
            })
            SET s.full_text = $new_content,
                s.version = coalesce(s.version, 0) + 1,
                s.last_updated = datetime($timestamp)
            CREATE (s)-[:HAS_VERSION]->(v)
            RETURN v.version_number AS version
            """
            
            result = self.neo4j_client.run_query(
                version_query,
                {
                    'product_code': product_code,
                    'full_section_id': full_section_id,
                    'section_id': update.section_id,  # Short ID for version node
                    'new_content': update.updated_content,
                    'timestamp': datetime.utcnow().isoformat(),
                    'author': author,
                    'comment': comment or f"Auto-update: {', '.join(update.changes_applied[:2])}",
                    'confidence': update.confidence_score,
                    'changes': update.changes_applied,
                    'strategy': update.strategy.value
                }
            )
            
            if result:
                version_num = result[0]['version']
                log.info(f"✅ Stored version {version_num} for {update.section_id}")
                return True
            else:
                log.error("Failed to store update")
                return False
        
        except Exception as e:
            log.error(f"Storage failed: {e}", exc_info=True)
            return False
    
    def store_batch(
        self,
        updates: List[SectionUpdate],
        product_code: str,
        author: str = "system"
    ) -> Dict[str, bool]:
        """
        Store multiple updates in batch.
        
        Args:
            updates: List of section updates
            product_code: Product identifier
            author: Who made updates
        
        Returns:
            Dict mapping section_id → success status
        """
        log.info(f"Storing batch of {len(updates)} updates")
        
        results = {}
        for update in updates:
            success = self.store_update(
                update=update,
                product_code=product_code,
                author=author,
                comment=f"Batch update: {update.section_title}"
            )
            results[update.section_id] = success
        
        successful = sum(1 for v in results.values() if v)
        log.info(f"Batch complete: {successful}/{len(updates)} successful")
        
        return results
    
    def get_version_history(
        self,
        product_code: str,
        section_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get version history for a section.
        
        Args:
            product_code: Product identifier
            section_id: Section identifier
            limit: Max versions to return
        
        Returns:
            List of version dicts (newest first)
        """
        # Build full section ID
        full_section_id = f"{product_code}__section__{section_id}"
        
        query = """
        MATCH (s:Section {product_code: $product_code, section_id: $full_section_id})
        MATCH (s)-[:HAS_VERSION]->(v:SectionVersion)
        RETURN v.version_number AS version,
               v.updated_at AS timestamp,
               v.updated_by AS author,
               v.comment AS comment,
               v.confidence_score AS confidence,
               v.strategy AS strategy,
               size(v.content) AS content_length
        ORDER BY v.version_number DESC
        LIMIT $limit
        """
        
        try:
            results = self.neo4j_client.run_query(
                query,
                {'product_code': product_code, 'full_section_id': full_section_id, 'limit': limit}
            )
            return results
        except Exception as e:
            log.error(f"Failed to get version history: {e}")
            return []


# Singleton
_storage_instance: Optional[UpdateStorage] = None


def get_update_storage(neo4j_client: Optional[Neo4jClient] = None) -> UpdateStorage:
    """Get singleton UpdateStorage instance."""
    global _storage_instance
    
    if _storage_instance is None:
        _storage_instance = UpdateStorage(neo4j_client=neo4j_client)
    
    return _storage_instance
