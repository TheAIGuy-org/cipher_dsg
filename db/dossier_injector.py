"""
PHASE 10: Dossier Graph Injection
Updates Neo4j graph with approved content changes.
Handles: new section creation, renumbering, hierarchy updates, versioning.
"""

from typing import Dict, List, Optional
from datetime import datetime
from graph.neo4j_client import Neo4jClient, client as neo4j_singleton
from llm.content_generator import GeneratedContent
from utils.logger import get_logger

logger = get_logger(__name__)


class InjectionResult:
    """Result of dossier injection operation."""
    
    def __init__(self):
        self.success = False
        self.sections_created = []
        self.sections_updated = []
        self.sections_renumbered = []
        self.errors = []
        self.version_created = None
    
    def __str__(self):
        status = "✅ SUCCESS" if self.success else "❌ FAILED"
        return f"""
{status}
  Created: {len(self.sections_created)} sections
  Updated: {len(self.sections_updated)} sections
  Renumbered: {len(self.sections_renumbered)} sections
  Errors: {len(self.errors)}
  Version: {self.version_created}
"""


class DossierInjector:
    """
    Injects approved content into Neo4j dossier graph.
    Handles structural changes (new sections, renumbering, hierarchy).
    """
    
    def __init__(self, neo4j_client: Optional[Neo4jClient] = None):
        """
        Initialize injector.
        
        Args:
            neo4j_client: Connected Neo4j client (uses singleton if not provided)
        """
        self.neo4j = neo4j_client or neo4j_singleton
        logger.info("DossierInjector initialized (Phase 10)")
    
    def inject_approved_content(
        self, 
        content: GeneratedContent,
        author: str = "realtime_agent",
        comment: Optional[str] = None
    ) -> InjectionResult:
        """
        Inject approved content into Neo4j graph.
        
        Args:
            content: Approved generated content
            author: Who approved this change
            comment: Optional comment about the change
            
        Returns:
            InjectionResult with details of what was modified
        """
        result = InjectionResult()
        
        if content.status != "APPROVED":
            result.errors.append("Content not approved")
            logger.error("Cannot inject non-approved content")
            return result
        
        logger.info(f"🚀 Injecting content for {content.product_code} Section {content.section_number}")
        
        try:
            # Create version snapshot before modification
            version_id = self._create_version_snapshot(
                content.product_code,
                author,
                comment or f"Auto-update: {', '.join(content.changes_applied)}"
            )
            result.version_created = version_id
            logger.info(f"   📸 Created version snapshot: {version_id}")
            
            # Step 1: Renumber existing sections if needed
            if content.requires_renumbering:
                renumbered = self._renumber_sections(
                    content.product_code,
                    content.renumbering_map
                )
                result.sections_renumbered = renumbered
                logger.info(f"   🔄 Renumbered {len(renumbered)} sections")
            
            # Step 2: Create or update section
            if content.is_new_section:
                section_id = self._create_new_section(content)
                result.sections_created.append(section_id)
                logger.info(f"   ➕ Created new section: {section_id}")
            else:
                section_id = self._update_existing_section(content)
                result.sections_updated.append(section_id)
                logger.info(f"   ✏️  Updated section: {section_id}")
            
            # Step 3: Update hierarchy relationships
            self._update_hierarchy(content)
            logger.info(f"   🔗 Updated hierarchy relationships")
            
            result.success = True
            logger.info(f"   ✅ Injection complete!")
            
        except Exception as e:
            logger.error(f"Injection failed: {e}", exc_info=True)
            result.errors.append(str(e))
            result.success = False
        
        return result
    
    def _create_version_snapshot(self, product_code: str, author: str, comment: str) -> str:
        """Create a version snapshot before modifying dossier."""
        version_id = f"v_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        query = """
        MATCH (d:Dossier {product_code: $product_code})
        CREATE (v:Version {
            id: $version_id,
            product_code: $product_code,
            timestamp: datetime(),
            author: $author,
            comment: $comment
        })
        CREATE (d)-[:HAS_VERSION]->(v)
        
        // Snapshot all current sections
        WITH v
        MATCH (d:Dossier {product_code: $product_code})-[:HAS_SECTION]->(s:Section)
        CREATE (v)-[:SNAPSHOT_OF]->(s)
        
        RETURN v.id as version_id
        """
        
        result = self.neo4j.run_auto_commit(
            query,
            params={
                "product_code": product_code,
                "version_id": version_id,
                "author": author,
                "comment": comment
            }
        )
        
        return version_id
    
    def _renumber_sections(self, product_code: str, renumbering_map: Dict[str, str]) -> List[str]:
        """
        Renumber existing sections according to map.
        
        Args:
            product_code: Product identifier
            renumbering_map: {old_number: new_number}
            
        Returns:
            List of renumbered section IDs
        """
        renumbered = []
        
        for old_num, new_num in renumbering_map.items():
            query = """
            MATCH (s:Section {product_code: $product_code, section_number: $old_num})
            SET s.section_number = $new_num,
                s.previous_number = $old_num,
                s.renumbered_at = datetime()
            RETURN s.id as section_id
            """
            
            result = self.neo4j.run_auto_commit(
                query,
                params={
                    "product_code": product_code,
                    "old_num": old_num,
                    "new_num": new_num
                }
            )
            
            if result:
                section_id = result[0].get('section_id')
                renumbered.append(f"{old_num} → {new_num}")
                logger.info(f"      Renumbered: {old_num} → {new_num}")
        
        return renumbered
    
    def _create_new_section(self, content: GeneratedContent) -> str:
        """
        Create a new section node with content.
        
        Returns:
            Created section ID
        """
        section_id = f"{content.product_code}_{content.section_number.replace('.', '_')}"
        
        query = """
        MATCH (d:Dossier {product_code: $product_code})
        
        CREATE (s:Section {
            id: $section_id,
            section_number: $section_number,
            title: $title,
            product_code: $product_code,
            full_text: $content,
            content: $content,
            created_at: datetime(),
            created_by: 'realtime_agent',
            version: 1,
            status: 'active'
        })
        
        CREATE (d)-[:HAS_SECTION]->(s)
        
        // Find parent section and link
        WITH s
        OPTIONAL MATCH (parent:Section {
            product_code: $product_code
        })
        WHERE $section_number STARTS WITH parent.section_number + '.'
        AND size(split($section_number, '.')) = size(split(parent.section_number, '.')) + 1
        
        FOREACH (_ IN CASE WHEN parent IS NOT NULL THEN [1] ELSE [] END |
            CREATE (parent)-[:HAS_SUBSECTION {order: toInteger(split($section_number, '.')[-1])}]->(s)
        )
        
        RETURN s.id as section_id
        """
        
        result = self.neo4j.run_auto_commit(
            query,
            params={
                "section_id": section_id,
                "section_number": content.section_number,
                "title": content.section_title,
                "product_code": content.product_code,
                "content": content.generated_text
            }
        )
        
        return section_id
    
    def _update_existing_section(self, content: GeneratedContent) -> str:
        """
        Update existing section with new content.
        Maintains version history.
        
        Returns:
            Updated section ID
        """
        query = """
        MATCH (s:Section {
            product_code: $product_code,
            section_number: $section_number
        })
        
        // Archive old content and update BOTH fields
        SET s.previous_content = COALESCE(s.content, s.full_text),
            s.previous_full_text = s.full_text,
            s.previous_version = COALESCE(s.version, 1),
            s.content = $new_content,
            s.full_text = $new_content,
            s.version = COALESCE(s.version, 1) + 1,
            s.updated_at = datetime(),
            s.updated_by = 'realtime_agent'
        
        RETURN s.id as section_id
        """
        
        result = self.neo4j.run_auto_commit(
            query,
            params={
                "product_code": content.product_code,
                "section_number": content.section_number,
                "new_content": content.generated_text
            }
        )
        
        return result[0].get('section_id') if result else None
    
    def _update_hierarchy(self, content: GeneratedContent):
        """
        Update hierarchy relationships after section changes.
        Recalculates order indices for siblings.
        """
        # Extract parent section number (e.g., 2.2.2 from 2.2.2.2)
        parts = content.section_number.split('.')
        if len(parts) <= 1:
            return  # Top-level section, no parent
        
        parent_number = '.'.join(parts[:-1])
        
        query = """
        MATCH (parent:Section {
            product_code: $product_code,
            section_number: $parent_number
        })
        MATCH (parent)-[r:HAS_SUBSECTION]->(child:Section)
        
        // Sort children by section number and update order
        WITH parent, child, r
        ORDER BY child.section_number
        WITH parent, collect(child) as children, collect(r) as rels
        
        UNWIND range(0, size(children)-1) as idx
        WITH children[idx] as child, rels[idx] as rel, idx
        
        SET rel.order = idx + 1
        
        RETURN count(*) as updated
        """
        
        self.neo4j.run_auto_commit(
            query,
            params={
                "product_code": content.product_code,
                "parent_number": parent_number
            }
        )
    
    def get_section_hierarchy(self, product_code: str, parent_number: str = None) -> List[dict]:
        """
        Retrieve current section hierarchy.
        Useful for displaying before/after structure.
        """
        if parent_number:
            query = """
            MATCH (parent:Section {product_code: $product_code, section_number: $parent_number})
            MATCH (parent)-[r:HAS_SUBSECTION]->(child:Section)
            RETURN child.section_number as number, 
                   child.title as title,
                   r.order as order
            ORDER BY r.order
            """
            params = {"product_code": product_code, "parent_number": parent_number}
        else:
            query = """
            MATCH (s:Section {product_code: $product_code})
            WHERE NOT (s)<-[:HAS_SUBSECTION]-()  // Top-level only
            RETURN s.section_number as number, 
                   s.title as title
            ORDER BY s.section_number
            """
            params = {"product_code": product_code}
        
        results = self.neo4j.run_query(query, params)
        return [dict(r) for r in results]
