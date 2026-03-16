"""
db/reference_finder.py
----------------------
Phase 6: Cross-Dossier Reference Finder

Finds best reference section from other dossiers when NEW_PATTERN detected.
Uses structured graph filtering + semantic ranking.

This is the key component for cross-dossier learning - enables the system
to find templates from other products when creating new sections.
"""
from typing import List, Dict, Optional
from graph.neo4j_client import Neo4jClient, client as neo4j_singleton
from embeddings.embedder import EmbedderProtocol, get_embedder
from utils.logger import get_logger
import numpy as np

log = get_logger(__name__)


class CrossDossierReferenceFinder:
    """
    Finds best reference section from other dossiers when pattern changes.
    Uses structured graph filtering + semantic ranking.
    
    Workflow:
    1. Graph filter: Find sections with similar concept in other products
    2. Semantic rank: Compare new situation embedding to candidate profiles
    3. Return top-1 reference (or None if no suitable reference exists)
    
    NOTE: Returning None is NOT an error - means no existing reference.
    Pipeline will mark plan as PENDING_MANUAL_TEMPLATE.
    """
    
    def __init__(
        self,
        neo4j_client: Optional[Neo4jClient] = None,
        embedder: Optional[EmbedderProtocol] = None
    ):
        """
        Initialize reference finder.
        
        Args:
            neo4j_client: Neo4j client for graph queries
            embedder: Embedder for semantic comparison
        """
        self.neo4j = neo4j_client or neo4j_singleton
        self.embedder = embedder or get_embedder()
        
        log.info("CrossDossierReferenceFinder initialized")
    
    def find_reference_section(
        self,
        target_product_code: str,
        concept: str,
        new_situation_description: str,
        top_k: int = 5
    ) -> Optional[Dict]:
        """
        Find best reference section across all other dossiers.
        
        Steps:
        1. Graph filter: Find sections with similar concept in other products
        2. Semantic rank: Compare new_situation to candidate profiles
        3. Return top-1 reference (or None if no suitable reference exists)
        
        Args:
            target_product_code: Product code to EXCLUDE (don't search own product)
            concept: Regulatory concept to search for
            new_situation_description: Inferred new situation after changes
            top_k: Number of candidates to consider before ranking
        
        Returns:
            Best reference section dict or None if no reference found
        """
        log.info(
            f"Finding reference for concept '{concept}' "
            f"(target product: {target_product_code})"
        )
        
        # Step 1: Structured filter - get candidate sections
        candidates = self._get_candidate_sections(
            target_product_code=target_product_code,
            concept=concept,
            top_k=top_k
        )
        
        if not candidates:
            log.info(
                f"No candidate sections found for concept '{concept}' - "
                f"will require manual template"
            )
            return None  # Not an error - just no reference available
        
        log.info(f"Found {len(candidates)} candidate sections")
        
        # Step 2: Semantic ranking - compare to new situation
        ranked = self._rank_by_semantic_similarity(
            new_situation_description=new_situation_description,
            candidates=candidates
        )
        
        if ranked:
            best_match = ranked[0]
            log.info(
                f"Best reference: Product {best_match['product_code']} "
                f"Section {best_match['section_number']} "
                f"(similarity: {best_match['similarity_score']:.3f})"
            )
            return best_match
        
        return None
    
    def _get_candidate_sections(
        self,
        target_product_code: str,
        concept: str,
        top_k: int = 10
    ) -> List[Dict]:
        """
        Query Neo4j for sections that address similar concept in OTHER products.
        Uses semantic search on domain_concepts.
        
        Args:
            target_product_code: Product to exclude
            concept: Concept to search for
            top_k: Max candidates to return
        
        Returns:
            List of candidate section dicts
        """
        log.debug(f"Querying graph for sections with concept '{concept}'")
        
        # Embed the concept for similarity search
        concept_embedding = self.embedder.embed(concept)
        
        # Query sections from OTHER products
        query = """
        MATCH (p:Product)-[:HAS_DOSSIER]->(d:DossierVersion)-[:HAS_SECTION]->(s:Section)
        WHERE p.product_code <> $target_product_code
          AND s.domain_concepts IS NOT NULL
          AND size(s.domain_concepts) > 0
          AND s.semantic_embedding IS NOT NULL
        RETURN s.section_id AS section_id,
               s.section_number AS section_number,
               s.title AS title,
               s.full_text AS full_text,
               s.content_format AS content_format,
               s.parent_number AS parent_number,
               s.semantic_description AS semantic_description,
               s.semantic_embedding AS semantic_embedding,
               s.semantic_characteristics AS semantic_characteristics,
               s.domain_concepts AS domain_concepts,
               p.product_code AS product_code,
               p.product_name AS product_name,
               d.dossier_id AS dossier_id
        LIMIT 50
        """
        
        try:
            results = self.neo4j.run_query(
                query,
                {"target_product_code": target_product_code}
            )
            
            if not results:
                log.debug("No sections found in other products")
                return []
            
            # Filter by semantic similarity to concept
            candidates = []
            for section in results:
                # Check if domain concepts semantically match
                section_concepts_text = ", ".join(section['domain_concepts'])
                if not section_concepts_text:
                    continue
                
                concepts_embedding = self.embedder.embed(section_concepts_text)
                similarity = self._cosine_similarity(concept_embedding, concepts_embedding)
                
                # Threshold for concept relevance (relaxed to 0.5 to get more candidates)
                if similarity > 0.5:
                    section['concept_similarity'] = similarity
                    candidates.append(section)
            
            # Sort by concept similarity and take top_k
            candidates.sort(key=lambda x: x['concept_similarity'], reverse=True)
            top_candidates = candidates[:top_k]
            
            log.debug(f"Filtered to {len(top_candidates)} candidates with concept match")
            return top_candidates
            
        except Exception as e:
            log.error(f"Failed to get candidate sections: {e}", exc_info=True)
            return []
    
    def _rank_by_semantic_similarity(
        self,
        new_situation_description: str,
        candidates: List[Dict]
    ) -> List[Dict]:
        """
        Rank candidates by semantic similarity to new_situation.
        
        Args:
            new_situation_description: Inferred new situation after changes
            candidates: Candidate sections from graph
        
        Returns:
            Ranked list of candidates (best first)
        """
        # Embed the new situation
        new_embedding = self.embedder.embed(new_situation_description)
        
        scored = []
        for candidate in candidates:
            try:
                candidate_embedding = candidate['semantic_embedding']
                similarity = self._cosine_similarity(new_embedding, candidate_embedding)
                
                candidate['similarity_score'] = similarity
                scored.append(candidate)
            except Exception as e:
                log.warning(f"Failed to score candidate {candidate.get('section_id')}: {e}")
        
        # Sort by similarity descending
        scored.sort(key=lambda x: x['similarity_score'], reverse=True)
        
        score_text = f"(score: {scored[0]['similarity_score']:.3f})" if scored else "(score: 0)"
        log.debug(
            f"Top reference: {scored[0]['section_number'] if scored else 'None'} "
            f"{score_text}"
        )
        
        return scored
    
    @staticmethod
    def _cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
        """
        Calculate cosine similarity between two vectors.
        
        Args:
            vec1: First vector
            vec2: Second vector
        
        Returns:
            Similarity score (0.0 to 1.0)
        """
        try:
            a = np.array(vec1)
            b = np.array(vec2)
            return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
        except Exception as e:
            log.warning(f"Similarity calculation failed: {e}")
            return 0.0
    
    def get_section_hierarchy(self, section_id: str) -> Dict:
        """
        Get hierarchical context for a section (parent, siblings).
        
        Args:
            section_id: Section identifier
        
        Returns:
            Hierarchy context dict
        """
        query = """
        MATCH (s:Section {section_id: $section_id})
        OPTIONAL MATCH (parent:Section)
        WHERE parent.section_number = s.parent_number
          AND parent.product_code = s.product_code
        OPTIONAL MATCH (sibling:Section)
        WHERE sibling.parent_number = s.parent_number
          AND sibling.product_code = s.product_code
          AND sibling.section_id <> s.section_id
        RETURN s.section_number AS section_number,
               s.parent_number AS parent_number,
               parent.section_number AS parent_section_number,
               parent.title AS parent_title,
               collect(DISTINCT {number: sibling.section_number, title: sibling.title}) AS siblings
        """
        
        try:
            result = self.neo4j.run_query(query, {"section_id": section_id})
            return result[0] if result else {}
        except Exception as e:
            log.error(f"Failed to get hierarchy for {section_id}: {e}")
            return {}


# Singleton
_finder_instance: Optional[CrossDossierReferenceFinder] = None


def get_reference_finder(
    neo4j_client: Optional[Neo4jClient] = None,
    embedder: Optional[EmbedderProtocol] = None
) -> CrossDossierReferenceFinder:
    """
    Get singleton CrossDossierReferenceFinder instance.
    
    Args:
        neo4j_client: Optional Neo4j client
        embedder: Optional embedder
    
    Returns:
        CrossDossierReferenceFinder instance
    """
    global _finder_instance
    
    if _finder_instance is None:
        _finder_instance = CrossDossierReferenceFinder(
            neo4j_client=neo4j_client,
            embedder=embedder
        )
    
    return _finder_instance
