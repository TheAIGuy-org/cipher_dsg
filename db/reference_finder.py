"""
db/reference_finder.py
----------------------
Phase 6: Cross-Dossier Reference Finder

Finds TOP-K candidate references, then LLM selects best one.

INTELLIGENT SELECTION APPROACH:
1. RAG: Get top-K=3 semantically similar sections
2. LLM: Analyze all candidates and pick the best based on:
   - Format appropriateness
   - Content structure match
   - Applicability to the change

This is truly adaptive - not "section exists → use it" rigidity.
"""
from typing import List, Dict, Optional
from pydantic import BaseModel, Field
from graph.neo4j_client import Neo4jClient, client as neo4j_singleton
from embeddings.embedder import EmbedderProtocol, get_embedder
from llm.azure_client import AzureLLMClient, get_llm_client
from utils.logger import get_logger
import numpy as np

log = get_logger(__name__)


class ReferenceSelectionOutput(BaseModel):
    """LLM output for selecting best reference from candidates."""
    selected_index: int = Field(
        ...,
        description="Index of selected reference (0-based)",
        ge=0
    )
    reasoning: str = Field(
        ...,
        description="Why this reference is most appropriate",
        min_length=50
    )
    format_match: str = Field(
        ...,
        description="How well the format matches the need",
        pattern="^(excellent|good|acceptable|poor)$"
    )


class CrossDossierReferenceFinder:
    """
    Finds and intelligently selects best reference section from other dossiers.

    NEW APPROACH:
    1. RAG: Get top-K candidates by semantic similarity
    2. LLM: Analyze all candidates and select most appropriate
    3. Return: Best reference for content generation

    This removes rigid "section exists → use it" logic in favor of
    intelligent selection from multiple options.
    """

    def __init__(
        self,
        neo4j_client: Optional[Neo4jClient] = None,
        embedder: Optional[EmbedderProtocol] = None,
        llm: Optional[AzureLLMClient] = None
    ):
        """
        Initialize reference finder with LLM selector.

        Args:
            neo4j_client: Neo4j client for graph queries
            embedder: Embedder for semantic comparison
            llm: LLM for intelligent reference selection
        """
        self.neo4j = neo4j_client or neo4j_singleton
        self.embedder = embedder or get_embedder()
        self.llm = llm or get_llm_client()

        log.info("CrossDossierReferenceFinder initialized")

    def find_reference_section(
        self,
        target_product_code: str,
        concept: str,
        new_situation_description: str,
        top_k: int = 3
    ) -> List[Dict]:
        """
        Find TOP-K candidate reference sections for LLM-based selection.

        Steps:
        1. Graph filter: Find sections with similar concept
        2. Semantic rank: Get top-K by similarity
        3. Return all K candidates for LLM selection

        Args:
            target_product_code: Product code to EXCLUDE
            concept: Regulatory concept to search for
            new_situation_description: Inferred new situation
            top_k: Number of candidates to return (default: 3)

        Returns:
            List of top-K reference section dicts (or empty if none found)
        """
        log.info(
            f"Finding reference for concept '{concept}' "
            f"(target product: {target_product_code})"
        )

        candidates = self._get_candidate_sections(
            target_product_code=target_product_code,
            concept=concept,
            top_k=top_k
        )

        if not candidates:
            log.info(f"No candidate sections found for concept '{concept}'")
            return []

        log.info(f"Found {len(candidates)} candidate sections")

        ranked = self._rank_by_semantic_similarity(
            new_situation_description=new_situation_description,
            candidates=candidates
        )

        top_candidates = ranked[:top_k]

        log.info(f"Returning top {len(top_candidates)} candidates for LLM selection:")
        for idx, candidate in enumerate(top_candidates, 1):
            log.info(
                f"  {idx}. Product {candidate['product_code']} "
                f"Section {candidate['section_number']} "
                f"(similarity: {candidate['similarity_score']:.3f})"
            )

        return top_candidates

    def select_best_reference_with_llm(
        self,
        candidates: List[Dict],
        concept: str,
        new_situation: str,
        change_description: str,
        target_section_info: Optional[Dict] = None
    ) -> Optional[Dict]:
        """
        Use LLM to intelligently select best reference from K candidates.

        Args:
            candidates: Top-K candidates from RAG (already ranked by similarity)
            concept: Regulatory concept being addressed
            new_situation: Description of what needs to be added/changed
            change_description: Human description of the compliance change
            target_section_info: Optional info about target section (if exists)

        Returns:
            Best reference dict or None if no suitable reference
        """
        if not candidates:
            log.info("No candidates provided - returning None")
            return None

        if len(candidates) == 1:
            log.info(f"Only one candidate - auto-selecting: {candidates[0]['section_number']}")
            return candidates[0]

        log.info(f"LLM selecting best from {len(candidates)} candidates...")

        # Build candidate descriptions for LLM
        candidates_text = self._format_candidates_for_llm(candidates)

        # Build target section context
        target_context = ""
        if target_section_info:
            full_text = target_section_info.get('full_text', '').strip()
            content_status = (
                "Empty / no content yet"
                if not target_section_info.get('has_content')
                else f"{target_section_info.get('content_length', 0)} chars"
            )
            target_context = f"""
TARGET SECTION CONTEXT:
- Section: {target_section_info.get('section_number', 'Unknown')}
- Title: {target_section_info.get('title', 'Unknown')}
- Current Format: {target_section_info.get('content_format', 'Unknown')}
- Current Content Status: {content_status}
- Current Full Text:
{full_text if full_text else '(no content)'}

IMPORTANT: The candidates below are from OTHER products. Use the target section's
current content above as the baseline — the selected reference should complement
or extend this existing structure, not replace it wholesale.
"""
            log.info(
                f"  Target section full_text "
                f"({target_section_info.get('section_number', '?')}): "
                f"{(full_text or '(empty)')[:500]}"
            )

        system_prompt = """You are an expert regulatory document format and structure analyst. Your task is to select the most optimal template/reference from a list of candidates to format new regulatory data.

Your goal is strictly to identify the best structural and stylistic template for the new data. Do not focus purely on semantic meaning, but focus primarily on whether the candidate's layout, tables, bullet points, or prose style can seamlessly accommodate the shape and volume of the new data requirements.

EVALUATION CRITERIA:
1. Data Shape Match (PRIMARY): If the new requirement introduces a list of items (e.g., multiple ingredients), prioritize candidates that naturally use lists or tables. If the requirement introduces single threshold values, prioritize candidates with clean key-value prose or simple paragraphs.
2. Structural Capacity: Does the candidate have the structural capacity to house the described change? E.g., if we are adding structured safety limits for a chemical, a paragraph might be messy, but a table layout from a candidate would be excellent.
3. Tone and Vocabulary Alignment: Does the candidate employ a formal, regulatory tone that fits standard compliance declarations?
4. Target Section Transition: If the target section exists but is currently empty or states "None," prioritize candidates that provide a robust framework to transition the target section into an active, data-rich state.

INSTRUCTIONS:
- Review the Change Requirements and Target Context.
- Analyze ALL Candidate References provided.
- You MUST select the single best candidate. Provide the 0-based integer index of your choice.
- Justify your choice by comparing its structural advantages over the others."""

        user_prompt = f"""CHANGE REQUIREMENTS:
Concept: {concept}
New Situation: {new_situation}
Specific Changes:
{change_description}

{target_context}

CANDIDATE REFERENCES:
{candidates_text}

Analyze the candidates against the evaluation criteria and select the most appropriate reference format skeleton to house the new changes.
Output JSON strictly conforming to the schema with:
- selected_index: (int) 0-based index of best candidate
- reasoning: (str) Clear explanation detailing structural/format advantages
- format_match: (str) "excellent", "good", "acceptable", or "poor\""""

        try:
            selection = self.llm.ask_structured_pydantic(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=ReferenceSelectionOutput
            )

            selected_idx = selection.selected_index
            if selected_idx < 0 or selected_idx >= len(candidates):
                log.error(f"Invalid selection index: {selected_idx} (candidates: {len(candidates)})")
                selected_idx = 0

            selected = candidates[selected_idx]

            print(f"LLM selected reference #{selected_idx + 1}: Product {selected['product_code']} Section {selected['section_number']}")
            log.info(f"concept: {concept}")
            log.info(f"new_situation: {new_situation}")
            log.info(f"change_description: {change_description}")
            log.info(
                f"✅ LLM selected reference #{selected_idx + 1}: "
                f"Product {selected['product_code']} Section {selected['section_number']}"
            )
            log.info(f"   Reasoning: {selection.reasoning}")
            log.info(f"   Format match: {selection.format_match}")

            return selected

        except Exception as e:
            log.error(f"LLM selection failed: {e}")
            log.info("Fallback: Using first candidate")
            return candidates[0]

    def _format_candidates_for_llm(self, candidates: List[Dict]) -> str:
        """
        Format candidates into readable text for LLM analysis.

        Provides FULL content (no truncation) so LLM can evaluate:
        - Format style (bullets, tables, prose)
        - Completeness (multiple items vs single)
        - Detail level (comprehensive vs basic)
        - Best match for the specific change type

        Args:
            candidates: List of candidate section dicts

        Returns:
            Formatted candidates text with FULL content
        """
        formatted = []
        for idx, candidate in enumerate(candidates):
            full_content = candidate.get('full_text', '')
            content_length = len(full_content)

            candidate_text = f"""
[{idx}] Product: {candidate['product_code']} - {candidate.get('product_name', 'Unknown')}
    Section: {candidate['section_number']} - {candidate['title']}
    Format: {candidate.get('content_format', 'Unknown')}
    Content Length: {content_length} chars
    Similarity Score: {candidate.get('similarity_score', 0):.3f}
    Semantic Description: {candidate.get('semantic_description', 'N/A')}
    Characteristics: {candidate.get('semantic_characteristics', 'N/A')}

    FULL CONTENT:
{full_content}
"""
            formatted.append(candidate_text)

        return "\n".join(formatted)

    def _get_candidate_sections(
        self,
        target_product_code: str,
        concept: str,
        top_k: int = 3
    ) -> List[Dict]:
        """
        Query Neo4j for sections that address similar concept in OTHER products.

        Args:
            target_product_code: Product to exclude
            concept: Concept to search for
            top_k: Max candidates to return (default: 3)

        Returns:
            List of candidate section dicts (up to top_k)
        """
        log.debug(f"Querying graph for sections with concept '{concept}'")

        concept_embedding = self.embedder.embed(concept)

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

            candidates = []
            for section in results:
                section_concepts_text = ", ".join(section['domain_concepts'])
                if not section_concepts_text:
                    continue

                concepts_embedding = self.embedder.embed(section_concepts_text)
                similarity = self._cosine_similarity(concept_embedding, concepts_embedding)

                if similarity > 0.5:
                    section['concept_similarity'] = similarity
                    candidates.append(section)

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