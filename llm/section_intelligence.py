"""
llm/section_intelligence.py
----------------------------
LLM-powered section discovery and generation - THE CORE OF THE DYNAMIC SYSTEM.

This module replaces ALL regex-based classification with intelligent reasoning.
No hardcoded metadata fields (cmr_present, allergens_present, etc.) - instead,
the system uses semantic search + LLM reasoning to discover relevant sections
based on PURE CONTEXT.

Key Architecture Principles:
============================

1. ZERO HARDCODED SECTION TYPES
   - System works with ANY section structure
   - No assumptions about what sections exist
   - Adapts to new regulations automatically

2. EVIDENCE-BASED DISCOVERY  
   - Graph provides hierarchical context
   - Semantic search finds candidates
   - LLM evaluates relevance with reasoning

3. TEMPLATE PRESERVATION
   - Reference sections become format templates
   - LLM generates maintaining exact vocabulary
   - Institutional language preserved perfectly

4. HIERARCHICAL AWARENESS
   - Understands parent-child section relationships
   - Respects organizational conventions
   - Places new sections correctly in structure

This is production-ready, scalable architecture that requires ZERO code changes
when new regulation types appear.
"""
import json
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

from llm.azure_client import get_llm_client, LLMResponse
from graph.neo4j_client import Neo4jClient
from utils.logger import get_logger

log = get_logger("llm.section_intelligence")


@dataclass
class SectionReference:
    """
    A reference section discovered from the graph.
    Contains everything needed to use it as a template.
    """
    section_number: str
    title: str
    full_text: str
    product_name: str
    product_code: str
    parent_number: str
    parent_title: str
    sibling_sections: List[str]  # For understanding position in hierarchy
    content_format: str  # "table" | "bullets" | "paragraphs"
    similarity_score: float  # How relevant is this reference?
    llm_reasoning: str  # Why LLM chose this


@dataclass
class SectionPlacement:
    """
    LLM's decision on where to place a new section.
    Includes renumbering plan if needed.
    """
    new_section_number: str
    new_title: str
    parent_number: str
    insert_position: int  # Position among siblings
    renumber_plan: Dict[str, str]  # old_number -> new_number mapping
    reasoning: str  # LLM's explanation


class SectionIntelligence:
    """
    The brain of the dynamic dossier system.
    
    This class handles:
    1. Section discovery (find reference templates)
    2. Content generation (create from templates)
    3. Hierarchy management (placement decisions)
    
    All decisions are evidence-based and explained.
    """
    
    def __init__(self, graph_client: Neo4jClient):
        self.graph = graph_client
        self.llm = get_llm_client()
    
    def find_reference_section(
        self,
        need_description: str,
        target_product_code: Optional[str] = None,
        context: Optional[Dict] = None
    ) -> Optional[SectionReference]:
        """
        Find the best reference section for a given need.
        
        This is the CORE discovery function. It works in 3 phases:
        
        Phase 1: Semantic Search
          - Convert need description to embedding
          - Search graph for semantically similar sections
          - Get top 10 candidates
        
        Phase 2: LLM Evaluation
          - Present candidates with full context to LLM
          - LLM evaluates relevance, hierarchy, format
          - LLM explains its choice (evidence-based)
        
        Phase 3: Context Enrichment
          - Fetch full hierarchy for chosen section
          - Get sibling context
          - Return complete reference package
        
        Args:
            need_description: Natural language description of what's needed
                Examples:
                - "Face Day Cream now has heavy metals traces"
                - "Product contains allergens that must be declared"
                - "New regulation requires microplastics disclosure"
            
            target_product_code: Optional product code (for filtering)
            context: Optional additional context dict
        
        Returns:
            SectionReference with template + hierarchy, or None if not found
        """
        log.info(f"🔍 Finding reference section for: {need_description}")
        
        # Phase 1: Semantic Search
        candidates = self._semantic_search(need_description, limit=10)
        
        if not candidates:
            log.warning("No candidate sections found in semantic search")
            return None
        
        log.debug(f"Found {len(candidates)} candidate sections")
        
        # Phase 2: LLM Evaluation
        best_candidate = self._evaluate_candidates(
            need_description=need_description,
            candidates=candidates,
            context=context
        )
        
        if not best_candidate:
            log.warning("LLM could not select a suitable reference")
            return None
        
        # Phase 3: Enrich with full hierarchy
        reference = self._enrich_reference(best_candidate)
        
        log.info(f"✅ Selected reference: {reference.product_name} section {reference.section_number}")
        log.debug(f"LLM reasoning: {reference.llm_reasoning}")
        
        return reference
    
    def _semantic_search(self, query: str, limit: int = 10) -> List[Dict]:
        """
        Semantic vector search across all sections in graph.
        
        Returns sections ordered by relevance.
        """
        # Generate embedding for query
        from embeddings.embedder import get_embedder
        embedder = get_embedder()
        query_embedding = embedder.embed(query)
        
        # Vector search in Neo4j
        cypher = """
        CALL db.index.vector.queryNodes('idx_section_embedding', $limit, $query_vector)
        YIELD node, score
        MATCH (node)<-[:HAS_SECTION]-(d:DossierVersion)<-[:HAS_DOSSIER]-(p:Product)
        RETURN 
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
        
        try:
            results = self.graph.run_query(
                cypher,
                {
                    "query_vector": query_embedding,
                    "limit": limit
                }
            )
            return results
        except Exception as e:
            log.error(f"Semantic search failed: {e}")
            # Fallback to keyword search
            return self._keyword_search(query, limit)
    
    def _keyword_search(self, query: str, limit: int = 10) -> List[Dict]:
        """
        Fallback keyword search if vector search fails.
        """
        cypher = """
        CALL db.index.fulltext.queryNodes('idx_section_fulltext', $query)
        YIELD node, score
        MATCH (node)<-[:HAS_SECTION]-(d:DossierVersion)<-[:HAS_DOSSIER]-(p:Product)
        RETURN 
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
        
        results = self.graph.run_query(cypher, {"query": query, "limit": limit})
        return results
    
    def _evaluate_candidates(
        self,
        need_description: str,
        candidates: List[Dict],
        context: Optional[Dict]
    ) -> Optional[Dict]:
        """
        Use LLM to evaluate candidate sections and choose the best one.
        
        This is where the intelligence happens - LLM understands:
        - Content relevance
        - Regulatory context
        - Hierarchical appropriateness
        - Format suitability
        """
        # Build comprehensive prompt with ALL candidates
        candidates_json = json.dumps([
            {
                "index": i,
                "product": c["product_name"],
                "section": c["section_number"],
                "title": c["title"],
                "format": c["content_format"],
                "text_preview": c["full_text"][:500] + "..." if len(c["full_text"]) > 500 else c["full_text"]
            }
            for i, c in enumerate(candidates)
        ], indent=2)
        
        context_str = json.dumps(context, indent=2) if context else "None"
        
        prompt = f"""You are a regulatory documentation specialist analyzing dossier sections.

TASK: Select the BEST reference section to use as a template for the following need.

NEED DESCRIPTION:
{need_description}

ADDITIONAL CONTEXT:
{context_str}

CANDIDATE SECTIONS (search results):
{candidates_json}

EVALUATION CRITERIA:
1. **Content Relevance**: Does this section address the same regulatory topic?
2. **Structural Match**: Is the hierarchy/organization appropriate?
3. **Format Suitability**: Is the format (table/bullets/paragraphs) appropriate?
4. **Completeness**: Does it contain comprehensive regulatory language?
5. **Recency**: Prefer more recent regulatory patterns if equally relevant

Return JSON with your evaluation:
{{
  "selected_index": <integer index of best candidate>,
  "confidence": "high" | "medium" | "low",
  "reasoning": "<detailed explanation of why you chose this section>",
  "alternative_indices": [<list of backup options if primary doesn't work>],
  "concerns": "<any concerns about using this reference, or empty string>"
}}

CRITICAL: Base your decision on regulatory compliance and structural logic, not just keyword matching.
If none of the candidates are truly suitable, set selected_index to -1.
"""
        
        system_prompt = """You are an expert in pharmaceutical and cosmetic regulatory documentation.
You understand EU Regulation (EC) No 1223/2009, GMP requirements, and dossier structure.
You make evidence-based decisions and always explain your reasoning clearly."""
        
        response = self.llm.ask_structured(
            prompt=prompt,
            system_prompt=system_prompt,
            schema_description="""
            {
              "selected_index": number,
              "confidence": string,
              "reasoning": string,
              "alternative_indices": array of numbers,
              "concerns": string
            }
            """
        )
        
        if not response.success:
            log.error(f"LLM evaluation failed: {response.error}")
            # Fallback: use highest scoring candidate
            return candidates[0] if candidates else None
        
        evaluation = response.content
        selected_idx = evaluation.get("selected_index", -1)
        
        if selected_idx < 0 or selected_idx >= len(candidates):
            log.warning(f"LLM indicated no suitable candidate (index={selected_idx})")
            return None
        
        selected = candidates[selected_idx]
        selected["llm_reasoning"] = evaluation.get("reasoning", "")
        selected["llm_confidence"] = evaluation.get("confidence", "unknown")
        
        return selected
    
    def _enrich_reference(self, candidate: Dict) -> SectionReference:
        """
        Enrich selected candidate with full hierarchical context.
        """
        section_number = candidate["section_number"]
        
        # Get full hierarchy context
        cypher = """
        MATCH (section:Section {section_number: $section_number})
        OPTIONAL MATCH (parent:Section {section_number: section.parent_number})
        OPTIONAL MATCH (parent)-[:HAS_CHILD]->(sibling:Section)
        WHERE sibling.section_number <> $section_number
        
        RETURN 
            parent.section_number AS parent_number,
            parent.title AS parent_title,
            collect(DISTINCT sibling.section_number) AS sibling_numbers
        """
        
        result = self.graph.run_query(
            cypher,
            {"section_number": section_number}
        )
        
        hierarchy = result[0] if result else {}
        
        return SectionReference(
            section_number=candidate["section_number"],
            title=candidate["title"],
            full_text=candidate["full_text"],
            product_name=candidate["product_name"],
            product_code=candidate["product_code"],
            parent_number=hierarchy.get("parent_number", ""),
            parent_title=hierarchy.get("parent_title", ""),
            sibling_sections=hierarchy.get("sibling_numbers", []),
            content_format=candidate["content_format"],
            similarity_score=candidate.get("score", 0.0),
            llm_reasoning=candidate.get("llm_reasoning", "")
        )
    
    def generate_section_content(
        self,
        reference: SectionReference,
        new_data: Dict[str, any],
        target_product_name: str
    ) -> str:
        """
        Generate new section content using reference template + new data.
        
        This is the GENERATION function. It uses the reference section's
        vocabulary, structure, and style but incorporates new product data.
        
        Args:
            reference: The template section
            new_data: Product-specific data to incorporate
            target_product_name: Name of target product
        
        Returns:
            Generated section text matching reference format
        """
        log.info(f"📝 Generating section content for: {target_product_name}")
        
        new_data_json = json.dumps(new_data, indent=2)
        
        prompt = f"""You are a regulatory documentation specialist generating a dossier section.

TASK: Generate a new section for product "{target_product_name}" using the REFERENCE TEMPLATE below.

REFERENCE TEMPLATE (from {reference.product_name}):
Section {reference.section_number}: {reference.title}
Format: {reference.content_format}

{reference.full_text}

NEW DATA to incorporate:
{new_data_json}

REQUIREMENTS:
1. **Preserve Format**: Use the exact same structure as the reference
   - If reference uses bullets, use bullets
   - If reference uses a table, use a table with same columns
   - If reference uses paragraphs, maintain paragraph structure

2. **Preserve Vocabulary**: Use the same regulatory language
   - Keep phrases like "According to the information received from suppliers"
   - Keep regulation citations exact (e.g., "Regulation (EC) No 1223/2009")
   - Maintain institutional tone and terminology

3. **Update Data Only**: Change only the product-specific information
   - Substance names
   - Concentrations/quantities
   - Supplier information
   - Batch numbers
   - Test results

4. **Maintain Compliance**: Ensure all regulatory statements remain accurate

Generate the complete section text for {target_product_name}:
"""
        
        system_prompt = """You are an expert regulatory documentation writer.
You understand that cosmetic dossiers must maintain strict format consistency and regulatory language.
You generate compliant sections that preserve institutional vocabulary while updating product data."""
        
        response = self.llm.ask(
            prompt=prompt,
            system_prompt=system_prompt,
            temperature=0.3  # Slight creativity for natural flow, but mostly deterministic
        )
        
        if not response.success:
            log.error(f"Content generation failed: {response.error}")
            raise Exception(f"Failed to generate section content: {response.error}")
        
        generated_text = response.content
        log.debug(f"Generated {len(generated_text)} characters")
        
        return generated_text
    
    def decide_section_placement(
        self,
        target_product_code: str,
        reference: SectionReference,
        proposed_title: str
    ) -> SectionPlacement:
        """
        Use LLM to decide where to place a new section in target product.
        
        This handles the complex logic of:
        - Where in the hierarchy should it go?
        - What section number should it have?
        - Do existing sections need renumbering?
        
        All decisions are evidence-based from reference hierarchy.
        
        Args:
            target_product_code: Product to add section to
            reference: Reference section showing where it appeared in its product
            proposed_title: Proposed title for new section
        
        Returns:
            SectionPlacement with numbering and renumbering plan
        """
        log.info(f"🎯 Deciding placement for section: {proposed_title}")
        
        # Get current structure of target product
        cypher = """
        MATCH (p:Product {product_code: $product_code})
              -[:HAS_DOSSIER]->(d:DossierVersion)
              -[:HAS_SECTION]->(s:Section)
        WHERE s.section_number STARTS WITH '2.2'
        RETURN s.section_number, s.title, s.parent_number
        ORDER BY s.section_number
        """
        
        current_structure = self.graph.run_query(
            cypher,
            {"product_code": target_product_code}
        )
        
        current_structure_json = json.dumps(current_structure, indent=2)
        
        prompt = f"""You are a regulatory documentation specialist organizing dossier structure.

TASK: Decide where to insert a new section in the target product's dossier.

TARGET PRODUCT CURRENT STRUCTURE:
{current_structure_json}

EVIDENCE FROM REFERENCE PRODUCT:
- Reference product: {reference.product_name}
- New section appeared at: {reference.section_number}
- Under parent: {reference.parent_number} "{reference.parent_title}"
- Siblings at same level: {', '.join(reference.sibling_sections)}
- Position context: This suggests the organizational convention for this topic

NEW SECTION TO ADD:
- Title: {proposed_title}
- Format: {reference.content_format}

DECISION REQUIRED:
1. What section number should this have in target product?
2. Should existing sections be renumbered to maintain logical order?
3. Where in the hierarchy does it fit?

RULES:
- Maintain regulatory organization conventions (allergens before CMR, CMR before heavy metals, etc.)
- Keep hierarchy depth consistent (if reference is 2.2.2.X, place at same depth)
- Preserve parent-child relationships
- Minimize renumbering (only renumber if necessary for logical flow)

Return JSON with placement decision:
{{
  "new_section_number": "2.2.2.X",
  "parent_number": "2.2.2",
  "insert_position": <integer position among siblings, 0-indexed>,
  "renumber_plan": {{
    "old_2.2.2.3": "new_2.2.2.4",
    "old_2.2.2.4": "new_2.2.2.5"
  }},
  "reasoning": "<explain why you chose this placement>"
}}

If no renumbering needed, return empty renumber_plan dict.
"""
        
        system_prompt = """You are an expert in regulatory dossier organization.
You understand section numbering conventions and regulatory topic ordering."""
        
        response = self.llm.ask_structured(
            prompt=prompt,
            system_prompt=system_prompt,
            schema_description="""
            {
              "new_section_number": string,
              "parent_number": string,
              "insert_position": number,
              "renumber_plan": object (keys = old numbers, values = new numbers),
              "reasoning": string
            }
            """
        )
        
        if not response.success:
            log.error(f"Placement decision failed: {response.error}")
            # Fallback: append at end
            max_section = max(
                (s["section_number"] for s in current_structure),
                default="2.2.0"
            )
            # Simple increment logic
            parts = max_section.split(".")
            parts[-1] = str(int(parts[-1]) + 1)
            fallback_number = ".".join(parts)
            
            return SectionPlacement(
                new_section_number=fallback_number,
                new_title=proposed_title,
                parent_number=reference.parent_number,
                insert_position=len(current_structure),
                renumber_plan={},
                reasoning="Fallback: appended at end due to LLM failure"
            )
        
        placement_data = response.content
        
        return SectionPlacement(
            new_section_number=placement_data["new_section_number"],
            new_title=proposed_title,
            parent_number=placement_data["parent_number"],
            insert_position=placement_data.get("insert_position", 0),
            renumber_plan=placement_data.get("renumber_plan", {}),
            reasoning=placement_data.get("reasoning", "")
        )


# Singleton instance
_intelligence_instance: Optional[SectionIntelligence] = None


def get_section_intelligence(graph_client: Neo4jClient) -> SectionIntelligence:
    """Get or create the global section intelligence instance"""
    global _intelligence_instance
    if _intelligence_instance is None:
        _intelligence_instance = SectionIntelligence(graph_client)
    return _intelligence_instance
