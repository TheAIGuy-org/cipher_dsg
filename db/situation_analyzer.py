"""
db/situation_analyzer.py
------------------------
Phase 5: Section Situation Analyzer

Determines whether format changes are needed (SAME_PATTERN vs NEW_PATTERN).
Uses LLM reasoning with reference evidence - no confidence guessing.

This is the intelligence layer that detects:
- When current format can accommodate new data (SAME_PATTERN)
- When a different format is needed (NEW_PATTERN - triggers cross-dossier search)
"""
from typing import List, Dict, Optional
from parsers.models import ImpactedSection, PatternDecisionOutput, NewSituationOutput
from prompts import get_prompt
from llm.azure_client import AzureLLMClient, get_llm_client
from embeddings.embedder import EmbedderProtocol, get_embedder
from graph.neo4j_client import Neo4jClient, client as neo4j_singleton
from utils.logger import get_logger

log = get_logger(__name__)


class SectionSituationAnalyzer:
    """
    Analyzes section situations to determine if format changes are needed.
    
    Core intelligence:
    1. Infers new situation after changes (LLM)
    2. Compares old vs new semantic profiles
    3. Gets reference format evidence from graph
    4. LLM decides: SAME_PATTERN (extend current) or NEW_PATTERN (use different format)
    
    Evidence-based decisions - not guessing!
    """
    
    def __init__(
        self,
        llm: Optional[AzureLLMClient] = None,
        embedder: Optional[EmbedderProtocol] = None,
        neo4j_client: Optional[Neo4jClient] = None
    ):
        """
        Initialize situation analyzer.
        
        Args:
            llm: Azure LLM client for reasoning
            embedder: Embedder for creating situation vectors
            neo4j_client: Neo4j client for reference queries
        """
        self.llm = llm or get_llm_client()
        self.embedder = embedder or get_embedder()
        self.neo4j = neo4j_client or neo4j_singleton
        
        log.info("SectionSituationAnalyzer initialized")
    
    def analyze_situations(
        self,
        impacted_sections: List[ImpactedSection]
    ) -> List[Dict]:
        """
        Analyze situation changes for all impacted sections.
        
        For each section:
        1. Infer new situation after changes
        2. Compare to current situation
        3. Get reference format evidence
        4. Determine pattern change (SAME vs NEW)
        
        Args:
            impacted_sections: Sections affected by concept changes
        
        Returns:
            List of analysis dicts with pattern decisions
        """
        log.info(f"Analyzing situations for {len(impacted_sections)} sections")
        
        analyses = []
        for section in impacted_sections:
            try:
                analysis = self._analyze_single_section(section)
                analyses.append(analysis)
                
                log.debug(
                    f"  Section {section.section_number}: "
                    f"{analysis['pattern_change_type']} "
                    f"(confidence: {analysis.get('confidence', 'N/A')})"
                )
                
            except Exception as e:
                log.error(f"Failed to analyze {section.section_id}: {e}", exc_info=True)
        
        log.info(f"Completed {len(analyses)} situation analyses")
        return analyses
    
    def _analyze_single_section(self, section: ImpactedSection) -> Dict:
        """
        Analyze one section's situation change.
        
        Returns dict with:
        - section_id, section_number, title
        - old_semantic_description, new_semantic_description
        - pattern_change_type, pattern_reasoning
        - related_concept_changes
        - confidence
        """
        log.debug(f"Analyzing section: {section.section_number} - {section.title}")
        
        # Step 1: Infer new situation
        new_situation = self._infer_new_situation(
            current_description=section.current_semantic_description,
            concept_changes=section.related_concept_changes
        )
        
        log.debug(f"  New situation inferred: {new_situation[:100]}...")
        
        # Step 2: Get reference format evidence
        reference_evidence = self._get_reference_format_evidence(
            section_id=section.section_id,
            product_code=section.product_code,
            domain_concepts=section.current_domain_concepts
        )
        
        # Step 3: Determine pattern change
        pattern_decision = self._determine_pattern_change(
            old_description=section.current_semantic_description,
            new_description=new_situation,
            reference_evidence=reference_evidence
        )
        
        log.debug(
            f"  Pattern decision: {pattern_decision.pattern_change} - "
            f"{pattern_decision.reasoning[:80]}..."
        )
        
        return {
            'section_id': section.section_id,
            'section_number': section.section_number,
            'title': section.title,
            'product_code': section.product_code,
            'dossier_id': section.dossier_id,
            'old_semantic_description': section.current_semantic_description,
            'new_semantic_description': new_situation,
            'pattern_change_type': pattern_decision.pattern_change,
            'pattern_reasoning': pattern_decision.reasoning,
            'confidence': 'high',  # Pattern decisions are evidence-based, not probabilistic
            'related_concept_changes': section.related_concept_changes
        }
    
    def _infer_new_situation(
        self,
        current_description: str,
        concept_changes: List
    ) -> str:
        """
        Use LLM to infer what the new situation will be after changes.
        
        Args:
            current_description: Current semantic description
            concept_changes: List of ConceptChangeOutput objects
        
        Returns:
            New situation description (2-3 sentences)
        """
        # Build changes text
        changes_text = "\n".join([
            f"- {cc.change_type}: {cc.description}"
            for cc in concept_changes
        ])
        
        prompt = f"""A dossier section currently has this situation:
{current_description}

The following changes will be applied:
{changes_text}

Describe the NEW situation after these changes in 2-3 sentences.
Focus on:
- What information the section will now contain
- How many items/entities will be present
- The expected format/structure

Return JSON: {{"new_situation": "..."}}
"""
        
        response = self.llm.ask_structured_pydantic(
            system_prompt="You are analyzing regulatory document changes. Describe the new state accurately.",
            user_prompt=prompt,
            response_model=NewSituationOutput,
            temperature=0.2
        )
        
        return response.new_situation
    
    def _get_reference_format_evidence(
        self,
        section_id: str,
        product_code: str,
        domain_concepts: List[str]
    ) -> str:
        """
        Get format examples from similar sections in other products.
        Used to make evidence-based pattern decisions.
        
        Args:
            section_id: Current section ID
            product_code: Current product code
            domain_concepts: Section's domain concepts
        
        Returns:
            Formatted evidence string for LLM
        """
        if not domain_concepts:
            return "No reference sections available (no domain concepts tagged)"
        
        try:
            # Query for similar sections in OTHER products
            query = """
            MATCH (s:Section)
            WHERE s.product_code <> $product_code
              AND any(concept IN s.domain_concepts WHERE concept IN $concepts)
            RETURN s.section_number AS section_number,
                   s.title AS title,
                   s.content_format AS format_style,
                   s.semantic_characteristics AS characteristics,
                   s.product_code AS product_code
            LIMIT 5
            """
            
            results = self.neo4j.run_query(
                query,
                {
                    'product_code': product_code,
                    'concepts': domain_concepts
                }
            )
            
            if not results:
                return "No reference sections found in other products"
            
            # Format evidence for LLM
            evidence_parts = []
            for r in results:
                import json
                try:
                    chars = json.loads(r.get('characteristics', '{}'))
                    item_count = chars.get('item_count', 'unknown')
                except:
                    item_count = 'unknown'
                
                evidence_parts.append(
                    f"Product {r['product_code']}, Section {r['section_number']}: "
                    f"{r['title']} - {r['format_style']} format "
                    f"(~{item_count} items)"
                )
            
            return "Reference sections:\n" + "\n".join(evidence_parts)
            
        except Exception as e:
            log.warning(f"Failed to get reference evidence: {e}")
            return "Reference query failed - proceeding without evidence"
    
    def _determine_pattern_change(
        self,
        old_description: str,
        new_description: str,
        reference_evidence: str
    ) -> PatternDecisionOutput:
        """
        Use LLM to determine if pattern changed.
        EVIDENCE-BASED: LLM looks at actual reference formats, not confidence guessing.
        
        Args:
            old_description: Current situation
            new_description: New situation after changes
            reference_evidence: Format evidence from other products
        
        Returns:
            PatternDecisionOutput with decision and reasoning
        """
        # Get prompts from centralized store
        system_prompt = get_prompt('pattern_analysis', 'system')
        user_prompt_template = get_prompt('pattern_analysis', 'user')
        
        # Format user prompt
        user_prompt = user_prompt_template.format(
            old_situation_description=old_description,
            new_situation_description=new_description,
            reference_evidence=reference_evidence
        )
        
        # Call LLM with structured output
        try:
            response = self.llm.ask_structured_pydantic(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=PatternDecisionOutput,
                temperature=0.1  # Deterministic
            )
            
            return response
            
        except Exception as e:
            log.error(f"Pattern decision LLM call failed: {e}")
            # Conservative fallback: assume SAME_PATTERN (less disruptive)
            return PatternDecisionOutput(
                pattern_change="SAME_PATTERN",
                reasoning=f"Defaulting to SAME_PATTERN due to LLM error: {str(e)}",
                evidence_used="None (error fallback)"
            )


# Singleton
_analyzer_instance: Optional[SectionSituationAnalyzer] = None


def get_situation_analyzer(
    llm: Optional[AzureLLMClient] = None,
    embedder: Optional[EmbedderProtocol] = None,
    neo4j_client: Optional[Neo4jClient] = None
) -> SectionSituationAnalyzer:
    """
    Get singleton SectionSituationAnalyzer instance.
    
    Args:
        llm: Optional LLM client
        embedder: Optional embedder
        neo4j_client: Optional Neo4j client
    
    Returns:
        SectionSituationAnalyzer instance
    """
    global _analyzer_instance
    
    if _analyzer_instance is None:
        _analyzer_instance = SectionSituationAnalyzer(
            llm=llm,
            embedder=embedder,
            neo4j_client=neo4j_client
        )
    
    return _analyzer_instance
