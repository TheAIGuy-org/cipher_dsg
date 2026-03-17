"""
db/plan_builder.py
------------------
Phase 7: Section Update Plan Builder

Assembles complete SectionUpdatePlan objects from analysis results.
Combines pattern analysis + reference selection into final plans.

Output is ready for generation stage (Phase 9 - future).
"""
from typing import List, Dict, Optional
from parsers.models import SectionUpdatePlan, ConceptChangeOutput
from db.reference_finder import CrossDossierReferenceFinder, get_reference_finder
from graph.neo4j_client import Neo4jClient, client as neo4j_singleton
from utils.logger import get_logger

log = get_logger(__name__)


class UpdatePlanBuilder:
    """
    Assembles complete SectionUpdatePlan objects from analysis results.
    
    For each situation analysis:
    1. If SAME_PATTERN → use current section as reference
    2. If NEW_PATTERN → find cross-dossier reference
    3. Get hierarchy context
    4. Calculate overall confidence
    5. Return structured plan
    """
    
    def __init__(
        self,
        neo4j_client: Optional[Neo4jClient] = None,
        reference_finder: Optional[CrossDossierReferenceFinder] = None
    ):
        """
        Initialize plan builder.
        
        Args:
            neo4j_client: Neo4j client for current section queries
            reference_finder: Reference finder for cross-dossier search
        """
        self.neo4j = neo4j_client or neo4j_singleton
        self.reference_finder = reference_finder or get_reference_finder()
        
        log.info("UpdatePlanBuilder initialized")
    
    def build_plans(
        self,
        product_code: str,
        situation_analyses: List[Dict]
    ) -> List[SectionUpdatePlan]:
        """
        Create update plans from situation analysis results.
        
        Args:
            product_code: Product code being updated
            situation_analyses: Output from SectionSituationAnalyzer
        
        Returns:
            List of SectionUpdatePlan objects
        """
        log.info(f"Building update plans for {len(situation_analyses)} sections")
        
        plans = []
        for analysis in situation_analyses:
            try:
                plan = self._build_single_plan(product_code, analysis)
                plans.append(plan)
                
                log.debug(
                    f"  Plan: {plan.section_number} - "
                    f"{plan.pattern_change_type} - "
                    f"{plan.reference_source} - "
                    f"{plan.status}"
                )
                
            except Exception as e:
                log.error(
                    f"Failed to build plan for {analysis.get('section_id')}: {e}",
                    exc_info=True
                )
        
        log.info(f"Created {len(plans)} update plans")
        
        # Summary stats
        ready = sum(1 for p in plans if p.status == "READY_FOR_GENERATION")
        manual = sum(1 for p in plans if p.status == "PENDING_MANUAL_TEMPLATE")
        log.info(f"  {ready} ready for auto-generation, {manual} need manual template")
        
        return plans
    
    def _build_single_plan(
        self,
        product_code: str,
        analysis: Dict
    ) -> SectionUpdatePlan:
        """
        Build plan for one section.
        
        Args:
            product_code: Product code
            analysis: Situation analysis dict
        
        Returns:
            SectionUpdatePlan object
        """
        pattern_type = analysis['pattern_change_type']
        
        if pattern_type == 'SAME_PATTERN':
            # Use current section as reference
            log.debug(f"  SAME_PATTERN: Using current section as reference")
            reference_info = self._get_current_section_reference(analysis['section_id'])
            reference_source = "CURRENT_SECTION"
            plan_status = "READY_FOR_GENERATION"
            
        else:
            # NEW_PATTERN: Find cross-dossier reference with LLM selection
            log.debug(f"  NEW_PATTERN: Searching for cross-dossier reference")
            
            # Extract primary concept from first concept change
            primary_concept = analysis['related_concept_changes'][0].concept
            
            # Step 1: Get top-K candidates from RAG
            candidates = self.reference_finder.find_reference_section(
                target_product_code=product_code,
                concept=primary_concept,
                new_situation_description=analysis['new_semantic_description']
            )
            
            # Step 2: LLM selects best reference from candidates
            if candidates:
                # Get current section info for LLM context (if section exists)
                target_section_info = self._get_section_state_for_selection(
                    analysis['section_id']
                )
                
                # Build change description for LLM
                change_description = self._build_change_description(
                    analysis['related_concept_changes']
                )
                
                # LLM picks best reference
                reference_info = self.reference_finder.select_best_reference_with_llm(
                    candidates=candidates,
                    concept=primary_concept,
                    new_situation=analysis['new_semantic_description'],
                    change_description=change_description,
                    target_section_info=target_section_info
                )
            else:
                reference_info = None
            
            if not reference_info:
                # No reference found - mark for manual input at validation
                log.info(
                    f"  No suitable reference for {analysis['section_id']} - "
                    f"requires manual template at validation stage"
                )
                reference_info = {
                    'section_id': None,
                    'section_number': None,
                    'title': analysis.get('title', 'New Section'),
                    'full_text': '',
                    'content_format': 'unknown',
                    'product_code': None
                }
                reference_source = "NOT_FOUND"
                plan_status = "PENDING_MANUAL_TEMPLATE"
            else:
                reference_source = "CROSS_DOSSIER"
                plan_status = "READY_FOR_GENERATION"
        
        # Get hierarchy context
        hierarchy = self.reference_finder.get_section_hierarchy(
            reference_info.get('section_id') or analysis['section_id']
        )
        
        # Determine overall confidence
        overall_confidence = self._calculate_overall_confidence(
            analysis,
            reference_info
        )
        
        # Build plan object
        return SectionUpdatePlan(
            # Target section
            section_id=analysis['section_id'],
            section_number=analysis['section_number'],
            title=reference_info.get('title', analysis.get('title', 'Unknown')),
            product_code=product_code,
            dossier_id=reference_info.get('dossier_id', analysis.get('dossier_id', '')),
            
            # Status determines pipeline behavior
            status=plan_status,
            
            # Pattern analysis (Phase 5)
            pattern_change_type=pattern_type,
            pattern_reasoning=analysis['pattern_reasoning'],
            
            # Situation analysis
            old_semantic_description=analysis['old_semantic_description'],
            new_semantic_description=analysis['new_semantic_description'],
            
            # Reference template source (Phase 6)
            reference_source=reference_source,
            reference_section_id=reference_info.get('section_id'),
            reference_product_code=reference_info.get('product_code'),
            reference_section_number=reference_info.get('section_number'),
            reference_full_text=reference_info.get('full_text', ''),
            reference_content_format=reference_info.get('content_format', 'paragraphs'),
            
            # Hierarchy context
            parent_section_number=hierarchy.get('parent_number'),
            sibling_sections=hierarchy.get('siblings', []),
            
            # What changed
            concept_changes=analysis['related_concept_changes'],
            
            # Confidence
            overall_confidence=overall_confidence
        )
    
    def _get_section_state_for_selection(self, section_id: str) -> Optional[Dict]:
        """
        Get current section state info for LLM reference selection context.
        
        Provides LLM with info about target section (if exists):
        - Section number/title
        - Whether it has content or is empty
        - Current format
        
        Args:
            section_id: Section identifier
        
        Returns:
            Section state dict or None if section doesn't exist
        """
        query = """
        MATCH (s:Section {section_id: $section_id})
        RETURN s.section_number AS section_number,
               s.title AS title,
               s.full_text AS full_text,
               s.content_format AS content_format,
               size(coalesce(s.full_text, '')) AS content_length
        """
        
        try:
            result = self.neo4j.run_query(query, {"section_id": section_id})
            if result:
                section = result[0]
                return {
                    'section_number': section['section_number'],
                    'title': section['title'],
                    'has_content': section['content_length'] > 50,  # Meaningful content
                    'content_format': section['content_format'],
                    'content_length': section['content_length']
                }
            else:
                return None  # Section doesn't exist yet
        except Exception as e:
            log.error(f"Failed to get section state: {e}")
            return None
    
    def _build_change_description(
        self,
        concept_changes: List[ConceptChangeOutput]
    ) -> str:
        """
        Build human-readable change description for LLM.
        
        Args:
            concept_changes: List of concept changes
        
        Returns:
            Formatted change description
        """
        if not concept_changes:
            return "No specific changes"
        
        descriptions = []
        for change in concept_changes:
            desc = f"- {change.change_type}: {change.description}"
            if change.affected_entity:
                desc += f" (affects: {change.affected_entity})"
            desc += f" (confidence: {change.confidence})"
            descriptions.append(desc)
        
        return "\n".join(descriptions)
    
    def _get_current_section_reference(self, section_id: str) -> Dict:
        """
        Get current section info as reference.
        
        Args:
            section_id: Section identifier
        
        Returns:
            Section info dict
        """
        query = """
        MATCH (s:Section {section_id: $section_id})
        MATCH (s)<-[:HAS_SECTION]-(d:DossierVersion)
        MATCH (d)<-[:HAS_DOSSIER]-(p:Product)
        RETURN s.section_id AS section_id,
               s.section_number AS section_number,
               s.title AS title,
               s.full_text AS full_text,
               s.content_format AS content_format,
               s.parent_number AS parent_number,
               p.product_code AS product_code,
               d.dossier_id AS dossier_id
        """
        
        try:
            result = self.neo4j.run_query(query, {"section_id": section_id})
            if result:
                return result[0]
            else:
                log.warning(f"Current section not found: {section_id}")
                return {}
        except Exception as e:
            log.error(f"Failed to get current section reference: {e}")
            return {}
    
    @staticmethod
    def _calculate_overall_confidence(
        analysis: Dict,
        reference_info: Dict
    ) -> str:
        """
        Determine overall confidence level.
        
        Args:
            analysis: Situation analysis
            reference_info: Reference section info
        
        Returns:
            Confidence level: 'high', 'medium', or 'low'
        """
        # Pattern analysis confidence
        pattern_conf = analysis.get('confidence', 'medium')
        
        # Reference quality
        if reference_info and reference_info.get('section_id'):
            reference_score = reference_info.get('similarity_score', 0.5)
        else:
            reference_score = 0.0  # No reference
        
        # Simple heuristic
        if pattern_conf == 'high' and reference_score > 0.8:
            return 'high'
        elif pattern_conf == 'low' or reference_score < 0.3:
            return 'low'
        else:
            return 'medium'


# Singleton
_builder_instance: Optional[UpdatePlanBuilder] = None


def get_plan_builder(
    neo4j_client: Optional[Neo4jClient] = None,
    reference_finder: Optional[CrossDossierReferenceFinder] = None
) -> UpdatePlanBuilder:
    """
    Get singleton UpdatePlanBuilder instance.
    
    Args:
        neo4j_client: Optional Neo4j client
        reference_finder: Optional reference finder
    
    Returns:
        UpdatePlanBuilder instance
    """
    global _builder_instance
    
    if _builder_instance is None:
        _builder_instance = UpdatePlanBuilder(
            neo4j_client=neo4j_client,
            reference_finder=reference_finder
        )
    
    return _builder_instance
