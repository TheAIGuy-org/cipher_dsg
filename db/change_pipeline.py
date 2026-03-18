"""
db/change_pipeline.py
---------------------
Phase 8: Complete Change Detection Pipeline

End-to-end orchestration of Phases 2-7.
Wire all components together into cohesive flow.

This implements the EXACT architecture from IMPLEMENTATION_PLAN_DETAILED.md.
"""
from typing import List, Dict, Optional
from parsers.models import ChangeBundle, SectionUpdatePlan, ImpactedSection, ConceptChangeOutput
from db.sql_client import SQLServerClient, get_sql_client
from llm.change_interpreter import ChangeInterpreter, get_change_interpreter
from llm.section_mapper import SectionMapper, get_section_mapper
from llm.section_intelligence import SectionIntelligence, SectionReference, SectionPlacement
from graph.neo4j_client import Neo4jClient, client as neo4j_singleton
from db.situation_analyzer import SectionSituationAnalyzer, get_situation_analyzer
from db.reference_finder import CrossDossierReferenceFinder, get_reference_finder
from db.plan_builder import UpdatePlanBuilder, get_plan_builder
from utils.logger import get_logger

log = get_logger(__name__)


class ChangeDetectionPipeline:
    """
    End-to-end orchestration of DB change detection pipeline.
    
    Implements complete flow as designed:
    Phase 2: DB Change Detection (via poller)
    Phase 3: Concept Extraction
    Phase 4: Section Mapping
    Phase 5: Situation Analysis (SAME vs NEW pattern)
    Phase 6: Reference Finding (cross-dossier search)
    Phase 7: Plan Building
    
    Output: List[SectionUpdatePlan] ready for generation
    """
    
    def __init__(
        self,
        sql_client: Optional[SQLServerClient] = None,
        concept_extractor: Optional[ChangeInterpreter] = None,
        section_mapper: Optional[SectionMapper] = None,
        situation_analyzer: Optional[SectionSituationAnalyzer] = None,
        reference_finder: Optional[CrossDossierReferenceFinder] = None,
        plan_builder: Optional[UpdatePlanBuilder] = None,
        section_intelligence: Optional[SectionIntelligence] = None
    ):
        """
        Initialize pipeline with all phase components.
        
        Args:
            sql_client: SQL Server client
            concept_extractor: Phase 3 component
            section_mapper: Phase 4 component
            situation_analyzer: Phase 5 component
            reference_finder: Phase 6 component
            plan_builder: Phase 7 component
            section_intelligence: Hierarchical placement intelligence
        """
        self.sql_client = sql_client or get_sql_client()
        self.concept_extractor = concept_extractor or get_change_interpreter()
        self.section_mapper = section_mapper or get_section_mapper()
        self.situation_analyzer = situation_analyzer or get_situation_analyzer()
        self.reference_finder = reference_finder or get_reference_finder()
        self.plan_builder = plan_builder or get_plan_builder()
        self.section_intelligence = section_intelligence or SectionIntelligence(neo4j_singleton)
        
        log.info("ChangeDetectionPipeline initialized (Phases 2-7)")
    
    def process_change_bundle(
        self,
        change_bundle: ChangeBundle
    ) -> List[SectionUpdatePlan]:
        """
        Process one change bundle through full pipeline.
        
        Workflow:
        1. Phase 3: Extract concepts from raw DB changes
        2. Phase 4: Map concepts to affected sections  
        3. Phase 5: Analyze situations (SAME vs NEW pattern)
        4. Phase 6: Find references (implicit in plan builder)
        5. Phase 7: Build complete update plans
        
        Args:
            change_bundle: Grouped DB changes for one product
        
        Returns:
            List of SectionUpdatePlan ready for generation
        """
        product_code = change_bundle.product_code
        change_count = change_bundle.get_change_count()
        
        log.info("=" * 80)
        log.info(f"PROCESSING CHANGE BUNDLE: {product_code} ({change_count} changes)")
        log.info("=" * 80)
        
        try:
            # PHASE 3: Concept Extraction
            log.info("\n🔍 PHASE 3: Extracting Concepts...")
            concept_changes = self.concept_extractor.interpret_bundle(
                bundle=change_bundle,
                include_related_context=True
            )
            
            if not concept_changes:
                log.warning("  ⚠️  No concept changes extracted")
                return []
            
            log.info(f"  ✅ Extracted {len(concept_changes)} concept changes:")
            for cc in concept_changes:
                log.info(f"     - {cc.concept}: {cc.change_type}")
            
            # PHASE 4: Section Mapping
            log.info("\n🎯 PHASE 4: Mapping to Sections...")
            
            # Collect impacted sections - handle both EXISTING and NEW sections differently
            impacted_sections_map = {}
            new_section_suggestions_map = {}
            
            for concept in concept_changes:
                impacts = self.section_mapper.map_concept_to_sections(
                    concept=concept,
                    product_code=product_code
                )
                
                # Separate CREATE sections from existing sections
                for impact in impacts:
                    if impact.update_type.value == 'create':
                        # This is a NEW section that doesn't exist yet
                        new_key = impact.section_id
                        if new_key not in new_section_suggestions_map:
                            log.info(f"  🆕 Detected NEW section needed: {impact.section_id} - {impact.section_title}")
                            new_section_suggestions_map[new_key] = {
                                'section_number': impact.section_id,
                                'title': impact.section_title,
                                'concepts': [concept],
                                'rationales': [impact.rationale],
                                'priority': impact.priority
                            }
                        else:
                            new_section_suggestions_map[new_key]['concepts'].append(concept)
                            new_section_suggestions_map[new_key]['rationales'].append(impact.rationale)
                    else:
                        # Existing section - get details from Neo4j
                        sec_num = impact.section_id
                        if sec_num in impacted_sections_map:
                            impacted_sections_map[sec_num].related_concept_changes.append(concept)
                        else:
                            from graph.neo4j_client import client as neo4j_client
                            section_query = """
                            MATCH (s:Section {section_number: $section_number, product_code: $product_code})
                            OPTIONAL MATCH (s)<-[:HAS_SECTION]-(d:DossierVersion)
                            RETURN s.section_id AS section_id,
                                   d.dossier_id AS dossier_id,
                                   s.semantic_description AS semantic_description,
                                   s.semantic_embedding AS semantic_embedding,
                                   s.domain_concepts AS domain_concepts
                            """
                            section_data = neo4j_client.run_query(
                                section_query,
                                {
                                    'section_number': impact.section_id,
                                    'product_code': product_code
                                }
                            )
                            
                            if section_data:
                                sd = section_data[0]
                                impacted_section = ImpactedSection(
                                    section_id=sd['section_id'],
                                    section_number=impact.section_id,
                                    title=impact.section_title,
                                    dossier_id=sd.get('dossier_id', ''),
                                    product_code=product_code,
                                    current_semantic_description=sd.get('semantic_description', ''),
                                    current_semantic_embedding=sd.get('semantic_embedding', []),
                                    current_domain_concepts=sd.get('domain_concepts', []),
                                    related_concept_changes=[concept],
                                    mapping_confidence=impact.relevance_score
                                )
                                impacted_sections_map[sec_num] = impacted_section
            
            # Convert maps to consolidated lists
            impacted_sections_list = list(impacted_sections_map.values())
            new_section_suggestions = list(new_section_suggestions_map.values())

            # Handle new section suggestions - these bypass pattern analysis
            update_plans = []
            
            if new_section_suggestions:
                log.info(f"\n🆕 Processing {len(new_section_suggestions)} NEW section suggestions...")
                for new_sec in new_section_suggestions:
                    log.info(f"  Creating plan for NEW section: {new_sec['section_number']}")
                    
                    # CRITICAL: Don't blindly use reference section number!
                    # Need to determine WHERE this section fits in target product's hierarchy
                    
                    # Step 1: Find candidate reference sections
                    # Use aggregated conceptual values
                    combined_description = " AND ".join([c.description for c in new_sec['concepts']])
                    primary_concept = new_sec['concepts'][0]
                    combined_rationale = " | ".join(new_sec['rationales'])
                    
                    reference_candidates = self.reference_finder.find_reference_section(
                        target_product_code=product_code,
                        concept=primary_concept.concept,
                        new_situation_description=combined_description
                    )
                    
                    if reference_candidates:
                        # Step 1b: Use LLM to select the best reference from candidates
                        change_description = self.plan_builder._build_change_description(new_sec['concepts'])
                        reference_info = self.reference_finder.select_best_reference_with_llm(
                            candidates=reference_candidates,
                            concept=primary_concept.concept,
                            new_situation=combined_description,
                            change_description=change_description,
                            target_section_info=None
                        )
                    else:
                        reference_info = None
                    
                    if reference_info:
                        log.info(f"    ✅ Found reference: {reference_info['product_code']} Section {reference_info['section_number']}")
                        
                        # Step 2: Determine correct section number for TARGET product's hierarchy
                        placement = self._determine_section_placement(
                            target_product_code=product_code,
                            reference_section_number=reference_info['section_number'],
                            reference_title=reference_info['title'],
                            concept=primary_concept
                        )
                        target_section_number = placement.new_section_number
                        
                        log.info(f"    📍 Placement: Section {target_section_number} in {product_code}'s hierarchy")
                        
                        plan = SectionUpdatePlan(
                            section_id=f"{product_code}__section__{target_section_number}",
                            section_number=target_section_number,  # Use calculated placement!
                            title=reference_info['title'],  # Keep reference title
                            product_code=product_code,
                            dossier_id='',
                            status="READY_FOR_GENERATION",
                            pattern_change_type="NEW_PATTERN",
                            pattern_reasoning=f"New section required at {target_section_number}: {combined_rationale}. Using {reference_info['product_code']} section {reference_info['section_number']} as template.",
                            old_semantic_description="N/A - section does not exist yet",
                            new_semantic_description=combined_description,
                            reference_source="CROSS_DOSSIER",
                            reference_section_id=reference_info.get('section_id'),
                            reference_product_code=reference_info.get('product_code'),
                            reference_section_number=reference_info.get('section_number'),
                            reference_full_text=reference_info.get('full_text', ''),
                            reference_content_format=reference_info.get('content_format', 'paragraphs'),
                            parent_section_number=placement.parent_number,
                            sibling_sections=[{"old": k, "new": v} for k, v in placement.renumber_plan.items()] if placement.renumber_plan else [],
                            concept_changes=new_sec['concepts'],
                            overall_confidence="high"
                        )
                        update_plans.append(plan)
                    else:
                        log.warning(f"    ⚠️  No reference found for new section - manual template needed")
                        plan = SectionUpdatePlan(
                            section_id=f"{product_code}__section__{new_sec['section_number']}",
                            section_number=new_sec['section_number'],
                            title=new_sec['title'],
                            product_code=product_code,
                            dossier_id='',
                            status="PENDING_MANUAL_TEMPLATE",
                            pattern_change_type="NEW_PATTERN",
                            pattern_reasoning=f"New section required but no reference found: {combined_rationale}",
                            old_semantic_description="N/A - section does not exist yet",
                            new_semantic_description=combined_description,
                            reference_source="NOT_FOUND",
                            reference_section_id=None,
                            reference_product_code=None,
                            reference_section_number=None,
                            reference_full_text='',
                            reference_content_format='unknown',
                            parent_section_number=None,
                            sibling_sections=[],
                            concept_changes=new_sec['concepts'],
                            overall_confidence="low"
                        )
                        update_plans.append(plan)
            
            # Process existing sections through normal pattern analysis pipeline
            if not impacted_sections_list and not new_section_suggestions:
                log.warning("  ⚠️  No sections impacted by changes")
                return []
            
            if impacted_sections_list:
                log.info(f"  ✅ Mapped to {len(impacted_sections_list)} EXISTING sections:")
                for sec in impacted_sections_list:
                    log.info(f"     - {sec.section_number}: {sec.title}")
            
            # PHASE 5: Situation Analysis (only for existing sections)
            if impacted_sections_list:
                log.info("\n🧠 PHASE 5: Analyzing Situations for EXISTING sections...")
                situation_analyses = self.situation_analyzer.analyze_situations(
                    impacted_sections=impacted_sections_list
                )
                
                if not situation_analyses:
                    log.warning("  ⚠️  Situation analysis failed")
                else:
                    log.info(f"  ✅ Analyzed {len(situation_analyses)} situations:")
                    for analysis in situation_analyses:
                        log.info(
                            f"     - {analysis['section_number']}: "
                            f"{analysis['pattern_change_type']}"
                        )
                    
                    # PHASE 6 & 7: Plan Building for existing sections
                    log.info("\n📋 PHASE 6-7: Building Update Plans for EXISTING sections...")
                    existing_plans = self.plan_builder.build_plans(
                        product_code=product_code,
                        situation_analyses=situation_analyses
                    )
                    
                    if existing_plans:
                        log.info(f"  ✅ Built {len(existing_plans)} plans for existing sections")
                        for plan in existing_plans:
                            log.info(
                                f"     - {plan.section_number}: "
                                f"{plan.pattern_change_type} / "
                                f"{plan.reference_source} / "
                                f"{plan.status}"
                            )
                        update_plans.extend(existing_plans)
            
            # Combine all plans
            if not update_plans:
                log.warning("  ⚠️  No update plans generated")
                return []
            
            # CONSOLIDATE if we have NEW_PATTERN sections needing hierarchy placement
            # Even a SINGLE new section may need renumbering of existing sections
            new_pattern_plans = [p for p in update_plans if p.pattern_change_type == "NEW_PATTERN"]
            
            if len(new_pattern_plans) >= 1:
                # New section(s) detected - need to check if hierarchy placement required
                consolidated_plan = self._consolidate_plans(update_plans, product_code, concept_changes)
                
                if consolidated_plan:
                    log.info(f"\n🎯 CONSOLIDATED INTO PLAN WITH HIERARCHY PLACEMENT:")
                    log.info(f"   Section: {consolidated_plan.section_number} - {consolidated_plan.title}")
                    if hasattr(consolidated_plan, '__dict__') and 'renumbering_required' in consolidated_plan.__dict__:
                        renumbering = consolidated_plan.__dict__['renumbering_required']
                        if renumbering:
                            log.info(f"   Renumbering: {renumbering}")
                    log.info(f"   Status: {consolidated_plan.status}")
                    
                    # Return both consolidated new section AND any SAME_PATTERN updates
                    result_plans = [consolidated_plan]
                    same_pattern_plans = [p for p in update_plans if p.pattern_change_type == "SAME_PATTERN"]
                    result_plans.extend(same_pattern_plans)
                    
                    return result_plans
            
            # Return individual plans (no new sections needing hierarchy placement)
            log.info(f"\n✅ Generated {len(update_plans)} independent update plans:")
            for plan in update_plans:
                log.info(
                    f"     - {plan.section_number}: "
                    f"{plan.pattern_change_type} / "
                    f"{plan.reference_source} / "
                    f"{plan.status}"
                )
            
            # Summary
            log.info("\n" + "=" * 80)
            log.info("PIPELINE COMPLETE")
            log.info("=" * 80)
            
            ready = sum(1 for p in update_plans if p.status == "READY_FOR_GENERATION")
            manual = sum(1 for p in update_plans if p.status == "PENDING_MANUAL_TEMPLATE")
            new_sections = sum(1 for p in update_plans if p.pattern_change_type == "NEW_PATTERN")
            
            log.info(f"Generated {len(update_plans)} plans:")
            log.info(f"  • {ready} ready for auto-generation")
            log.info(f"  • {manual} require manual template")
            log.info(f"  • {new_sections} are NEW sections")
            log.info("=" * 80 + "\n")
            
            return update_plans
            
        except Exception as e:
            log.error(f"Pipeline failed: {e}", exc_info=True)
            raise
    
    def _determine_section_placement(
        self,
        target_product_code: str,
        reference_section_number: str,
        reference_title: str,
        concept: 'ConceptChangeOutput'
    ) -> SectionPlacement:
        """
        Determine WHERE in the target product's hierarchy this new section should go.
        
        CRITICAL: The reference section shows WHAT content/format to use, but NOT
        where to place it in the target. Different products have different hierarchies!
        
        Args:
            target_product_code: Product code to add section to
            reference_section_number: Section number from reference product (e.g., "2.2.4")
            reference_title: Title of reference section
            concept: ConceptChangeOutput describing the new content
        
        Returns:
            Calculated section number for target product's hierarchy
        """
        log.info(f"🔢 Determining placement for '{reference_title}' in {target_product_code}")
        log.info(f"   Reference was {reference_section_number}, but target may differ")
        
        # Create SectionReference for section_intelligence
        reference = SectionReference(
            section_number=reference_section_number,
            title=reference_title,
            full_text="",  # Not needed for placement decision
            product_name="",  # Not needed
            product_code="",  # Not needed
            parent_number=self._get_parent_number(reference_section_number),
            parent_title="",  # Not needed
            sibling_sections=[],  # Will be inferred
            content_format="paragraphs",  # Not critical for placement
            similarity_score=0.0,  # Not needed
            llm_reasoning=concept.description
        )
        
        # Use SectionIntelligence to decide hierarchical placement
        placement = self.section_intelligence.decide_section_placement(
            target_product_code=target_product_code,
            reference=reference,
            proposed_title=reference_title
        )
        
        log.info(f"   ✅ Placement decision: {placement.new_section_number}")
        log.info(f"   📝 Reasoning: {placement.reasoning}")
        
        if placement.renumber_plan:
            log.warning(f"   ⚠️ Requires renumbering: {placement.renumber_plan}")
        
        return placement
    
    def _get_parent_number(self, section_number: str) -> str:
        """
        Extract parent section number.
        Example: "2.2.3.1" -> "2.2.3"
        """
        parts = section_number.split('.')
        if len(parts) > 1:
            return '.'.join(parts[:-1])
        return ""
    
    def _consolidate_plans(
        self,
        plans: List[SectionUpdatePlan],
        product_code: str,
        concept_changes: List[ConceptChangeOutput]
    ) -> Optional[SectionUpdatePlan]:
        """
        Consolidate multiple plans into ONE unified plan with correct hierarchy.
        
        This handles the case where:
        - A new section needs to be created (e.g., 2.2.2.2 Heavy Metals)
        - Existing sections need to be renumbered (e.g., 2.2.2.2 CMR → 2.2.2.3)
        - Multiple sections are affected
        
        Returns a single plan with the PRIMARY new section and renumbering instructions.
        
        Args:
            plans: List of generated plans
            product_code: Target product
            concept_changes: Original concept changes
        
        Returns:
            Single consolidated SectionUpdatePlan or None
        """
        if not plans:
            return None
        
        # Identify new sections (completely new, not just new format)
        new_section_plans = [
            p for p in plans 
            if p.pattern_change_type == "NEW_PATTERN" 
            and "does not exist yet" in str(p.old_semantic_description)
        ]
        
        if not new_section_plans:
            # No new sections - just return best existing plan
            return plans[0] if plans else None
        
        # Take the first new section plan as PRIMARY
        primary_plan = new_section_plans[0]
        
        log.info(f"\n🔧 CONSOLIDATING {len(plans)} plans into 1 unified output...")
        log.info(f"   Primary new section: {primary_plan.section_number} - {primary_plan.title}")
        
        # Trust the LLM's dynamic placement logic entirely - no hardcoded rules
        renumbering_map = {}
        if hasattr(primary_plan, 'sibling_sections') and isinstance(primary_plan.sibling_sections, list):
            # Only extract dicts that have 'old' and 'new' keys (from placement logic)
            renumbering_map = {sib['old']: sib['new'] for sib in primary_plan.sibling_sections if isinstance(sib, dict) and 'old' in sib and 'new' in sib}
        
        if renumbering_map:
            log.info(f"   Renumbering required (decided by LLM):")
            for old, new in renumbering_map.items():
                log.info(f"     {old} → {new}")
        
        # Create consolidated plan based entirely on primary_plan logic
        consolidated = SectionUpdatePlan(
            section_id=primary_plan.section_id,
            section_number=primary_plan.section_number,
            title=primary_plan.title,
            product_code=product_code,
            dossier_id=primary_plan.dossier_id,
            status=primary_plan.status,
            pattern_change_type=primary_plan.pattern_change_type,
            pattern_reasoning=f"NEW SECTION at {primary_plan.section_number}: {primary_plan.title}. " +
                            f"Using {primary_plan.reference_product_code} section {primary_plan.reference_section_number} as template. " +
                            (f"Requires renumbering: {renumbering_map}" if renumbering_map else "No renumbering needed."),
            old_semantic_description=primary_plan.old_semantic_description,
            new_semantic_description=primary_plan.new_semantic_description,
            reference_source=primary_plan.reference_source,
            reference_section_id=primary_plan.reference_section_id,
            reference_product_code=primary_plan.reference_product_code,
            reference_section_number=primary_plan.reference_section_number,
            reference_full_text=primary_plan.reference_full_text,
            reference_content_format=primary_plan.reference_content_format,
            parent_section_number=primary_plan.parent_section_number,
            sibling_sections=primary_plan.sibling_sections,
            concept_changes=concept_changes,  # Include ALL concept changes
            overall_confidence=primary_plan.overall_confidence
        )
        
        # Add renumbering info as custom attribute (not in Pydantic model, but accessible)
        consolidated.__dict__['renumbering_required'] = renumbering_map if renumbering_map else None
        
        return consolidated


# Singleton
_pipeline_instance: Optional[ChangeDetectionPipeline] = None


def get_change_pipeline(
    sql_client: Optional[SQLServerClient] = None,
    concept_extractor: Optional[ChangeInterpreter] = None,
    section_mapper: Optional[SectionMapper] = None,
    situation_analyzer: Optional[SectionSituationAnalyzer] = None,
    reference_finder: Optional[CrossDossierReferenceFinder] = None,
    plan_builder: Optional[UpdatePlanBuilder] = None
) -> ChangeDetectionPipeline:
    """
    Get singleton ChangeDetectionPipeline instance.
    
    Args:
        All component dependencies (optional, will use singletons)
    
    Returns:
        ChangeDetectionPipeline instance
    """
    global _pipeline_instance
    
    if _pipeline_instance is None:
        _pipeline_instance = ChangeDetectionPipeline(
            sql_client=sql_client,
            concept_extractor=concept_extractor,
            section_mapper=section_mapper,
            situation_analyzer=situation_analyzer,
            reference_finder=reference_finder,
            plan_builder=plan_builder
        )
    
    return _pipeline_instance
