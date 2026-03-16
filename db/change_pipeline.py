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
from llm.section_intelligence import SectionIntelligence, SectionReference
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
            impacted_sections_list = []
            new_section_suggestions = []  # Sections that need to be created
            
            for concept in concept_changes:
                impacts = self.section_mapper.map_concept_to_sections(
                    concept=concept,
                    product_code=product_code
                )
                
                # Separate CREATE sections from existing sections
                for impact in impacts:
                    if impact.update_type.value == 'create':
                        # This is a NEW section that doesn't exist yet
                        log.info(f"  🆕 Detected NEW section needed: {impact.section_id} - {impact.section_title}")
                        new_section_suggestions.append({
                            'section_number': impact.section_id,
                            'title': impact.section_title,
                            'concept': concept,
                            'rationale': impact.rationale,
                            'priority': impact.priority
                        })
                    else:
                        # Existing section - get details from Neo4j
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
                            impacted_sections_list.append(impacted_section)
            
            # Handle new section suggestions - these bypass pattern analysis
            update_plans = []
            
            if new_section_suggestions:
                log.info(f"\n🆕 Processing {len(new_section_suggestions)} NEW section suggestions...")
                for new_sec in new_section_suggestions:
                    log.info(f"  Creating plan for NEW section: {new_sec['section_number']}")
                    
                    # CRITICAL: Don't blindly use reference section number!
                    # Need to determine WHERE this section fits in target product's hierarchy
                    
                    # Step 1: Find reference section for content/format template
                    reference_info = self.reference_finder.find_reference_section(
                        target_product_code=product_code,
                        concept=new_sec['concept'].concept,
                        new_situation_description=new_sec['concept'].description
                    )
                    
                    if reference_info:
                        log.info(f"    ✅ Found reference: {reference_info['product_code']} Section {reference_info['section_number']}")
                        
                        # Step 2: Determine correct section number for TARGET product's hierarchy
                        target_section_number = self._determine_section_placement(
                            target_product_code=product_code,
                            reference_section_number=reference_info['section_number'],
                            reference_title=reference_info['title'],
                            concept=new_sec['concept']
                        )
                        
                        log.info(f"    📍 Placement: Section {target_section_number} in {product_code}'s hierarchy")
                        
                        plan = SectionUpdatePlan(
                            section_id=f"{product_code}__section__{target_section_number}",
                            section_number=target_section_number,  # Use calculated placement!
                            title=reference_info['title'],  # Keep reference title
                            product_code=product_code,
                            dossier_id='',
                            status="READY_FOR_GENERATION",
                            pattern_change_type="NEW_PATTERN",
                            pattern_reasoning=f"New section required at {target_section_number}: {new_sec['rationale']}. Using {reference_info['product_code']} section {reference_info['section_number']} as template.",
                            old_semantic_description="N/A - section does not exist yet",
                            new_semantic_description=new_sec['concept'].description,
                            reference_source="CROSS_DOSSIER",
                            reference_section_id=reference_info.get('section_id'),
                            reference_product_code=reference_info.get('product_code'),
                            reference_section_number=reference_info.get('section_number'),
                            reference_full_text=reference_info.get('full_text', ''),
                            reference_content_format=reference_info.get('content_format', 'paragraphs'),
                            parent_section_number=self._get_parent_number(target_section_number),
                            sibling_sections=[],
                            concept_changes=[new_sec['concept']],
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
                            pattern_reasoning=f"New section required but no reference found: {new_sec['rationale']}",
                            old_semantic_description="N/A - section does not exist yet",
                            new_semantic_description=new_sec['concept'].description,
                            reference_source="NOT_FOUND",
                            reference_section_id=None,
                            reference_product_code=None,
                            reference_section_number=None,
                            reference_full_text='',
                            reference_content_format='unknown',
                            parent_section_number=None,
                            sibling_sections=[],
                            concept_changes=[new_sec['concept']],
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
            
            log.info(f"  ✅ Built {len(update_plans)} update plans:")
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
    ) -> str:
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
        
        return placement.new_section_number
    
    def _get_parent_number(self, section_number: str) -> str:
        """
        Extract parent section number.
        Example: "2.2.3.1" -> "2.2.3"
        """
        parts = section_number.split('.')
        if len(parts) > 1:
            return '.'.join(parts[:-1])
        return ""


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
