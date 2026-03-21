"""
Phase 4: Section Mapper Service  
================================

100% LLM-DRIVEN section mapping with NO hardcoded rules.

Maps regulatory concepts → affected dossier sections using AI reasoning.
The LLM analyzes concept details and available sections to determine:
- Which sections need updates
- Priority levels (critical/high/medium/low)
- Update types (replace/append/modify/remove)
- Relevance scores and rationale

Fully dynamic and adaptable to any dossier structure.
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

from parsers.models import ConceptChangeOutput
from graph.neo4j_client import client as neo4j_singleton, Neo4jClient
from llm.azure_client import get_llm_client, AzureLLMClient
from pydantic import BaseModel, Field
from utils.logger import get_logger

log = get_logger(__name__)


class SectionPriority(str, Enum):
    """Priority levels for section updates."""
    CRITICAL = "critical"  # Safety-critical, compliance mandatory
    HIGH = "high"  # Important regulatory statements
    MEDIUM = "medium"  # Standard updates
    LOW = "low"  # Optional or informational


class SectionUpdateType(str, Enum):
    """Type of update required for section."""
    REPLACE = "replace"  # Complete section rewrite
    APPEND = "append"  # Add new information
    MODIFY = "modify"  # Update existing content
    REMOVE = "remove"  # Delete information
    CREATE = "create"  # Create new section (doesn't exist yet)


@dataclass
class SectionImpact:
    """
    Represents impact of a concept change on a specific section.
    
    Attributes:
        section_id: Section identifier (short format like '2.2.2.1')
        section_title: Human-readable section title
        priority: Update priority (critical/high/medium/low)
        update_type: Type of update needed (replace/append/modify/remove)
        relevance_score: 0.0-1.0 score indicating how relevant this change is
        rationale: Explanation of why this section is affected
        current_content_length: Character count of existing section content
    """
    section_id: str
    section_title: str
    priority: SectionPriority
    update_type: SectionUpdateType
    relevance_score: float  # 0.0 to 1.0
    rationale: str
    current_content_length: int = 0


class SectionImpactDetails(BaseModel):
    """Individual section impact from LLM."""
    section_number: str = Field(
        description="Section number (e.g., '2.2.2.1')"
    )
    section_title: str = Field(
        description="Section title"
    )
    priority: str = Field(
        description="Priority: critical, high, medium, or low"
    )
    update_type: str = Field(
        description="Update type: replace, append, modify, or remove"
    )
    relevance_score: float = Field(
        description="Relevance score from 0.0 to 1.0",
        ge=0.0,
        le=1.0
    )
    rationale: str = Field(
        description="Brief explanation of why this section needs updating"
    )


class SectionMappingOutput(BaseModel):
    """LLM-structured output for section mapping."""
    reasoning_steps: List[str] = Field(
        description="Step-by-step CoT reasoning analyzing section exclusivity, specificity, and actionability before making final selection."
    )
    affected_sections: List[SectionImpactDetails] = Field(
        description="Filtered list of ONLY the most specific, non-overlapping, and actionable sections."
    )
    overall_assessment: str = Field(
        description="Overall assessment of the change impact on the dossier"
    )


class SectionMapper:
    """
    100% LLM-driven section mapper - NO hardcoded rules.
    
    Architecture:
    - Takes ConceptChangeOutput from Phase 3
    - Queries Neo4j for all available sections
    - Uses LLM to intelligently determine:
      * Which sections are affected
      * Priority and update type for each
      * Relevance scores and rationale
    - Returns SectionImpact objects (sorted by priority)
    
    The LLM has full context about:
    - The change concept (name, type, entities, confidence)
    - All available sections (number, title, content preview)
    - Semantic descriptions and domain concepts (if available)
    """
    
    def __init__(
        self,
        neo4j_client: Optional[Neo4jClient] = None,
        azure_client: Optional[AzureLLMClient] = None
    ):
        """
        Initialize section mapper.
        
        Args:
            neo4j_client: Neo4j client for section queries (optional, uses singleton)
            azure_client: Azure OpenAI client (optional, uses singleton)
        """
        self.neo4j_client = neo4j_client or neo4j_singleton
        self.azure_client = azure_client or get_llm_client()
        
        log.info("SectionMapper initialized (100% LLM-driven)")
    
    def map_concept_to_sections(
        self,
        concept: ConceptChangeOutput,
        product_code: str
    ) -> List[SectionImpact]:
        """
        Map a regulatory concept to affected dossier sections using LLM reasoning.
        
        100% dynamic - NO hardcoded rules. The LLM analyzes:
        - Concept details (name, type, entities, confidence)
        - All available sections (title, content preview, structure)
        - Semantic metadata (domain concepts, keywords)
        
        Args:
            concept: Interpreted concept from Phase 3
            product_code: Product identifier for section lookup
        
        Returns:
            List of SectionImpact objects (sorted by priority desc, relevance desc)
        """
        log.info(
            f"Mapping concept '{concept.concept}' ({concept.change_type}) "
            f"for product {product_code} - using LLM reasoning"
        )
        
        # Get all available sections from Neo4j
        all_sections = self._get_all_sections(product_code)
        
        if not all_sections:
            log.error(f"No sections found for product {product_code}")
            return []
        
        log.info(f"Found {len(all_sections)} sections for analysis")
        
        # Use LLM to determine affected sections
        impacts = self._apply_llm_mapping(concept, all_sections, product_code)
        
        if not impacts:
            log.warning(f"LLM mapping returned no sections for concept: {concept.concept}")
            return []
        
        # Sort by priority (critical first) then relevance score
        priority_order = {
            SectionPriority.CRITICAL: 0,
            SectionPriority.HIGH: 1,
            SectionPriority.MEDIUM: 2,
            SectionPriority.LOW: 3
        }
        
        impacts.sort(
            key=lambda x: (priority_order[x.priority], -x.relevance_score)
        )
        
        log.info(f"Mapped to {len(impacts)} sections (priority-sorted)")
        return impacts
    
    
    def _apply_llm_mapping(
        self,
        concept: ConceptChangeOutput,
        all_sections: List[Dict[str, Any]],
        product_code: str
    ) -> List[SectionImpact]:
        """
        Use LLM to intelligently map concept to sections.
        
        The LLM analyzes the concept and ALL available sections to determine:
        - Which sections are affected and why
        - Priority level for each section
        - Type of update needed
        - Relevance score
        
        Args:
            concept: Regulatory concept from Phase 3
            all_sections: All available sections from Neo4j
            product_code: Product identifier
        
        Returns:
            List of SectionImpact objects
        """
        # Build comprehensive section list for LLM
        # NO PREPROCESSING - let the LLM analyze everything intelligently
        section_descriptions = []
        
        for s in all_sections:
            # Get FULL content - NO TRUNCATION, NO FILTERING!
            # The LLM is smart enough to analyze everything
            content_full = s.get('content') or 'No content yet'
            
            # Mark status for LLM awareness
            status_marker = ""
            if s.get('section_status') == 'suggested_new':
                status_marker = " [CANDIDATE - exists in other products, not here]"
            elif s.get('section_status') == 'reference_only':
                status_marker = " [REFERENCE - from another product]"
            
            section_desc = f"""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Section {s['section_number']}: {s['title']}{status_marker}

FULL TEXT:
{content_full}

Domain: {s.get('domain_concepts', [])}
Status: {s.get('section_status', 'existing')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""
            
            section_descriptions.append(section_desc)
        
        sections_text = "\n\n".join(section_descriptions)
        
        # Build comprehensive prompt - COMPLETELY DYNAMIC
        system_prompt = """You are an elite regulatory affairs AI with deep expertise in compliance dossiers.

🧠 YOUR INTELLIGENCE:
You can understand ANY dossier format, ANY regulatory framework, ANY language or structure. You analyze content semantically, not through patterns or rules.

🎯 YOUR TASK:
A change has been detected. You must determine the EXACT, MOST SPECIFIC section(s) to update. 

⚡ DECISION FRAMEWORK & GATEKEEPER RULES:
You must apply these strict exclusivity rules before selecting any section:

**RULE 1: The Mutual Exclusivity Rule**
Different data categories belong in strictly separate regulatory buckets. If a change belongs to a specific regulatory concept (e.g., Category A), DO NOT also flag sections describing a related but distinct concept (e.g., Category B) simply because they share a semantic theme. 
*EXCEPTION FOR MATHEMATICAL DEPENDENCIES:* If the change involves a quantitative or numerical data point (e.g., a percentage, weight, or concentration), you MUST ALSO flag any downstream sections that explicitly contain mathematical totals, scores, or calculations that are computationally derived from the changed data points. You must determine this dynamically by reading the section text to see if it claims to calculate a total based on the modified data.

**RULE 2: The Actionability Rule (Strict Data Tracking)**
Do not select a section merely because it discusses the topic or entities involved. You must only select a section if the RAW TEXT of that specific section actively tracks the EXACT specific metric/data point that changed. For example, if an ingredient percentage changes, but the section's table only tracks ingredient names and NOT percentages, DO NOT select it. Never attempt to add new columns, paragraphs, or metrics to a section that didn't previously track that specific type of data.

**RULE 3: The Specificity Override (No Parent Containers)**
If the dossier structure contains parent containers (e.g., '1.0 Main Topic') and specific sub-sections (e.g., '1.1 Sub-Topic'), you must ONLY return the specific sub-section where the data lives. NEVER return the broader parent structural section.

**RULE 4: New Section Exclusivity**
If you determine that the concept requires creating a completely NEW section because no existing section represents this exact regulatory bucket, you must NOT also suggest modifying vaguely related existing fallback sections.

**STEP 1: Reason using Chain of Thought**
Before outputting `affected_sections`, you must populate `reasoning_steps` by concisely answering:
- Step 1: What is the highly specific regulatory category of this change?
- Step 2: What is the single most specific dossier section for this category? (Is it an existing subsection, or does it require a completely NEW section?)
- Step 3: Are there any parent container sections I need to drop/exclude?
- Step 4: Are there any conceptually related but distinctly categorized sections I need to drop/exclude? (Remember the Exception for Mathematical Dependencies cascading to calculated totals).

**STEP 2: Determine Update Type**
- ALWAYS prefer modifying an existing section if the topic logically fits there (e.g. updating a limit).
- HOWEVER, if the change introduces an entirely new regulatory category that is NOT adequately covered by ANY existing section, use update_type "create" and invent a logical placeholder (e.g., "NEW_CATEGORY_NAME").
- For value updates and concentration changes: ALWAYS modify the most specific existing section.

**STEP 3: Priority Assessment**
- CRITICAL: Safety, legal compliance, prohibited items
- HIGH: Regulatory declarations, major risk assessments
- MEDIUM: Standard property updates
- LOW: Minor clarifications

📊 OUTPUT FORMAT:
For each VERIFIED affected section provide section number, title, priority, update type, relevance score, and rationale."""
        
        user_prompt = f"""═══════════════════════════════════════════════════════════════════════
                              CHANGE DETECTED
═══════════════════════════════════════════════════════════════════════

Concept: {concept.concept}
Change Type: {concept.change_type}
Description: {concept.description}
Affected Entity: {concept.affected_entity}
Confidence: {concept.confidence}

═══════════════════════════════════════════════════════════════════════
                          AVAILABLE DOSSIER SECTIONS
                          (Full Content Provided)
═══════════════════════════════════════════════════════════════════════

{sections_text}

═══════════════════════════════════════════════════════════════════════
                              YOUR ANALYSIS
═══════════════════════════════════════════════════════════════════════

1. Read and understand each section's FULL TEXT
2. Identify which sections semantically relate to this change
3. Apply the Strict Exclusivity Gatekeeper Rules to deeply filter your selection.

You are acting as a strict semantic router.
Provide your reasoning steps first inside `reasoning_steps`, answering the 4 logical steps, then output only the deeply filtered list of genuinely actionable sections inside `affected_sections`."""
        
        try:
            # Call LLM with structured output
            log.info(f"Calling LLM for section mapping (analyzing {len(all_sections)} sections)")
            
            result = self.azure_client.ask_structured_pydantic(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=SectionMappingOutput
            )
            
            log.info(f"LLM identified {len(result.affected_sections)} affected sections")
            log.info(f"CoT Reasoning Execution:")
            for idx, step in enumerate(result.reasoning_steps):
                log.info(f"  Step {idx+1}: {step}")
            log.info(f"Overall assessment: {result.overall_assessment}")
            
            # Convert to SectionImpact objects
            impacts = []
            for section_detail in result.affected_sections:
                # Parse enums with fallback
                try:
                    priority = SectionPriority(section_detail.priority.lower())
                except (ValueError, AttributeError):
                    priority = SectionPriority.MEDIUM
                    log.warning(f"Invalid priority '{section_detail.priority}', using MEDIUM")
                
                try:
                    update_type_str = section_detail.update_type.lower()
                    if update_type_str == "create" or section_detail.section_number.startswith("NEW_"):
                        update_type = SectionUpdateType.CREATE
                        log.info(f"  Section {section_detail.section_number} marked as CREATE (suggested new)")
                    else:
                        update_type = SectionUpdateType(update_type_str)
                except ValueError:
                    update_type = SectionUpdateType.MODIFY
                    log.warning(f"Invalid update_type '{section_detail.update_type}', using MODIFY")
                
                # Find section in all_sections to get content length
                section_info = next(
                    (s for s in all_sections if s['section_number'] == section_detail.section_number),
                    None
                )
                
                content_length = 0
                if section_info and section_info.get('content'):
                    content_length = len(section_info['content'])
                
                impact = SectionImpact(
                    section_id=section_detail.section_number,
                    section_title=section_detail.section_title,
                    priority=priority,
                    update_type=update_type,
                    relevance_score=section_detail.relevance_score,
                    rationale=section_detail.rationale,
                    current_content_length=content_length
                )
                
                impacts.append(impact)
                log.debug(
                    f"  → {section_detail.section_number} ({priority.value}, "
                    f"relevance={section_detail.relevance_score:.2f}): {section_detail.rationale[:80]}"
                )
            
            return impacts
        
        except Exception as e:
            log.error(f"LLM mapping failed: {e}", exc_info=True)
            return []
    
    def _get_all_sections(self, product_code: str) -> List[Dict[str, Any]]:
        """
        Get sections for a product from Neo4j.
        
        ENHANCED: Also checks if similar sections exist in other products
        to detect missing sections that should be created.
        
        Args:
            product_code: Product identifier
        
        Returns:
            List of section dicts (includes both existing and suggested new sections)
        """
        # Get existing sections for target product
        query_existing = """
        MATCH (s:Section {product_code: $product_code})
        RETURN s.section_id AS id,
               s.section_number AS section_number,
               s.title AS title,
               s.full_text AS content,
               s.domain_concepts AS domain_concepts,
               $product_code AS source_product,
               'existing' AS section_status
        ORDER BY s.section_number
        """
        
        # Get sections from OTHER products with similar concepts
        # This enables cross-dossier awareness!
        query_other_products = """
        MATCH (s:Section)
        WHERE s.product_code <> $product_code
          AND s.domain_concepts IS NOT NULL
          AND size(s.domain_concepts) > 0
        RETURN DISTINCT s.section_number AS section_number,
               s.title AS title,
               s.domain_concepts AS domain_concepts,
               s.product_code AS source_product,
               'reference_only' AS section_status
        ORDER BY s.section_number
        LIMIT 20
        """
        
        try:
            # Get existing sections
            existing_sections = self.neo4j_client.run_query(
                query_existing,
                {'product_code': product_code}
            )
            
            # Get reference sections from other products
            reference_sections = self.neo4j_client.run_query(
                query_other_products,
                {'product_code': product_code}
            )
            
            # Detect missing sections: sections that exist in other products but not in target
            existing_numbers = {s['section_number'] for s in existing_sections}
            missing_sections = []
            
            for ref_sec in reference_sections:
                if ref_sec['section_number'] not in existing_numbers:
                    # This section exists elsewhere but not in target product
                    missing_sections.append({
                        'id': None,  # Doesn't exist yet
                        'section_number': ref_sec['section_number'],
                        'title': f"{ref_sec['title']} (NEW - from {ref_sec['source_product']})",
                        'content': f"Section exists in {ref_sec['source_product']} but not in {product_code}. Consider creating.",
                        'domain_concepts': ref_sec.get('domain_concepts', []),
                        'source_product': ref_sec['source_product'],
                        'section_status': 'suggested_new'
                    })
            
            # Combine existing + suggested
            all_sections = existing_sections + missing_sections
            
            log.debug(
                f"Found {len(existing_sections)} existing, "
                f"{len(missing_sections)} suggested new sections"
            )
            
            return all_sections
            
        except Exception as e:
            log.error(f"Failed to get sections: {e}")
            return []


# Singleton
_mapper_instance: Optional[SectionMapper] = None


def get_section_mapper(
    neo4j_client: Optional[Neo4jClient] = None,
    azure_client: Optional[AzureLLMClient] = None
) -> SectionMapper:
    """
    Get singleton SectionMapper instance.
    
    Args:
        neo4j_client: Optional Neo4j client
        azure_client: Optional Azure client
    
    Returns:
        SectionMapper instance
    """
    global _mapper_instance
    
    if _mapper_instance is None:
        _mapper_instance = SectionMapper(
            neo4j_client=neo4j_client,
            azure_client=azure_client
        )
    
    return _mapper_instance
