"""
PHASE 9: Section Content Generation
Generates actual formatted content from update plans.
100% LLM-driven - analyzes reference format and generates matching content.
"""

from typing import List, Optional
from pydantic import BaseModel, Field
from llm.azure_client import AzureLLMClient
from parsers.models import SectionUpdatePlan, ConceptChangeOutput
from graph.neo4j_client import client as neo4j_client
from utils.logger import get_logger

logger = get_logger(__name__)


class GeneratedContent(BaseModel):
    """
    Generated section content ready for user approval (Phase 9 output).
    """
    # Source plan
    plan_id: str = Field(..., description="Source plan identifier")
    section_number: str = Field(..., description="Target section number")
    section_title: str = Field(..., description="Section title")
    product_code: str = Field(..., description="Product code")
    
    # Generated content
    generated_text: str = Field(..., description="Generated section content")
    format_style: str = Field(..., description="Detected format (table, bullets, prose, etc.)")
    
    # Metadata
    generation_confidence: float = Field(..., description="Generation confidence (0-1)")
    changes_applied: List[str] = Field(..., description="List of changes incorporated")
    reference_product: Optional[str] = Field(None, description="Reference product used")
    
    # Structural changes
    is_new_section: bool = Field(..., description="Is this a new section?")
    requires_renumbering: bool = Field(default=False, description="Requires sibling renumbering?")
    renumbering_map: dict = Field(default_factory=dict, description="Old → New section numbers")
    
    # Approval tracking
    status: str = Field(default="PENDING_APPROVAL", description="PENDING_APPROVAL | APPROVED | REJECTED")
    user_feedback: Optional[str] = Field(None, description="User comments on generated content")


class SectionContentGenerator:
    """
    Generates actual section content from update plans.
    Analyzes reference format/vocabulary and applies to new data.
    """
    
    def __init__(self):
        self.llm = AzureLLMClient()
        logger.info("SectionContentGenerator initialized (Phase 9)")
    
    def generate_content(self, plan: SectionUpdatePlan) -> GeneratedContent:
        """
        Generate actual formatted content from plan.
        
        Args:
            plan: Complete update plan from Phase 7/8
            
        Returns:
            GeneratedContent with formatted text ready for approval
        """
        logger.info(f"Generating content for {plan.section_number}: {plan.title}")
        
        # Extract format from reference
        format_analysis = self._analyze_format(plan.reference_full_text)
        logger.info(f"   Detected format: {format_analysis['style']}")
        
        # Generate content using LLM
        generated_text = self._generate_with_llm(
            plan=plan,
            format_style=format_analysis
        )
        
        # Extract renumbering info if present
        renumbering_map = {}
        requires_renumbering = False
        
        if hasattr(plan, '__dict__') and 'renumbering_required' in plan.__dict__:
            renumbering_map = plan.__dict__.get('renumbering_required', {})
            requires_renumbering = bool(renumbering_map)
        
        # Check if section actually exists in graph
        # NEW_PATTERN doesn't mean "new section", it means "different format"
        is_new_section = self._check_section_exists(plan.product_code, plan.section_number) == False
        
        # Build output
        content = GeneratedContent(
            plan_id=f"{plan.product_code}_{plan.section_number}",
            section_number=plan.section_number,
            section_title=plan.title,
            product_code=plan.product_code,
            generated_text=generated_text,
            format_style=format_analysis['style'],
            generation_confidence=self._calculate_confidence(plan),
            changes_applied=[f"{cc.concept}: {cc.change_type}" for cc in plan.concept_changes],
            reference_product=plan.reference_product_code,
            is_new_section=is_new_section,
            requires_renumbering=requires_renumbering,
            renumbering_map=renumbering_map
        )
        
        logger.info(f"   ✅ Generated {len(generated_text)} chars (confidence: {content.generation_confidence:.2f})")
        
        return content
    
    def _analyze_format(self, reference_text: str) -> dict:
        """
        Analyze format and style of reference text.
        Detects: table, bullets, prose, numbered list, etc.
        """
        # Use LLM to detect format style
        system_prompt = """You are a document format analyst. Analyze the provided text and identify:
1. Format style (table, bullet list, numbered list, prose, mixed)
2. Key vocabulary patterns (e.g., "has been identified", "is present at", "concentration")
3. Structural patterns (e.g., substance name → concentration → unit → statement)

Provide a JSON response with:
{
    "style": "table|bullets|numbered|prose|mixed",
    "vocabulary_patterns": ["pattern1", "pattern2"],
    "structural_pattern": "description of how information is organized"
}"""
        
        user_prompt = f"""Analyze this reference section content:

{reference_text}

Identify the format style, vocabulary patterns, and structural organization."""
        
        try:
            response = self.llm.ask(
                prompt=user_prompt,
                system_prompt=system_prompt,
                temperature=0.1,
                response_format="json_object"
            )
            
            if response.success:
                # Response already parsed as JSON
                return response.content
            else:
                raise Exception(response.error)
            
        except Exception as e:
            logger.warning(f"Format analysis failed, using fallback: {e}")
            return {
                "style": "prose",
                "vocabulary_patterns": [],
                "structural_pattern": "Standard paragraph format"
            }
    
    def _generate_with_llm(self, plan: SectionUpdatePlan, format_style: dict) -> str:
        """
        Generate actual section content using LLM.
        Applies detected format to new data.
        """
        # Build change summary
        change_details = "\n".join([
            f"- {cc.concept}: {cc.change_type}\n  Description: {cc.description}\n  Affected: {cc.affected_entity}"
            for cc in plan.concept_changes
        ])
        
        # Prepare vocabulary patterns - NO TRUNCATION
        vocab_patterns_text = ', '.join(format_style.get('vocabulary_patterns', []))
        if not vocab_patterns_text:
            vocab_patterns_text = "standard regulatory terminology"
        
        system_prompt = f"""You are an expert regulatory dossier writer, responsible for drafting highly precise compliance documents.

Your objective is to generate the COMPLETE text for a specific section of a product dossier. You will be acting as a strict template engine: you are mapping NEW DATA into a REFERENCE FORMAT skeleton.

INSTRUCTIONS & RULES:
1. FORMAT MIMICRY (HOW to write):
   - You MUST adopt the exact structural layout of the provided Reference Section. If it uses markdown tables, you use tables. If it uses bullet points, you use bullet points. If it uses standard prose, you use prose.
   - You MUST mimic the tone, legal framing, and regulatory vocabulary found in the Reference Section (e.g., detected patterns: {vocab_patterns_text}).
   - Detected Reference Format Constraint: {format_style['style']}
   - Detected Reference Structural Pattern: {format_style['structural_pattern']}

2. DATA ISOLATION (WHAT to write):
   - You must NOT carry over ANY specific product data, chemical names, concentrations, or entity names from the Reference Section. It is strictly a formatting skeleton.
   - You MUST populate the skeleton ONLY with the data provided in the "DATA TO INJECT" block.
   - Retain facts from the "Current State" ONLY if they remain valid and are not overridden by the Specific Changes.

3. COMPLETENESS:
   - Output the finalized, complete content for the target section.
   - DO NOT wrap your output in markdown code blocks or add conversational filler. Output raw, immediate content.
"""

        user_prompt = f"""== REFERENCE SKELETON (Adopt format and vocabulary, drop specific data) ==
Source: Product {plan.reference_product_code}, Section {plan.reference_section_number}: {plan.title}
Text:
{plan.reference_full_text}

== DATA TO INJECT ==
Current State:
{plan.old_semantic_description}

New Target State:
{plan.new_semantic_description}

Specific Changes to Apply:
{change_details}

== TASK ==
Generate the complete content for Product {plan.product_code}, strictly applying the 'DATA TO INJECT' into the structural/vocabulary skeleton of the 'REFERENCE SKELETON'. Output only the finalized section text."""

        try:
            response = self.llm.ask(
                prompt=user_prompt,
                system_prompt=system_prompt,
                temperature=0.3,  # Some creativity for natural flow
                max_tokens=2000
            )
            
            if response.success:
                return response.content.strip()
            else:
                raise Exception(response.error)
            
        except Exception as e:
            logger.error(f"Content generation failed: {e}", exc_info=True)
            raise
    
    def _calculate_confidence(self, plan: SectionUpdatePlan) -> float:
        """Calculate generation confidence based on plan quality."""
        confidence = 0.5  # Base
        
        # Higher if using cross-dossier reference
        if plan.reference_source == "CROSS_DOSSIER":
            confidence += 0.2
        
        # Higher if plan has high confidence
        if plan.overall_confidence == "high":
            confidence += 0.2
        elif plan.overall_confidence == "medium":
            confidence += 0.1
        
        # Higher if reference text is substantial
        if len(plan.reference_full_text) > 200:
            confidence += 0.1
        
        return min(confidence, 1.0)
    
    def _check_section_exists(self, product_code: str, section_number: str) -> bool:
        """
        Check if section actually exists in Neo4j graph.
        
        This determines whether we CREATE or UPDATE.
        NEW_PATTERN doesn't mean "new section" - it means "different format".
        
        Args:
            product_code: Product code
            section_number: Section number
            
        Returns:
            True if section exists, False otherwise
        """
        query = """
        MATCH (s:Section {
            product_code: $product_code,
            section_number: $section_number
        })
        RETURN count(s) > 0 as exists
        """
        
        try:
            results = neo4j_client.run_query(
                query,
                {
                    "product_code": product_code,
                    "section_number": section_number
                }
            )
            
            if results and len(results) > 0:
                return results[0].get('exists', False)
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to check section existence: {e}")
            # Default to UPDATE (safer than creating duplicates)
            return True
