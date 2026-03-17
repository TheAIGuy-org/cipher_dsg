"""
PHASE 9: Section Content Generation
Generates actual formatted content from update plans.
100% LLM-driven - analyzes reference format and generates matching content.
"""

from typing import List, Optional
from pydantic import BaseModel, Field
from llm.azure_client import AzureLLMClient
from parsers.models import SectionUpdatePlan, ConceptChangeOutput
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
            is_new_section=(plan.pattern_change_type == "NEW_PATTERN"),
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
        
        system_prompt = f"""You are an expert regulatory dossier writer for cosmetic products.

Generate section content that:
1. Matches the EXACT format style of the reference: {format_style['style']}
2. Uses similar vocabulary patterns: {', '.join(format_style.get('vocabulary_patterns', [])[:3])}
3. Follows the structural pattern: {format_style['structural_pattern']}
4. Incorporates ALL the new changes with specific data (substance names, concentrations, classifications)
5. Maintains regulatory compliance and precision

CRITICAL - DATA vs FORMAT:
- The reference section is from a DIFFERENT product - use it ONLY for format/style/vocabulary
- DO NOT copy specific substance names, allergens, or data from the reference
- ONLY include data explicitly mentioned in the "NEW CHANGES" section
- If reference lists "Substance A, Substance B" but changes only add "Substance C" → output ONLY "Substance C"
- Reference = template for HOW to write, NOT WHAT to write

FORMAT RULES:
- Use the SAME format structure as the reference (bullets, tables, prose)
- Keep regulatory tone and vocabulary from reference
- Be precise with numbers and units from changes"""

        user_prompt = f"""Reference Section from Product {plan.reference_product_code}:
Section {plan.reference_section_number}: {plan.title}

{plan.reference_full_text}

---

NEW CHANGES to incorporate:
{change_details}

---

Current Situation:
{plan.old_semantic_description}

Target Situation:
{plan.new_semantic_description}

---

Generate the COMPLETE section content for Product {plan.product_code} that:
- Uses the same format style as the reference
- Incorporates all the new change data with specific details
- Maintains regulatory compliance
- Matches the vocabulary and tone

Output ONLY the section content (no explanations or meta-text)."""

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
