"""
PHASE 9: Section Content Generation
Generates actual formatted content from update plans.
100% LLM-driven - analyzes reference format and generates matching content.
"""

import re
from typing import List, Optional
from pydantic import BaseModel, Field
from parsers.models import SectionUpdatePlan, ConceptChangeOutput
from llm.azure_client import AzureLLMClient
from graph.neo4j_client import client as neo4j_client
from db.sql_client import get_sql_client
from utils.logger import get_logger

logger = get_logger(__name__)

# Strict mapping of Section Number to its isolated DB-Level Context View
CONTEXT_VIEW_MAP = {
    "2.2.1": "vw_Context_ReferenceFormula",
    "2.2.2.1": "vw_Context_Allergens",
    "2.2.2.2": "vw_Context_CMR",
    "2.2.7": "vw_Context_NaturalOrigin"
}

# Internal DB columns that should never reach the LLM or appear in documents
INTERNAL_COLUMNS = {'ProductID', 'ProductCode'}


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
        
        # [NEW ARCHITECTURE] Execute Targeted DB View Context Pull
        sql_client = get_sql_client()
        target_context_array = []
        context_view_name = CONTEXT_VIEW_MAP.get(plan.section_number)
        
        if context_view_name:
            logger.info(f"   Executing DB View Scoping: {context_view_name}")
            target_context_array = sql_client.fetch_context_view(context_view_name, plan.product_code)
            # Strip internal DB columns before they reach the LLM
            target_context_array = [
                {k: v for k, v in row.items() if k not in INTERNAL_COLUMNS}
                for row in target_context_array
            ]

        # Extract format from reference
        format_analysis = self._analyze_format(plan.reference_full_text)
        logger.info(f"   Detected format: {format_analysis['style']}")
        
        # Generate content using LLM
        generated_text = self._generate_with_llm(
            plan=plan,
            format_style=format_analysis,
            sql_context_array=target_context_array
        )
        
        # Extract dynamically calculated hierarchy cascades
        renumbering_map = {}
        requires_renumbering = False
        
        if hasattr(plan, 'sibling_sections') and plan.sibling_sections:
            renumbering_map = {sib['old']: sib['new'] for sib in plan.sibling_sections if isinstance(sib, dict) and 'old' in sib and 'new' in sib}
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
            renumbering_map=renumbering_map ##dict
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
    
    def _generate_with_llm(self, plan: SectionUpdatePlan, format_style: dict, sql_context_array: list) -> str:
        """
        Generate actual section content using LLM.
        Branches prompt strategy based on SAME_PATTERN vs NEW_PATTERN.
        """
        is_same_pattern = (plan.pattern_change_type == "SAME_PATTERN")

        # Build change summary
        change_details = "\n".join([
            f"- {cc.concept}: {cc.change_type}\n  Description: {cc.description}\n  Affected: {cc.affected_entity}"
            for cc in plan.concept_changes
        ])

        # Build the conditional Rule 1 block
        if is_same_pattern:
            rule_1_block = f"""1. DOCUMENT PRESERVATION (HOW to write):
   - The Reference Section IS your current live document for this product.
   - PRESERVE all introductory paragraphs, concluding paragraphs, and regulatory boilerplate language VERBATIM. These are not templates — they are the actual document text and must appear unchanged in your output.
   - PRESERVE all data rows/entries that are NOT targeted by the Specific Changes exactly as they appear in the Reference Section. Do not regenerate untouched rows from the database context.
   - Only MODIFY the specific rows/entries/values targeted by the changes.
   - You MUST fix any obvious typographical or OCR spacing errors (e.g., missing spaces between words) found in the Reference Section when producing your final text.
   - Detected Format: {format_style['style']}
   - Detected Structural Pattern: {format_style['structural_pattern']}"""
        else:
            rule_1_block = f"""1. SKELETON REPLICATION (HOW to write):
   - The Reference Section is a FORMAT TEMPLATE from a different product. Adopt its exact structural layout, formatting style, and regulatory vocabulary.
   - CRITICAL ISOLATION: Do NOT carry over any product-specific data from the Reference Section (ingredient names, concentrations, supplier names, entity names). Only replicate its structure, regulatory phrasing patterns, and layout.
   - You MUST fix any obvious typographical or OCR spacing errors (e.g., missing spaces between words) found in the Reference Section when producing your final text.
   - Detected Reference Format Constraint: {format_style['style']}
   - Detected Reference Structural Pattern: {format_style['structural_pattern']}"""

        system_prompt = f"""You are an expert regulatory dossier writer, responsible for drafting highly precise compliance documents.

Your objective is to generate the COMPLETE text for a specific section of a product dossier by surgically applying changes to the current data.

INSTRUCTIONS & RULES:

{rule_1_block}

2. DATA INJECTION (WHAT to write):
   - You will receive "Specific Changes" and optionally a "LIVE DATABASE CONTEXT" array.
   - The DATABASE CONTEXT provides current factual values from the database. Use it to verify and populate data values.
   - POINT-BLANK REPLACEMENT: For each Specific Change, locate the exact corresponding entry in the document and execute a surgical replacement of ONLY its value. Do NOT append updates to the end if an existing entry already covers that entity.
   - NOVEL ENTITY INSERTION: If a change introduces a new entity not present in the current document, insert it in proper alphabetical or hierarchical order, matching the established structural pattern.
   - SILENT SKIP: If a listed change has NO corresponding entry in the DATABASE CONTEXT, do NOT mention it at all. Only include data that is verifiable in the database context. NEVER write changelog-style statements like "X has been updated" or "X has been modified to reflect changes."

3. MARKDOWN INTEGRITY:
   - If editing a Markdown table, meticulously maintain pipe `|` alignment. Prevent jagged edges. Guarantee that the modified cell seamlessly occupies the correct corresponding column.

4. REQUIRED OUTPUT FORMAT:
   - Step 1: You must FIRST write out your Chain of Thought analysis enclosed strictly within `<reasoning>` and `</reasoning>` tags. Detail exactly what rows exist, what needs modification, what needs creating, and how you will format it, before continuing.
   - Step 2: Immediately following the closing `</reasoning>` tag, output the finalized, complete content for the target section.
   - DO NOT wrap your text in markdown formatting code blocks unless mandated by the structure itself. Output raw text.
   - Your output must read as a final regulatory document. No meta-commentary, no change descriptions, no annotations about what was modified."""

        # Build the conditional TASK block
        if is_same_pattern:
            task_block = f"""== TASK ==
Generate the complete and updated section content.
1. Think step-by-step inside a `<reasoning>...</reasoning>` block. Identify:
   - Which specific rows/entries are targeted by the changes
   - What their current values are in the Reference Section
   - What the new values should be (from the DATABASE CONTEXT or change descriptions)
   - Which parts of the document remain completely untouched

2. FORMAT RULE: Output using the SAME format as the Reference Section. If the Reference uses bullet points, output bullet points. If it uses a table, output a table. If it uses prose, output prose. The Reference Section's format is authoritative — do NOT convert between formats.

3. COLUMN/LABEL NAMING: When presenting data in tables or lists, use the same column headers and label style as the Reference Section — NOT the raw database key names. For example, if the Reference Section says "Manufacturer (or Supplier)" but the database key is "Supplier Name", use "Manufacturer (or Supplier)".

4. For rows NOT targeted by any change, preserve them exactly from the Reference Section text — do not regenerate them from the database array.

5. Below the reasoning block, output ONLY the full, final target dossier section. DO NOT output the "Source:" or "Text:" headers. Include all introductory and concluding paragraphs from the Reference Section."""
        else:
            task_block = f"""== TASK ==
Generate the complete and updated section content.
1. Think step-by-step inside a `<reasoning>...</reasoning>` block. Map exactly how you will populate the Reference Skeleton format with the provided data.

2. FORMAT RULE: Output using the SAME format as the Reference Section. If the Reference uses bullet points, output bullet points. If it uses a table, output a table. If it uses prose, output prose. The Reference Section's format is authoritative — do NOT convert between formats.

3. DATA SCOPE: The LIVE DATABASE CONTEXT defines the complete data set for this section. Format its entries into the Reference Skeleton's structure. If the database context is empty, rely on the change descriptions.

4. COLUMN/LABEL NAMING: When presenting data, use human-readable regulatory terminology that matches the Reference Section's style — NOT raw database key names (e.g., use "Ingredient" not "INCI_Name", use "Allergen" not "AllergenName").

5. Below the reasoning block, output ONLY the full, final target dossier section. Adopt the Reference Skeleton's introductory regulatory language and phrasing patterns, but populate with YOUR product's data only."""

        user_prompt = f"""== REFERENCE SECTION ==
Source: Product {plan.reference_product_code}, Section {plan.reference_section_number}: {plan.title}
Text:
{plan.reference_full_text}

== CONTEXT ==
Current Situation Summary (for context only, not for output):
{plan.old_semantic_description}

Specific Database Changes Triggering this Update:
{change_details}

LIVE DATABASE CONTEXT (current factual values):
{str(sql_context_array) if sql_context_array else "No database context available for this section."}

{task_block}"""

        try:
            response = self.llm.ask(
                prompt=user_prompt,
                system_prompt=system_prompt,
                temperature=0.3,  # Some creativity for natural flow
                max_tokens=2000
            )
            
            if response.success:
                raw_content = response.content.strip()
                
                # Strip out the <reasoning> block to yield purely dossier text
                cleaned_content = re.sub(r'<reasoning>.*?</reasoning>\s*', '', raw_content, flags=re.DOTALL).strip()
                
                # If everything was stripped, fallback to original just in case
                if not cleaned_content:
                    return raw_content
                
                return cleaned_content
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
