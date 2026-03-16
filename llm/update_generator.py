"""
Phase 5: Update Generator Service
==================================

Generates actual dossier section updates based on detected changes.
Takes concept + section mapping → produces new section text.

Core responsibility: Transform regulatory concepts into compliant dossier text.
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

from parsers.models import ConceptChangeOutput
from llm.section_mapper import SectionImpact, SectionUpdateType
from graph.neo4j_client import client as neo4j_singleton, Neo4jClient
from llm.azure_client import get_llm_client, AzureLLMClient
from pydantic import BaseModel, Field
from utils.logger import get_logger

log = get_logger(__name__)


class UpdateStrategy(str, Enum):
    """Strategy for generating section updates."""
    COMPLETE_REWRITE = "complete_rewrite"  # Generate entirely new section
    TARGETED_MODIFICATION = "targeted_modification"  # Update specific parts
    APPEND_NEW_INFO = "append_new_info"  # Add new information
    REMOVE_CONTENT = "remove_content"  # Delete specific content


@dataclass
class SectionUpdate:
    """
    Represents a generated section update.
    
    Attributes:
        section_id: Section identifier
        section_title: Section title
        original_content: Current section content
        updated_content: New section content
        strategy: Update strategy used
        changes_applied: Summary of changes made
        confidence_score: 0.0-1.0 confidence in update quality
        validation_notes: Any validation warnings or notes
    """
    section_id: str
    section_title: str
    original_content: str
    updated_content: str
    strategy: UpdateStrategy
    changes_applied: List[str]
    confidence_score: float
    validation_notes: List[str]


class SectionUpdateOutput(BaseModel):
    """LLM-structured output for section updates."""
    updated_content: str = Field(
        description="Complete updated section text in Markdown format"
    )
    changes_summary: List[str] = Field(
        description="List of specific changes made"
    )
    confidence: str = Field(
        description="Confidence level: high, medium, or low"
    )
    warnings: List[str] = Field(
        default_factory=list,
        description="Any validation warnings or concerns"
    )


class UpdateGenerator:
    """
    Generates dossier section updates based on detected changes.
    
    Architecture:
    - Takes ConceptChangeOutput + SectionImpact from Phases 3-4
    - Retrieves current section content + profile from Neo4j
    - Uses LLM to generate compliant updated text
    - Validates updates against section requirements
    - Returns SectionUpdate objects
    
    Update Strategies:
    1. Complete Rewrite: For critical changes (Article 17 rcl classification)
    2. Targeted Modification: For specific updates (add allergen, update statement)
    3. Append: For new information (new test result, new supplier doc)
    4. Remove: For deletions (allergen removed, ingredient eliminated)
    """
    
    def __init__(
        self,
        neo4j_client: Optional[Neo4jClient] = None,
        azure_client: Optional[AzureLLMClient] = None
    ):
        """Initialize update generator."""
        self.neo4j_client = neo4j_client or neo4j_singleton
        self.azure_client = azure_client or get_llm_client()
        
        log.info("UpdateGenerator initialized")
    
    def generate_update(
        self,
        concept: ConceptChangeOutput,
        impact: SectionImpact,
        product_code: str
    ) -> SectionUpdate:
        """
        Generate section update for a specific impact.
        
        Args:
            concept: Regulatory concept from Phase 3
            impact: Section impact from Phase 4
            product_code: Product identifier
        
        Returns:
            SectionUpdate with generated content
        """
        log.info(
            f"Generating update for section {impact.section_id} "
            f"based on concept '{concept.concept}'"
        )
        
        # Get current section content + context
        section_data = self._get_section_context(product_code, impact.section_id)
        
        if not section_data:
            log.error(f"No section data found for {impact.section_id}")
            raise ValueError(f"Section {impact.section_id} not found")
        
        # Determine update strategy
        strategy = self._select_strategy(impact, concept.change_type)
        
        # Generate update using LLM
        result = self._generate_with_llm(
            concept=concept,
            impact=impact,
            section_data=section_data,
            strategy=strategy
        )
        
        # Validate update
        validation_notes = self._validate_update(
            original=section_data['content'],
            updated=result.updated_content,
            section_id=impact.section_id
        )
        
        # Parse confidence
        confidence_map = {"high": 0.9, "medium": 0.7, "low": 0.5}
        confidence_score = confidence_map.get(result.confidence.lower(), 0.7)
        
        update = SectionUpdate(
            section_id=impact.section_id,
            section_title=impact.section_title,
            original_content=section_data['content'],
            updated_content=result.updated_content,
            strategy=strategy,
            changes_applied=result.changes_summary,
            confidence_score=confidence_score,
            validation_notes=validation_notes + result.warnings
        )
        
        log.info(
            f"Update generated: {len(result.updated_content)} chars, "
            f"confidence={result.confidence}"
        )
        
        return update
    
    def generate_batch_updates(
        self,
        concept: ConceptChangeOutput,
        impacts: List[SectionImpact],
        product_code: str
    ) -> List[SectionUpdate]:
        """
        Generate updates for multiple sections from single concept.
        
        Args:
            concept: Regulatory concept
            impacts: List of section impacts
            product_code: Product identifier
        
        Returns:
            List of SectionUpdate objects
        """
        log.info(f"Generating batch updates for {len(impacts)} sections")
        
        updates = []
        for impact in impacts:
            try:
                update = self.generate_update(concept, impact, product_code)
                updates.append(update)
            except Exception as e:
                log.error(f"Failed to generate update for {impact.section_id}: {e}")
                # Continue with other sections
                continue
        
        log.info(f"Generated {len(updates)}/{len(impacts)} updates successfully")
        return updates
    
    def _get_section_context(
        self,
        product_code: str,
        section_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive section context from Neo4j.
        
        Includes:
        - Current section content
        - Section profile (keywords, requirements)
        - Related sections (for consistency)
        - Product metadata
        
        Args:
            product_code: Product identifier
            section_id: Section identifier (short format like '2.2.2.1')
        
        Returns:
            Dict with section context
        """
        # Build full section ID
        full_section_id = f"{product_code}__section__{section_id}"
        
        query = """
        MATCH (s:Section {product_code: $product_code, section_id: $full_section_id})
        MATCH (p:Product {product_code: $product_code})
        RETURN s.section_id AS section_id,
               s.section_number AS section_number,
               s.title AS title,
               s.full_text AS content,
               s.content_format AS content_format,
               p.product_name AS product_name
        """
        
        try:
            results = self.neo4j_client.run_query(
                query,
                {'product_code': product_code, 'full_section_id': full_section_id}
            )
            
            if results:
                return results[0]
            else:
                return None
        
        except Exception as e:
            log.error(f"Failed to get section context: {e}")
            return None
    
    def _select_strategy(
        self,
        impact: SectionImpact,
        change_type: str
    ) -> UpdateStrategy:
        """
        Select update strategy based on impact and change type.
        
        Args:
            impact: Section impact
            change_type: Concept change type (NEW, MODIFIED, etc.)
        
        Returns:
            UpdateStrategy
        """
        # Critical sections with RECLASSIFIED or REPLACED → complete rewrite
        if impact.update_type == SectionUpdateType.REPLACE:
            return UpdateStrategy.COMPLETE_REWRITE
        
        # Append for new information
        elif impact.update_type == SectionUpdateType.APPEND:
            return UpdateStrategy.APPEND_NEW_INFO
        
        # Remove for deletions
        elif impact.update_type == SectionUpdateType.REMOVE:
            return UpdateStrategy.REMOVE_CONTENT
        
        # Default to targeted modification
        else:
            return UpdateStrategy.TARGETED_MODIFICATION
    
    def _generate_with_llm(
        self,
        concept: ConceptChangeOutput,
        impact: SectionImpact,
        section_data: Dict[str, Any],
        strategy: UpdateStrategy
    ) -> SectionUpdateOutput:
        """
        Use LLM to generate section update.
        
        Args:
            concept: Regulatory concept
            impact: Section impact
            section_data: Current section context
            strategy: Update strategy
        
        Returns:
            Structured LLM output
        """
        system_prompt = f"""You are an expert regulatory dossier writer. Generate compliant section updates.

Strategy: {strategy.value}
Section: {impact.section_id} - {impact.section_title}
Article: {section_data.get('article', 'Cosmetic')}

Requirements:
- Maintain regulatory compliance (Article 17/19 if applicable)
- Use clear, professional language
- Include specific details from the change
- Preserve existing formatting structure
- Return complete section text in Markdown format

Output:
- updated_content: Complete updated section text
- changes_summary: List of specific changes made
- confidence: high/medium/low
- warnings: Any concerns or validation notes"""
        
        # Build strategy-specific prompt
        if strategy == UpdateStrategy.COMPLETE_REWRITE:
            instruction = "Generate a complete rewrite of this section incorporating the change."
        elif strategy == UpdateStrategy.TARGETED_MODIFICATION:
            instruction = "Update the specific parts affected by this change, preserving unchanged content."
        elif strategy == UpdateStrategy.APPEND_NEW_INFO:
            instruction = "Append new information to the existing section maintaining flow and structure."
        else:  # REMOVE_CONTENT
            instruction = "Remove the specified content while maintaining section coherence."
        
        current_content = section_data['content'] or "## [New Section]\n\n(No existing content)"
        
        user_prompt = f"""Change to incorporate:
- Concept: {concept.concept}
- Type: {concept.change_type}
- Description: {concept.description}
- Affected Entity: {concept.affected_entity}

Current Section Content:
```markdown
{current_content[:2000]}  
{f"... (truncated, total {len(current_content)} chars)" if len(current_content) > 2000 else ""}
```

{instruction}

Generate the updated section content."""
        
        try:
            result = self.azure_client.ask_structured_pydantic(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=SectionUpdateOutput
            )
            
            return result
        
        except Exception as e:
            log.error(f"LLM generation failed: {e}")
            raise
    
    def _validate_update(
        self,
        original: str,
        updated: str,
        section_id: str
    ) -> List[str]:
        """
        Validate generated update.
        
        Checks:
        - Length change is reasonable (not truncated)
        - Critical keywords preserved (if Article 17/19)
        - Markdown formatting valid
        - No obvious errors
        
        Args:
            original: Original section content
            updated: Updated section content
            section_id: Section identifier
        
        Returns:
            List of validation warnings (empty if all good)
        """
        warnings = []
        
        # Length validation
        orig_len = len(original) if original else 0
        updated_len = len(updated)
        
        if updated_len < orig_len * 0.5:
            warnings.append(
                f"Updated content significantly shorter than original "
                f"({updated_len} vs {orig_len} chars)"
            )
        
        if updated_len == 0:
            warnings.append("Updated content is empty!")
        
        # Article 17/19 keyword preservation (simple check)
        critical_keywords = {
            '3.4.1': ['CMR', 'Article 17', 'prohibited', 'restriction'],
            '3.5.2': ['Article 19', 'allergen', 'labeling']
        }
        
        if section_id in critical_keywords:
            for keyword in critical_keywords[section_id]:
                if keyword.lower() in original.lower() and keyword.lower() not in updated.lower():
                    warnings.append(f"Critical keyword '{keyword}' was removed")
        
        # Basic Markdown validation
        if updated.count('```') % 2 != 0:
            warnings.append("Unclosed code block in Markdown")
        
        return warnings


# Singleton
_generator_instance: Optional[UpdateGenerator] = None


def get_update_generator(
    neo4j_client: Optional[Neo4jClient] = None,
    azure_client: Optional[AzureLLMClient] = None
) -> UpdateGenerator:
    """Get singleton UpdateGenerator instance."""
    global _generator_instance
    
    if _generator_instance is None:
        _generator_instance = UpdateGenerator(
            neo4j_client=neo4j_client,
            azure_client=azure_client
        )
    
    return _generator_instance
