"""
parsers/models.py
------------------
Pydantic models for structured LLM outputs in parsing phase.
All models use Pydantic v2 for validation and structured generation.
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any


class SemanticCharacteristics(BaseModel):
    """
    Flexible characteristics extracted from section.
    No predefined schema - adapts to any document type.
    
    Note: All fields are optional to maintain flexibility.
    LLM populates only what's relevant for each section.
    """
    model_config = {"extra": "forbid"}  # Required by OpenAI structured outputs
    
    item_count: Optional[int] = Field(
        None,
        description="Number of items/entities in section (e.g., allergens, suppliers, test results)"
    )
    format_style: Optional[str] = Field(
        None,
        description="Primary format style: 'bullet_list', 'table', 'narrative', 'mixed'"
    )
    has_regulatory_references: Optional[bool] = Field(
        None,
        description="Whether section cites regulatory documents (e.g., Annex III, Article 17)"
    )
    complexity_level: Optional[str] = Field(
        None,
        description="Content complexity: 'simple', 'moderate', 'complex'"
    )
    uses_numerical_data: Optional[bool] = Field(
        None,
        description="Whether section contains numerical values, percentages, or measurements"
    )
    hierarchical_structure: Optional[bool] = Field(
        None,
        description="Whether section has nested subsections or hierarchical organization"
    )


class SemanticProfileOutput(BaseModel):
    """
    Structured output from semantic profile generation.
    Fully generic - works for any document domain.
    """
    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "description": "Section lists 2 allergens (Vanillin, Linalool) in bullet format with Annex III references. Each allergen includes notes on presence source and regulatory classification.",
                "characteristics": {
                    "item_count": 2,
                    "format_style": "bullet_list",
                    "has_regulatory_references": True,
                    "complexity_level": "moderate",
                    "uses_numerical_data": False,
                    "hierarchical_structure": False
                }
            }
        }
    }
    
    description: str = Field(
        ...,
        description="2-3 sentence description of section's situation, format, and content",
        min_length=50,
        max_length=500
    )
    characteristics: SemanticCharacteristics = Field(
        ...,
        description="Structured characteristics extracted from section"
    )


class DomainConceptsOutput(BaseModel):
    """
    Structured output from domain concept extraction.
    No predefined taxonomy - LLM discovers concepts from content.
    """
    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "examples": [
                {
                    "concepts": ["allergen declaration", "raw material safety"],
                    "reasoning": "Section declares allergens present in raw materials with Annex III compliance"
                },
                {
                    "concepts": ["adverse event reporting", "clinical trial results"],
                    "reasoning": "Section presents adverse events from Phase III clinical trials with incidence rates"
                },
                {
                    "concepts": ["nutritional content", "allergen warning"],
                    "reasoning": "Section lists nutritional facts and declares common food allergens"
                }
            ]
        }
    }
    
    concepts: List[str] = Field(
        ...,
        description="1-3 short descriptive concept labels (2-5 words each)",
        min_length=1,
        max_length=3
    )
    reasoning: Optional[str] = Field(
        None,
        description="Brief explanation of why these concepts were identified"
    )


class ReferenceFormatEvidence(BaseModel):
    """
    Evidence from reference sections for pattern decisions.
    """
    model_config = {"extra": "forbid"}
    
    product_name: str
    section_number: str
    format_style: str
    item_count: Optional[int] = None
    description: str


class PatternDecisionOutput(BaseModel):
    """
    Structured output for pattern change decisions.
    Evidence-based, not confidence-based.
    """
    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "pattern_change": "SAME_PATTERN",
                "reasoning": "Current section lists 1 allergen in sentence format. Reference sections with 1-2 allergens also use sentence format. Adding 2nd allergen can be accommodated by extending sentence with comma separation.",
                "evidence_used": "Lipstick section 2.2.2.1 with single allergen in sentence"
            }
        }
    }
    
    pattern_change: str = Field(
        ...,
        description="Pattern decision: SAME_PATTERN or NEW_PATTERN",
        pattern="^(SAME_PATTERN|NEW_PATTERN)$"
    )
    reasoning: str = Field(
        ...,
        description="Detailed reasoning based on reference evidence",
        min_length=30
    )
    evidence_used: Optional[str] = Field(
        None,
        description="Which reference section(s) informed this decision"
    )


class ConceptChangeOutput(BaseModel):
    """
    Structured output from concept extraction (DB changes).
    
    100% FLEXIBLE - No hardcoded change type constraints.
    The LLM describes the change type in natural language based on semantic understanding.
    """
    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "examples": [
                {
                    "concept": "allergen presence in formulation",
                    "change_type": "new item added",
                    "description": "Allergen 'Linalool' added to perfume ingredient in product formulation",
                    "affected_entity": "Raw material: Perfum Vanille SA26",
                    "confidence": "high"
                },
                {
                    "concept": "trace substance regulatory limit",
                    "change_type": "threshold value increased",
                    "description": "Maximum Toluene level updated from 80 ppm to 90 ppm in Vitamin E Acetate",
                    "affected_entity": "Raw material: DL Alpha Tocopheryl Acetate",
                    "confidence": "high"
                },
                {
                    "concept": "supplier certification status",
                    "change_type": "expiration date approaching",
                    "description": "ISO 9001 certification for Supplier ABC expires within 30 days",
                    "affected_entity": "Supplier: ABC Chemicals Ltd",
                    "confidence": "high"
                },
                {
                    "concept": "batch testing protocol",
                    "change_type": "methodology replaced",
                    "description": "Nitrosamine testing method changed from GC-MS to LC-MS/MS for higher sensitivity",
                    "affected_entity": "Quality control protocol: Nitrosamine Analysis",
                    "confidence": "high"
                }
            ]
        }
    }
    
    concept: str = Field(
        ...,
        description="Regulatory/compliance concept affected (2-5 words)",
        min_length=5,
        max_length=100
    )
    change_type: str = Field(
        ...,
        description="Natural language description of the change type. Be specific and semantic. Examples: 'new item added', 'value exceeded threshold', 'entity replaced', 'certification expired', 'methodology updated', 'status flag changed', 'reference document updated', 'batch recalled', etc.",
        min_length=5,
        max_length=50
    )
    description: str = Field(
        ...,
        description="One-sentence human-readable description of the change",
        min_length=20,
        max_length=300
    )
    affected_entity: str = Field(
        ...,
        description="Specific entity that changed (e.g., 'Raw material X', 'Supplier Y', 'Batch #123')",
        max_length=200
    )
    confidence: str = Field(
        default="high",
        description="Extraction confidence",
        pattern="^(high|medium|low)$"
    )


class SectionPlacementOutput(BaseModel):
    """
    Structured output for section placement decisions.
    """
    model_config = {"extra": "forbid"}
    
    action: str = Field(
        ...,
        description="Required action",
        pattern="^(ADD_SECTION|UPDATE_SECTION|NO_ACTION)$"
    )
    target_section: Optional[str] = Field(
        None,
        description="Target section number (existing or proposed new)"
    )
    parent_section: Optional[str] = Field(
        None,
        description="Parent section for hierarchy"
    )
    reasoning: str = Field(
        ...,
        description="Detailed explanation of placement decision",
        min_length=30
    )


# ============================================================================
# Phase 2-7: DB Change Detection Models
# ============================================================================

class DBChangeRecord(BaseModel):
    """
    Raw change record from ProductChangeLog table.
    Maps directly to SQL Server table structure.
    """
    model_config = {
        "extra": "forbid",
        "json_schema_extra": {
            "example": {
                "change_log_id": 42,
                "product_code": "BP_CREAM_001",
                "change_timestamp": "2026-03-15T10:30:00Z",
                "source_table": "RawMaterialAllergens",
                "operation_type": "INSERT",
                "column_name": "AllergenName",
                "old_value": None,
                "new_value": "Linalool",
                "changed_by": "john.doe"
            }
        }
    }
    
    change_log_id: int = Field(..., description="Unique change log ID")
    product_code: str = Field(..., description="Product identifier")
    change_timestamp: str = Field(..., description="When change occurred (ISO format)")
    source_table: str = Field(..., description="Table where change occurred")
    operation_type: str = Field(
        ...,
        description="Type of operation: INSERT, UPDATE, DELETE",
        pattern="^(INSERT|UPDATE|DELETE)$"
    )
    column_name: str = Field(..., description="Column that changed")
    old_value: Optional[str] = Field(None, description="Previous value (NULL for INSERT)")
    new_value: Optional[str] = Field(None, description="New value (NULL for DELETE)")
    changed_by: Optional[str] = Field(None, description="User who made the change")


class ChangeBundle(BaseModel):
    """
    Collection of related changes for a single product.
    Groups changes by product_code for batch processing.
    """
    model_config = {"extra": "forbid"}
    
    product_code: str = Field(..., description="Product identifier")
    changes: List[DBChangeRecord] = Field(
        ...,
        description="All changes for this product",
        min_length=1
    )
    detected_at: str = Field(..., description="When bundle was created (ISO format)")
    
    def get_affected_tables(self) -> List[str]:
        """Get unique list of tables affected in this bundle."""
        return list(set(change.source_table for change in self.changes))
    
    def get_change_count(self) -> int:
        """Get total number of changes in bundle."""
        return len(self.changes)


# ============================================================================
# Phase 4-7: Section Impact & Update Planning Models
# ============================================================================

class ImpactedSection(BaseModel):
    """
    Section affected by concept changes (Phase 4 output).
    Links concept changes to specific dossier sections.
    """
    model_config = {"extra": "forbid"}
    
    section_id: str = Field(..., description="Full section identifier")
    section_number: str = Field(..., description="Section number (e.g., '2.2.2.1')")
    title: str = Field(..., description="Section title")
    dossier_id: str = Field(..., description="Dossier identifier")
    product_code: str = Field(..., description="Product code")
    
    # Current semantic profile
    current_semantic_description: str = Field(..., description="Current situation description")
    current_semantic_embedding: List[float] = Field(..., description="Current situation vector")
    current_domain_concepts: List[str] = Field(default_factory=list, description="Current concepts")
    
    # Related changes
    related_concept_changes: List[ConceptChangeOutput] = Field(
        ...,
        description="Concept changes affecting this section"
    )
    
    # Mapping confidence
    mapping_confidence: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence that this section is affected"
    )


class SectionUpdatePlan(BaseModel):
    """
    Complete update plan for one section (Phase 7 output).
    Ready for generation stage.
    """
    model_config = {"extra": "forbid"}
    
    # Target section
    section_id: str = Field(..., description="Section identifier")
    section_number: str = Field(..., description="Section number")
    title: str = Field(..., description="Section title")
    product_code: str = Field(..., description="Product code")
    dossier_id: str = Field(..., description="Dossier identifier")
    
    # Status determines pipeline behavior
    status: str = Field(
        ...,
        description="Plan status",
        pattern="^(READY_FOR_GENERATION|PENDING_MANUAL_TEMPLATE)$"
    )
    
    # Pattern analysis (Phase 5)
    pattern_change_type: str = Field(
        ...,
        description="Pattern decision",
        pattern="^(SAME_PATTERN|NEW_PATTERN)$"
    )
    pattern_reasoning: str = Field(..., description="LLM explanation for pattern decision")
    
    # Situation analysis
    old_semantic_description: str = Field(..., description="Current situation")
    new_semantic_description: str = Field(..., description="Inferred new situation after changes")
    
    # Reference template source (Phase 6)
    reference_source: str = Field(
        ...,
        description="Where reference came from",
        pattern="^(CURRENT_SECTION|CROSS_DOSSIER|NOT_FOUND)$"
    )
    reference_section_id: Optional[str] = Field(None, description="Reference section ID if cross-dossier")
    reference_product_code: Optional[str] = Field(None, description="Reference product if cross-dossier")
    reference_section_number: Optional[str] = Field(None, description="Reference section number")
    reference_full_text: str = Field(..., description="Template text")
    reference_content_format: str = Field(..., description="Format style (table, bullets, etc.)")
    
    # Hierarchy context
    parent_section_number: Optional[str] = Field(None, description="Parent section")
    sibling_sections: List[Dict[str, str]] = Field(default_factory=list, description="Sibling sections")
    
    # What changed
    concept_changes: List[ConceptChangeOutput] = Field(..., description="Triggering changes")
    
    # Confidence
    overall_confidence: str = Field(
        ...,
        description="Overall plan confidence",
        pattern="^(high|medium|low)$"
    )


class NewSituationOutput(BaseModel):
    """
    Structured output for inferring new situation after changes.
    Used by SectionSituationAnalyzer.
    """
    model_config = {"extra": "forbid"}
    
    new_situation: str = Field(
        ...,
        description="Description of new situation after changes (2-3 sentences)",
        min_length=50,
        max_length=500
    )
