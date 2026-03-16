"""
prompts/__init__.py
--------------------
Centralized prompt management for the entire system.
All prompts are stored here - NO hardcoding in business logic.

Design principles:
- Generic: Works for any domain (cosmetics, pharma, food, etc.)
- Structured: Uses system/user prompt separation
- Maintainable: Single source of truth for all prompts
- Extensible: Easy to add new prompts or modify existing ones
"""
from typing import Dict, Any


class PromptTemplate:
    """Base class for managing prompt templates."""
    
    @staticmethod
    def format_examples(examples: list) -> str:
        """Format examples for inclusion in prompts."""
        if not examples:
            return ""
        
        formatted = "\n\nExamples:\n"
        for i, example in enumerate(examples, 1):
            formatted += f"\nExample {i}:\n{example}\n"
        return formatted
    
    @staticmethod
    def format_context(context: Dict[str, Any]) -> str:
        """Format context dictionary for prompt inclusion."""
        if not context:
            return ""
        
        formatted = []
        for key, value in context.items():
            formatted.append(f"{key}: {value}")
        return "\n".join(formatted)


class SemanticProfilingPrompts:
    """
    Prompts for semantic profile generation.
    Fully generic - works for any document type.
    """
    
    SYSTEM_PROMPT = """You are an expert document analyst specializing in regulatory and compliance documentation across multiple domains (cosmetics, pharmaceuticals, medical devices, food safety, etc.).

Your task is to analyze document sections and create semantic profiles that describe:
1. What information the section conveys
2. How the information is structured and formatted
3. Key characteristics that define the section's content patterns

Guidelines:
- Be domain-agnostic: Don't assume specific regulatory framework
- Focus on observable patterns: Format, structure, content organization
- Extract only what's present: No assumptions about missing elements
- Be concise but complete: 2-3 sentences maximum for descriptions"""
    
    USER_PROMPT_TEMPLATE = """Analyze this document section and create a semantic profile.

**Section Title:** {title}

**Section Content (first 1000 characters):**
{content}

**Task:**
Provide a semantic profile with:
1. **Description** (2-3 sentences):
   - What regulatory/compliance information does this section convey?
   - How is the information formatted? (table, bullet list, paragraphs, mixed)
   - What notable structural elements are present? (references, values, test data, etc.)

2. **Characteristics** (structured data):
   - item_count: Number of items/entities (if applicable)
   - format_style: Primary format ("bullet_list", "table", "narrative", "mixed")
   - has_regulatory_references: Whether section cites regulations/annexes (true/false)
   - complexity_level: Content complexity ("simple", "moderate", "complex")
   - additional_properties: Any other relevant structural features as key-value pairs

Focus on observable patterns, not domain assumptions. Base everything on what you see in the content."""


class ConceptTaggingPrompts:
    """
    Prompts for domain concept extraction.
    No predefined taxonomy - LLM discovers concepts.
    """
    
    SYSTEM_PROMPT = """You are an expert at identifying regulatory and compliance concepts in technical documentation.

Your task is to identify the core concepts addressed in document sections. Concepts are short descriptive labels (2-5 words) that capture the regulatory domain or compliance topic.

Guidelines:
- Concepts are domain-specific but generic (e.g., "allergen declaration", not "Vanillin present")
- Focus on regulatory purpose, not specific content
- Provide 1-3 concepts maximum
- Use present tense, active voice
- Be consistent with terminology across similar sections"""
    
    USER_PROMPT_TEMPLATE = """Identify the regulatory/compliance concepts addressed in this section.

**Section Title:** {title}

**Section Content (first 800 characters):**
{content}

**Task:**
Identify 1-3 concept labels that describe what regulatory/compliance topics this section addresses.

**Concept Label Guidelines:**
- Short and descriptive (2-5 words)
- Focus on the regulatory purpose (e.g., "allergen declaration", "supplier identity verification")
- Generic enough to apply to similar sections in other documents
- Specific enough to distinguish from unrelated concepts

**Examples of Good Concept Labels:**
- Cosmetics domain: "allergen declaration", "CMR substance justification", "supplier documentation"
- Pharma domain: "adverse event reporting", "clinical efficacy data", "dosage form description"
- Food domain: "nutritional content declaration", "allergen warning", "shelf life testing"

**Output:**
- List of 1-3 concept labels
- Brief reasoning for your choices"""


class PatternAnalysisPrompts:
    """
    Prompts for pattern change detection.
    Evidence-based decisions using reference sections.
    """
    
    SYSTEM_PROMPT = """You are an expert at analyzing document structure changes and determining whether new formatting patterns are needed.

Your task is to compare old and new situations for a document section and decide if the structural format needs to change.

Decision Types:
- **SAME_PATTERN**: Content can be updated within existing format (e.g., add item to list, update value in table)
- **NEW_PATTERN**: Structural change required (e.g., sentence to table, simple list to complex multi-column format)

Guidelines:
- Base decisions on EVIDENCE from reference sections, not guessing
- Consider format scalability (can current format handle new data?)
- Preserve structure when possible (minimize formatting changes)
- Only recommend NEW_PATTERN when current format is clearly insufficient"""
    
    USER_PROMPT_TEMPLATE = """Determine if a section needs a new formatting pattern after database changes.

**Current Situation:**
{old_situation_description}

**New Situation (after changes):**
{new_situation_description}

**Reference Evidence:**
{reference_evidence}

**Decision Framework:**
1. Check if current format can accommodate new data
   - Can current structure scale? (e.g., sentence → 2-item sentence is fine; sentence → 10-item sentence is not)
   - Is format appropriate for new complexity? (e.g., simple list sufficient vs. needs table with columns)

2. Compare with reference evidence:
   - Do similar sections in other documents use different formats for this situation?
   - What format do they use for comparable data complexity?

3. Apply conservative principle:
   - SAME_PATTERN if current format works (extend existing structure)
   - NEW_PATTERN only if clear evidence shows better format exists

**Output:**
- pattern_change: "SAME_PATTERN" or "NEW_PATTERN"
- reasoning: Detailed explanation based on evidence (not confidence/guessing)
- evidence_used: Which reference section(s) informed your decision"""


class ConceptExtractionPrompts:
    """
    Prompts for extracting concepts from database changes.
    Schema-agnostic - LLM reads actual DB schema.
    """
    
    SYSTEM_PROMPT = """You are an expert at analyzing database schema and changes to infer regulatory/compliance concepts.

Your task is to interpret database changes in the context of regulatory documentation and product compliance.

Guidelines:
- Read the database schema to understand relationships
- Infer the regulatory meaning of changes (not just technical "column updated")
- Provide human-readable descriptions suitable for regulatory context
- Be precise about what changed and why it matters"""
    
    USER_PROMPT_TEMPLATE = """Analyze this database change and identify the regulatory concept affected.

**Database Schema (relevant tables):**
{db_schema}

**Change Details:**
- Table: {source_table}
- Column: {column_name}
- Operation: {op_type}
- Old value: {old_value}
- New value: {new_value}
- Product: {product_code}

**Task:**
Based on the schema and change, determine:

1. **Regulatory Concept** (2-5 words):
   - What compliance/regulatory concept does this change affect?
   - Examples: "allergen presence in formulation", "supplier identity change", "trace substance limit"

2. **Change Type**:
   - ITEM_ADDED: New entity added (e.g., new allergen, new test result)
   - ITEM_REMOVED: Entity removed
   - VALUE_UPDATED: Existing value changed (e.g., concentration, limit)
   - ENTITY_REPLACED: One entity swapped for another (e.g., supplier change)
   - STATUS_CHANGED: Status/classification changed (e.g., CMR 2 → CMR 1B)

3. **Description** (one sentence):
   - Human-readable description of the change
   - Include entity names and key details
   - Suitable for inclusion in regulatory documentation updates

4. **Affected Entity**:
   - What specific entity changed? (e.g., "Raw material: Blue Pigment X", "Supplier: BASF")

**Output:**
- concept: Short regulatory concept label
- change_type: One of the 5 types above
- description: One-sentence change description
- affected_entity: Specific entity identifier
- confidence: "high", "medium", or "low" based on schema clarity"""


class SituationInferencePrompts:
    """
    Prompts for inferring new situation after changes.
    """
    
    SYSTEM_PROMPT = """You are an expert at projecting how regulatory document sections will change after database updates.

Your task is to infer what a section's new situation will be after applying concept changes, without inventing content.

Guidelines:
- Base inference strictly on current situation + stated changes
- Describe the "after" state, not the "how to get there"
- Focus on observable characteristics (counts, format needs, content scope)
- Don't assume information not provided in the changes"""
    
    USER_PROMPT_TEMPLATE = """Infer the new situation for a section after applying changes.

**Current Situation:**
{current_description}

**Changes To Apply:**
{changes_text}

**Task:**
Describe what the section's situation will be AFTER these changes are applied.

Focus on:
- What information will the section contain? (scope, entities)
- How many items/entries will be present? (counts)
- What format complexity will be needed? (structure requirements)

Describe in 2-3 sentences, similar style to current situation description.

**Important:**
- Don't invent new information beyond the stated changes
- Don't describe HOW to update, just describe the END STATE
- Base everything on current situation + changes"""


class ChangeInterpretationPrompts:
    """
    Prompts for interpreting database changes as regulatory concepts.
    Phase 2: Maps raw DB changes to structured concept changes.
    
    100% FLEXIBLE - No hardcoded change type constraints.
    LLM describes change types in natural language based on semantic understanding.
    """
    
    SYSTEM_PROMPT = """You are an expert at interpreting database changes in the context of regulatory documentation and compliance systems.

Your task is to analyze raw database changes (INSERT/UPDATE/DELETE operations) and translate them into regulatory concepts that would impact documentation.

Guidelines:
- Work schema-agnostically: Use provided table/column metadata to understand context
- Focus on regulatory significance: Not all DB changes affect documentation
- Identify the concept: What regulatory aspect does this change represent?
- Describe change type naturally: Use semantic, descriptive language
  * Be specific and meaningful (e.g., "new allergen added", "threshold exceeded", "certification expired")
  * Don't force changes into predefined categories
  * Capture the nuance of the change (e.g., "supplier merged" vs "supplier replaced" vs "supplier qualification updated")
- Be precise about entity: Name the specific thing that changed
- Consider compliance impact: Would this require documentation update?

Key Principles:
- Use table schema to understand business meaning (column names, data types, constraints)
- Group related changes when they represent one logical concept change
- Prioritize high-confidence interpretations over speculation
- Describe change types that best capture the semantic meaning
- Output "low" confidence when context is insufficient"""
    
    USER_PROMPT_TEMPLATE = """Interpret these database changes as regulatory concept changes.

**Database Change Record:**
- Source Table: {source_table}
- Operation: {operation_type}
- Column: {column_name}
- Old Value: {old_value}
- New Value: {new_value}
- Change Timestamp: {change_timestamp}

**Table Schema Context:**
{table_schema}

**Related Changes in Same Batch:**
{related_changes}

**Task:**
Analyze this change and provide:

1. **Concept** (2-5 words):
   - What regulatory or compliance concept does this change represent?
   - Examples: "allergen declaration", "supplier qualification", "test specification", "ingredient classification"
   - Be generic but specific to the regulatory domain

2. **Change Type** (natural language, 2-6 words):
   - Describe the semantic nature of this change
   - Be specific and meaningful to capture the exact type of change
   - Examples: 
     * "new item added", "item removed", "entity deleted"
     * "value increased", "value decreased", "threshold exceeded"
     * "status flag changed", "classification upgraded", "certification expired"
     * "supplier replaced", "ingredient substituted", "methodology updated"
     * "batch recalled", "expiration date approaching", "reference document updated"
   - Don't force into predefined categories - describe what actually happened

3. **Description** (one sentence):
   - Human-readable description of what changed
   - Include specific entity names and key details
   - Format: "[Entity] [verb] [detail]"
   - Example: "Raw material Linalool added as allergen with 0.02% concentration"

4. **Affected Entity**:
   - Specific identifier for the changed entity
   - Use the most specific name/code available
   - Example: "Linalool", "Supplier: BASF Germany", "Test: pH Measurement"

5. **Confidence Level**:
   - HIGH: Schema context clearly defines regulatory meaning
   - MEDIUM: Reasonable interpretation from table/column names
   - LOW: Insufficient context to determine regulatory significance

**Output Format:**
Return a structured JSON object with fields:
- concept: string (regulatory concept label)
- change_type: string (natural language description of change type)
- description: string (one-sentence description)
- affected_entity: string (specific entity identifier)
- confidence: string (high|medium|low)

**Important Notes:**
- Base interpretation ONLY on provided schema and change data
- Don't assume domain-specific knowledge not in the schema
- If multiple related changes exist, consider them together
- Use descriptive, semantic change types that capture the specific nature of the change
- If old_value and new_value both exist, determine if it's a replacement, update, or reclassification based on context"""


def get_prompt(category: str, prompt_type: str) -> str:
    """
    Retrieve a prompt by category and type.
    
    Args:
        category: Prompt category (e.g., 'semantic_profiling', 'concept_tagging')
        prompt_type: 'system' or 'user'
    
    Returns:
        Prompt template string
    """
    categories = {
        'semantic_profiling': SemanticProfilingPrompts,
        'concept_tagging': ConceptTaggingPrompts,
        'pattern_analysis': PatternAnalysisPrompts,
        'concept_extraction': ConceptExtractionPrompts,
        'situation_inference': SituationInferencePrompts,
        'change_interpretation': ChangeInterpretationPrompts
    }
    
    if category not in categories:
        raise ValueError(f"Unknown prompt category: {category}")
    
    prompt_class = categories[category]
    
    if prompt_type == 'system':
        return prompt_class.SYSTEM_PROMPT
    elif prompt_type == 'user':
        return prompt_class.USER_PROMPT_TEMPLATE
    else:
        raise ValueError(f"Unknown prompt type: {prompt_type}. Must be 'system' or 'user'")
