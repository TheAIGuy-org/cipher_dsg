"""
Phase 3: Change Interpreter Service
====================================

Interprets raw database changes into regulatory concepts.
Maps low-level DB operations → high-level regulatory concepts.

Examples:
- RawMaterialAllergens INSERT → "allergen declaration" concept (NEW)
- RawMaterialTraces UPDATE Classification → "CMR reclassification" (RECLASSIFIED)
- Products UPDATE statement → "regulatory statement update" (MODIFIED)
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from parsers.models import (
    DBChangeRecord, 
    ChangeBundle, 
    ConceptChangeOutput
)
from db.sql_client import SQLServerClient, get_sql_client
from llm.azure_client import AzureLLMClient, get_llm_client
from prompts import get_prompt
from utils.logger import get_logger

log = get_logger(__name__)


class ChangeInterpreter:
    """
    Interprets database changes into regulatory concepts.
    
    Core responsibility: Transform technical DB changes into business/regulatory
    concepts that can be mapped to dossier sections.
    
    Architecture:
    - Takes ChangeBundle (grouped by product)
    - Calls LLM with table schema context
    - Returns structured ConceptChangeOutput per change
    - Handles batch interpretation for efficiency
    """
    
    def __init__(
        self, 
        sql_client: Optional[SQLServerClient] = None,
        azure_client: Optional[AzureLLMClient] = None
    ):
        """
        Initialize change interpreter.
        
        Args:
            sql_client: SQL client for schema introspection (optional, will use singleton)
            azure_client: Azure OpenAI client (optional, will use singleton)
        """
        self.sql_client = sql_client or get_sql_client()
        self.azure_client = azure_client or get_llm_client()
        
        # Cache table schemas to avoid repeated queries
        self._schema_cache: Dict[str, str] = {}
        
        log.info("ChangeInterpreter initialized")
    
    def interpret_bundle(
        self, 
        bundle: ChangeBundle,
        include_related_context: bool = True
    ) -> List[ConceptChangeOutput]:
        """
        Interpret all changes in a bundle.
        
        CRITICAL: Groups changes by table+row to interpret them together.
        Example: INSERT into RawMaterialTraces creates 3 changes (SubstanceName, 
        Classification, MaxLevelPPM) but they should be ONE concept!
        
        Args:
            bundle: ChangeBundle with changes for a single product
            include_related_context: Whether to pass related changes as context
        
        Returns:
            List of ConceptChangeOutput, one per LOGICAL change (may be multiple DB columns)
        """
        log.info(
            f"Interpreting bundle for product {bundle.product_code}: "
            f"{len(bundle.changes)} changes"
        )
        
        # STEP 1: Group changes by table + operation + timestamp (same "logical" change)
        change_groups = self._group_related_changes(bundle.changes)
        
        log.info(f"Grouped {len(bundle.changes)} DB changes into {len(change_groups)} logical changes")
        
        concepts = []
        
        for group_idx, (group_key, group_changes) in enumerate(change_groups.items()):
            try:
                # Build related changes context (from OTHER groups)
                related_changes = None
                if include_related_context and len(change_groups) > 1:
                    other_groups = {k: v for k, v in change_groups.items() if k != group_key}
                    related_changes = self._build_group_context(other_groups)
                
                # Interpret group of related changes as ONE concept
                concept = self.interpret_change_group(
                    changes=group_changes,
                    related_changes=related_changes
                )
                
                concepts.append(concept)
                
                log.debug(
                    f"Interpreted change {group_idx+1}/{len(change_groups)}: "
                    f"{concept.concept} ({concept.change_type})"
                )
            
            except Exception as e:
                first_change = group_changes[0]  # Get representative change for error logging
                log.error(
                    f"Failed to interpret change group {first_change.change_log_id}: {e}",
                    exc_info=True
                )
                # Create fallback concept with low confidence
                concepts.append(
                    ConceptChangeOutput(
                        concept=f"database change in {first_change.source_table}",
                        change_type="technical modification",
                        description=f"{first_change.operation_type} operation on {first_change.column_name} in {first_change.source_table}",
                        affected_entity=f"Product {first_change.product_code}",
                        confidence="low"
                    )
                )
        
        log.info(f"Interpreted {len(concepts)} concepts from bundle")
        return concepts
    
    def interpret_change(
        self,
        change: DBChangeRecord,
        related_changes: Optional[str] = None
    ) -> ConceptChangeOutput:
        """
        Interpret a single database change into a regulatory concept.
        
        Args:
            change: Single database change record
            related_changes: Context about related changes (optional)
        
        Returns:
            Structured concept interpretation
        """
        log.debug(
            f"Interpreting change: {change.source_table}.{change.column_name} "
            f"({change.operation_type})"
        )
        
        # Get table schema for LLM context
        schema = self._get_table_schema(change.source_table)
        
        # Build prompt
        system_prompt = get_prompt('change_interpretation', 'system')
        user_prompt_template = get_prompt('change_interpretation', 'user')
        
        user_prompt = user_prompt_template.format(
            source_table=change.source_table,
            operation_type=change.operation_type,
            column_name=change.column_name,
            old_value=change.old_value or 'NULL',
            new_value=change.new_value or 'NULL',
            change_timestamp=change.change_timestamp,
            table_schema=schema,
            related_changes=related_changes or 'None (single change)'
        )
        
        # Call LLM with structured output
        try:
            result = self.azure_client.ask_structured_pydantic(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=ConceptChangeOutput
            )
            
            log.debug(
                f"LLM interpretation: {result.concept} "
                f"({result.change_type}, confidence={result.confidence})"
            )
            
            return result
        
        except Exception as e:
            log.error(f"LLM interpretation failed: {e}", exc_info=True)
            raise
    
    def _get_table_schema(self, table_name: str) -> str:
        """
        Get table schema as formatted string for LLM context.
        
        Uses caching to avoid repeated SQL queries.
        
        Args:
            table_name: Name of the table
        
        Returns:
            Formatted schema string
        """
        # Check cache
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]
        
        # Query schema
        try:
            if not self.sql_client.is_connected():
                self.sql_client.connect()
            
            # Get CREATE TABLE statement
            schema = self.sql_client.get_table_schema(table_name)
            
            # Cache for future use
            self._schema_cache[table_name] = schema
            
            log.debug(f"Cached schema for {table_name}: {len(schema)} chars")
            return schema
        
        except Exception as e:
            log.error(f"Failed to get schema for {table_name}: {e}")
            # Return minimal schema
            return f"TABLE {table_name} (schema unavailable)"
    
    def _group_related_changes(
        self, 
        changes: List[DBChangeRecord]
    ) -> Dict[str, List[DBChangeRecord]]:
        """
        Group changes that represent the same logical operation.
        
        Example: INSERT into RawMaterialTraces creates 3 change records:
        - SubstanceName: NULL → Lead
        - Classification: NULL → Heavy Metal  
        - MaxLevelPPM: NULL → 0.001
        
        These should be interpreted TOGETHER as one concept: "heavy metal monitoring"
        not separately as three concepts.
        
        Grouping key: table_name + operation_type + timestamp (within 2 seconds)
        
        Args:
            changes: List of change records
        
        Returns:
            Dict mapping group_key → List[DBChangeRecord]
        """
        from collections import defaultdict
        from datetime import datetime
        
        groups = defaultdict(list)
        
        for change in changes:
            # Parse timestamp
            try:
                ts = datetime.fromisoformat(change.change_timestamp.replace('Z', '+00:00'))
                ts_bucket = int(ts.timestamp() / 2)  # 2-second buckets
            except:
                ts_bucket = 0
            
            # Group key: table + operation + time bucket
            # Changes within 2 seconds on same table are likely the same row
            group_key = f"{change.source_table}_{change.operation_type}_{ts_bucket}"
            groups[group_key].append(change)
        
        log.debug(f"Grouped changes: {dict((k, len(v)) for k, v in groups.items())}")
        return dict(groups)
    
    def interpret_change_group(
        self,
        changes: List[DBChangeRecord],
        related_changes: Optional[str] = None
    ) -> ConceptChangeOutput:
        """
        Interpret a GROUP of related changes as ONE regulatory concept.
        
        This is the key fix: instead of interpreting SubstanceName, Classification,
        MaxLevelPPM separately, we interpret them TOGETHER to understand the full
        semantic meaning (e.g., "heavy metal monitoring").
        
        Args:
            changes: List of related change records (same row/operation)
            related_changes: Context about OTHER change groups (optional)
        
        Returns:
            Single ConceptChangeOutput representing the full semantic change
        """
        log.debug(f"Interpreting group of {len(changes)} related changes")
        
        # Use first change for metadata
        primary_change = changes[0]
        
        # Get table schema
        schema = self._get_table_schema(primary_change.source_table)
        
        # Build ALL column changes for this operation
        column_changes = []
        for change in changes:
            column_changes.append({
                'column': change.column_name,
                'old_value': change.old_value or 'NULL',
                'new_value': change.new_value or 'NULL'
            })
        
        # Format for LLM
        columns_summary = "\n".join([
            f"  • {c['column']}: {c['old_value']} → {c['new_value']}"
            for c in column_changes
        ])
        
        # Build prompt
        system_prompt = get_prompt('change_interpretation', 'system')
        
        # Enhanced user prompt with ALL columns
        user_prompt = f"""
DATABASE CHANGE DETECTED:

Table: {primary_change.source_table}
Operation: {primary_change.operation_type}
Timestamp: {primary_change.change_timestamp}

COLUMN CHANGES (interpret these TOGETHER as ONE semantic change):
{columns_summary}

TABLE SCHEMA:
{schema}

RELATED CHANGES IN THIS BUNDLE:
{related_changes or 'None (single change group)'}

CRITICAL INSTRUCTION:
Look at ALL column values together to understand the FULL semantic meaning.
Example: If SubstanceName='Lead' AND Classification='Heavy Metal', 
the concept is "heavy metal monitoring", NOT just "substance traceability"!

Use the Classification/Type columns to identify SPECIFIC regulatory concepts:
- If Classification contains "Heavy Metal" → concept should be "heavy metal content"
- If Classification contains "CMR" → concept should be "CMR substance presence"  
- If Classification contains "Allergen" → concept should be "allergen declaration"
- Use the MOST SPECIFIC concept that applies!

Extract the regulatory concept that this change represents.
"""
        
        # Call LLM with structured output
        try:
            result = self.azure_client.ask_structured_pydantic(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_model=ConceptChangeOutput
            )
            
            log.debug(
                f"LLM interpretation (group): {result.concept} "
                f"({result.change_type}, confidence={result.confidence})"
            )
            
            return result
        
        except Exception as e:
            log.error(f"LLM interpretation failed: {e}", exc_info=True)
            raise
    
    def _build_group_context(
        self,
        other_groups: Dict[str, List[DBChangeRecord]]
    ) -> str:
        """
        Build context string about other change groups in the bundle.
        
        Args:
            other_groups: Dict of other change groups
        
        Returns:
            Formatted string describing other changes
        """
        if not other_groups:
            return "None (single change group)"
        
        summaries = []
        for group_key, group_changes in list(other_groups.items())[:5]:
            table = group_changes[0].source_table
            op = group_changes[0].operation_type
            columns = ', '.join([c.column_name for c in group_changes])
            summaries.append(f"- {table} {op}: {columns}")
        
        if len(other_groups) > 5:
            summaries.append(f"... and {len(other_groups) - 5} more operations")
        
        return "\n".join(summaries)
    
    def _build_related_context(
        self, 
        bundle: ChangeBundle, 
        current_index: int
    ) -> str:
        """
        Build context string about related changes in the bundle.
        
        Helps LLM understand broader context (e.g., multiple allergens added
        at once, or combined statement updates).
        
        Args:
            bundle: Full change bundle
            current_index: Index of current change being interpreted
        
        Returns:
            Formatted string describing related changes
        """
        other_changes = [
            c for i, c in enumerate(bundle.changes) 
            if i != current_index
        ]
        
        if not other_changes:
            return "None (single change)"
        
        # Build concise summary
        summaries = []
        for change in other_changes[:5]:  # Limit to 5 most relevant
            summary = (
                f"- {change.source_table}.{change.column_name}: "
                f"{change.operation_type}"
            )
            if change.new_value and len(change.new_value) < 50:
                summary += f" → {change.new_value}"
            summaries.append(summary)
        
        if len(other_changes) > 5:
            summaries.append(f"... and {len(other_changes) - 5} more changes")
        
        return "\n".join(summaries)
    
    def clear_schema_cache(self):
        """Clear cached table schemas."""
        self._schema_cache.clear()
        log.debug("Schema cache cleared")


# Singleton pattern
_interpreter_instance: Optional[ChangeInterpreter] = None


def get_change_interpreter(
    sql_client: Optional[SQLServerClient] = None,
    azure_client: Optional[AzureLLMClient] = None
) -> ChangeInterpreter:
    """
    Get singleton ChangeInterpreter instance.
    
    Args:
        sql_client: Optional SQL client (uses singleton if not provided)
        azure_client: Optional Azure client (uses singleton if not provided)
    
    Returns:
        ChangeInterpreter instance
    """
    global _interpreter_instance
    
    if _interpreter_instance is None:
        _interpreter_instance = ChangeInterpreter(
            sql_client=sql_client,
            azure_client=azure_client
        )
    
    return _interpreter_instance
