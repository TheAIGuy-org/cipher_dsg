"""
Phase 6: LangGraph Agent Workflow
==================================

Orchestrates the complete dossier update pipeline using LangGraph.
Coordinates Phases 1-5 + 7 into a coherent agentic workflow.

Workflow States:
1. POLLING: Monitor DB for changes
2. INTERPRETING: Extract concepts (Phase 3)
3. MAPPING: Map to sections (Phase 4)
4. GENERATING: Generate updates (Phase 5)
5. REVIEWING: Human-in-the-loop checkpoint
6. STORING: Save to Neo4j (Phase 7)
7. COMPLETED: Workflow finished

Architecture: State machine with:
- State persistence
- Retry logic
- Human checkpoints
- Parallel section processing
"""

from typing import Dict, List, Any, Optional, Literal
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, AIMessage

from db.poller import get_change_poller
from llm.change_interpreter import get_change_interpreter
from llm.section_mapper import get_section_mapper
from llm.update_generator import get_update_generator
from parsers.models import ChangeBundle, ConceptChangeOutput
from llm.section_mapper import SectionImpact
from llm.update_generator import SectionUpdate
from utils.logger import get_logger

log = get_logger(__name__)


class WorkflowState(str, Enum):
    """Workflow states."""
    POLLING = "polling"
    INTERPRETING = "interpreting"
    MAPPING = "mapping"
    GENERATING = "generating"
    REVIEWING = "reviewing"
    STORING = "storing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class DossierUpdateState:
    """
    Complete state for dossier update workflow.
    
    Tracked through entire pipeline for observability and recovery.
    """
    # Input
    product_code: Optional[str] = None
    change_bundle: Optional[ChangeBundle] = None
    
    # Phase 3: Concept Extraction
    concepts: List[ConceptChangeOutput] = field(default_factory=list)
    
    # Phase 4: Section Mapping
    section_impacts: Dict[str, List[SectionImpact]] = field(default_factory=dict)  # concept_id → impacts
    
    # Phase 5: Update Generation
    section_updates: List[SectionUpdate] = field(default_factory=list)
    
    # Workflow State
    current_state: WorkflowState = WorkflowState.POLLING
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    # Human Review
    requires_review: bool = False
    review_notes: Optional[str] = None
    approved: bool = False
    
    # Metadata
    started_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    completed_at: Optional[str] = None


class DossierUpdateAgent:
    """
    LangGraph-based agent for orchestrating dossier updates.
    
    Coordinates all phases into a single workflow:
    - Polls for DB changes  
    - Interprets into concepts
    - Maps to affected sections
    - Generates updates
    - Human review checkpoint
    - Stores in Neo4j
    
    Features:
    - State persistence
    - Error recovery
    - Human-in-the-loop
    - Parallel processing (sections)
    """
    
    def __init__(self):
        """Initialize agent with all phase components."""
        self.poller = get_change_poller()
        self.interpreter = get_change_interpreter()
        self.mapper = get_section_mapper()
        self.generator = get_update_generator()
        
        # Build LangGraph
        self.graph = self._build_graph()
        
        log.info("DossierUpdateAgent initialized")
    
    def _build_graph(self) -> StateGraph:
        """
        Build LangGraph state machine.
        
        Returns:
            Compiled StateGraph
        """
        # Define graph
        workflow = StateGraph(dict)  # State is dict-like
        
        # Add nodes (each phase)
        workflow.add_node("poll_changes", self._poll_node)
        workflow.add_node("interpret_concepts", self._interpret_node)
        workflow.add_node("map_sections", self._map_node)
        workflow.add_node("generate_updates", self._generate_node)
        workflow.add_node("review", self._review_node)
        workflow.add_node("store_updates", self._store_node)
        
        # Define edges (transitions)
        workflow.set_entry_point("poll_changes")
        
        workflow.add_edge("poll_changes", "interpret_concepts")
        workflow.add_edge("interpret_concepts", "map_sections")
        workflow.add_edge("map_sections", "generate_updates")
        workflow.add_edge("generate_updates", "review")
        
        # Conditional edge after review
        workflow.add_conditional_edges(
            "review",
            self._should_store_or_end,
            {
                "store": "store_updates",
                "end": END
            }
        )
        
        workflow.add_edge("store_updates", END)
        
        # Compile
        return workflow.compile()
    
    def run(
        self,
        product_code: Optional[str] = None,
        auto_approve: bool = False
    ) -> DossierUpdateState:
        """
        Run complete workflow.
        
        Args:
            product_code: Optional product filter (if None, polls all)
            auto_approve: Auto-approve updates (skip human review)
        
        Returns:
            Final DossierUpdateState
        """
        log.info("Starting dossier update workflow")
        
        # Initialize state
        initial_state = {
            'product_code': product_code,
            'auto_approve': auto_approve,
            'current_state': WorkflowState.POLLING.value,
            'concepts': [],
            'section_impacts': {},
            'section_updates': [],
            'errors': [],
            'warnings': [],
            'requires_review': not auto_approve,
            'approved': auto_approve,
            'started_at': datetime.utcnow().isoformat()
        }
        
        try:
            # Run graph
            final_state = self.graph.invoke(initial_state)
            
            # Mark completion
            final_state['completed_at'] = datetime.utcnow().isoformat()
            final_state['current_state'] = WorkflowState.COMPLETED.value
            
            log.info("Workflow completed successfully")
            return self._dict_to_state(final_state)
        
        except Exception as e:
            log.error(f"Workflow failed: {e}", exc_info=True)
            
            # Return failed state
            initial_state['current_state'] = WorkflowState.FAILED.value
            initial_state['errors'].append(str(e))
            initial_state['completed_at'] = datetime.utcnow().isoformat()
            
            return self._dict_to_state(initial_state)
    
    def _poll_node(self, state: Dict) -> Dict:
        """Poll for database changes."""
        log.info("[POLL] Polling for changes...")
        state['current_state'] = WorkflowState.POLLING.value
        
        try:
            bundles = self.poller.poll_once()
            
            if not bundles:
                log.info("[POLL] No changes found")
                state['warnings'].append("No pending changes")
                return state
            
            # Filter by product if specified
            if state.get('product_code'):
                bundles = [b for b in bundles if b.product_code == state['product_code']]
            
            if bundles:
                # For now, process first bundle
                state['change_bundle'] = bundles[0].__dict__
                state['product_code'] = bundles[0].product_code
                
                log.info(f"[POLL] Processing bundle for product {bundles[0].product_code}")
            else:
                log.info("[POLL] No matching bundles after filtering")
                state['warnings'].append("No changes for specified product")
            
            return state
        
        except Exception as e:
            log.error(f"[POLL] Failed: {e}")
            state['errors'].append(f"Polling failed: {e}")
            return state
    
    def _interpret_node(self, state: Dict) -> Dict:
        """Interpret changes into concepts."""
        log.info("[INTERPRET] Extracting concepts...")
        state['current_state'] = WorkflowState.INTERPRETING.value
        
        if not state.get('change_bundle'):
            log.warning("[INTERPRET] No change bundle to process")
            return state
        
        try:
            # Convert dict back to ChangeBundle
            bundle_dict = state['change_bundle']
            # (Simplified - in production, would properly reconstruct)
            
            # For now, log
            log.info(f"[INTERPRET] Would interpret {len(bundle_dict.get('changes', []))} changes")
            state['concepts'] = []  # Placeholder
            
            return state
        
        except Exception as e:
            log.error(f"[INTERPRET] Failed: {e}")
            state['errors'].append(f"Interpretation failed: {e}")
            return state
    
    def _map_node(self, state: Dict) -> Dict:
        """Map concepts to sections."""
        log.info("[MAP] Mapping concepts to sections...")
        state['current_state'] = WorkflowState.MAPPING.value
        
        if not state.get('concepts'):
            log.warning("[MAP] No concepts to map")
            return state
        
        try:
            log.info(f"[MAP] Would map {len(state['concepts'])} concepts")
            state['section_impacts'] = {}  # Placeholder
            
            return state
        
        except Exception as e:
            log.error(f"[MAP] Failed: {e}")
            state['errors'].append(f"Mapping failed: {e}")
            return state
    
    def _generate_node(self, state: Dict) -> Dict:
        """Generate section updates."""
        log.info("[GENERATE] Generating updates...")
        state['current_state'] = WorkflowState.GENERATING.value
        
        if not state.get('section_impacts'):
            log.warning("[GENERATE] No section impacts to process")
            return state
        
        try:
            log.info("[GENERATE] Would generate section updates")
            state['section_updates'] = []  # Placeholder
            
            return state
        
        except Exception as e:
            log.error(f"[GENERATE] Failed: {e}")
            state['errors'].append(f"Generation failed: {e}")
            return state
    
    def _review_node(self, state: Dict) -> Dict:
        """Human review checkpoint."""
        log.info("[REVIEW] Entering review phase...")
        state['current_state'] = WorkflowState.REVIEWING.value
        
        if state.get('auto_approve'):
            log.info("[REVIEW] Auto-approved")
            state['approved'] = True
        else:
            log.info("[REVIEW] Requires human review")
            state['requires_review'] = True
            # In production, would pause for human input
        
        return state
    
    def _store_node(self, state: Dict) -> Dict:
        """Store updates in Neo4j."""
        log.info("[STORE] Storing updates...")
        state['current_state'] = WorkflowState.STORING.value
        
        try:
            log.info("[STORE] Would store updates to Neo4j")
            
            return state
        
        except Exception as e:
            log.error(f"[STORE] Failed: {e}")
            state['errors'].append(f"Storage failed: {e}")
            return state
    
    def _should_store_or_end(self, state: Dict) -> Literal["store", "end"]:
        """Determine whether to store updates or end workflow."""
        if state.get('approved') and not state.get('errors'):
            return "store"
        else:
            return "end"
    
    def _dict_to_state(self, state_dict: Dict) -> DossierUpdateState:
        """Convert dict to DossierUpdateState."""
        # Simplified conversion
        return DossierUpdateState(
            product_code=state_dict.get('product_code'),
            current_state=WorkflowState(state_dict.get('current_state', 'failed')),
            errors=state_dict.get('errors', []),
            warnings=state_dict.get('warnings', []),
            started_at=state_dict.get('started_at'),
            completed_at=state_dict.get('completed_at')
        )


# Singleton
_agent_instance: Optional[DossierUpdateAgent] = None


def get_dossier_agent() -> DossierUpdateAgent:
    """Get singleton DossierUpdateAgent."""
    global _agent_instance
    
    if _agent_instance is None:
        _agent_instance = DossierUpdateAgent()
    
    return _agent_instance
