"""
Real-Time Dossier Update Agent
==============================

Continuously monitors SQL Server for changes and processes them automatically.
Shows detailed phase-by-phase progress in terminal with CONSOLIDATED output.

Usage:
    python run_agent_realtime.py

Then in another terminal, run SQL changes to test:
    sqlcmd -S IN-41DG1D4 -d Bayer -C -Q "INSERT INTO RawMaterialTraces ..."

The agent will automatically:
1. Detect the change (Phase 2)
2-7. Process through complete pipeline
8. Return SINGLE CONSOLIDATED PLAN with correct hierarchy

Press Ctrl+C to stop.
"""

import sys
import time
import signal
from datetime import datetime
from typing import Optional

from db.sql_client import get_sql_client
from db.poller import get_change_poller
from db.change_pipeline import ChangeDetectionPipeline
from llm.content_generator import SectionContentGenerator
from db.dossier_injector import DossierInjector
from graph.neo4j_client import client as neo4j_client
from parsers.models import DBChangeRecord
from utils.logger import get_logger

logger = get_logger(__name__)


class RealtimeAgent:
    """
    Real-time agent that continuously monitors and processes changes.
    Uses complete pipeline with consolidation.
    """
    
    def __init__(self, poll_interval: int = 30):
        """
        Initialize real-time agent.
        
        Args:
            poll_interval: Seconds between polls (default: 30)
        """
        self.poll_interval = poll_interval
        self.running = False
        
        # Initialize components
        self.sql_client = get_sql_client()
        self.poller = get_change_poller()
        self.pipeline = ChangeDetectionPipeline()  # Phases 3-7: Plan generation
        self.generator = SectionContentGenerator()  # Phase 9: Content generation
        self.injector = DossierInjector(neo4j_client=neo4j_client)  # Phase 10: Use connected client
        
        # Stats
        self.cycles_run = 0
        self.changes_processed = 0
        self.plans_generated = 0
        self.content_generated = 0
        self.content_approved = 0
        self.content_rejected = 0
        self.injections_completed = 0
        self.errors_encountered = 0
        
        logger.info("RealtimeAgent initialized with complete pipeline")
    
    def start(self):
        """Start the agent polling loop."""
        self.running = True
        
        print("\n" + "="*80)
        print("🤖 REAL-TIME DOSSIER UPDATE AGENT (WITH CONSOLIDATION)")
        print("="*80)
        print(f"Poll interval: {self.poll_interval}s")
        print("Press Ctrl+C to stop")
        print("="*80 + "\n")
        
        # Connect to databases
        try:
            self.sql_client.connect()
            neo4j_client.connect()
            print("✅ Connected to SQL Server and Neo4j\n")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return
        
        # Register signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        
        # Main polling loop
        while self.running:
            try:
                self._poll_cycle()
                
                if self.running:  # Only sleep if not stopping
                    time.sleep(self.poll_interval)
                    
            except Exception as e:
                logger.error(f"Cycle error: {e}", exc_info=True)
                self.errors_encountered += 1
                time.sleep(5)  # Brief pause on error
        
        self._print_summary()
    
    def _poll_cycle(self):
        """Execute one polling cycle."""
        self.cycles_run += 1
        cycle_start = time.time()
        
        print(f"\n{'─'*80}")
        print(f"📊 Poll Cycle #{self.cycles_run} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'─'*80}")
        
        # PHASE 2: Poll for changes
        print("\n🔍 PHASE 2: Polling for DB changes...")
        bundles = self.poller.poll_once()
        
        if not bundles:
            print("   ⏸️  No pending changes")
            return
        
        print(f"   ✅ Found {len(bundles)} bundle(s)")
        
        # Process each bundle
        for bundle_idx, bundle in enumerate(bundles, 1):
            print(f"\n{'─'*60}")
            print(f"📦 Bundle {bundle_idx}/{len(bundles)}: Product {bundle.product_code}")
            print(f"   Changes: {len(bundle.changes)}")
            print(f"{'─'*60}")
            
            try:
                self._process_bundle(bundle)
            except Exception as e:
                logger.error(f"Bundle processing failed: {e}", exc_info=True)
                self.errors_encountered += 1
                print(f"   ❌ Error: {e}")
        
        cycle_duration = time.time() - cycle_start
        print(f"\n⏱️  Cycle completed in {cycle_duration:.2f}s")
    
    def _process_bundle(self, bundle):
        """Process a single change bundle through complete end-to-end pipeline."""
        
        # ============================================================
        # PHASES 3-7: Plan Generation (with Consolidation)
        # ============================================================
        print(f"\n🚀 PHASES 3-7: Generating consolidated update plan...")
        
        plans = self.pipeline.process_change_bundle(bundle)
        
        if not plans:
            print("   ⚠️  No plans generated")
            return
        
        print(f"\n📊 CONSOLIDATED PLAN OUTPUT:")
        print(f"   Generated {len(plans)} plan(s)")
        
        # Process each plan
        for plan_idx, plan in enumerate(plans, 1):
            print(f"\n{'═'*70}")
            print(f"📋 PLAN #{plan_idx}: {plan.section_number} - {plan.title}")
            print(f"{'═'*70}")
            print(f"   Pattern: {plan.pattern_change_type}")
            print(f"   Status: {plan.status}")
            print(f"   Confidence: {plan.overall_confidence}")
            
            if plan.reference_source == "CROSS_DOSSIER":
                print(f"   📚 Template from: {plan.reference_product_code} Section {plan.reference_section_number}")
            
            # Show structural changes
            if hasattr(plan, '__dict__') and 'renumbering_required' in plan.__dict__:
                renumbering = plan.__dict__.get('renumbering_required', {})
                if renumbering:
                    print(f"   🔄 Renumbering required:")
                    for old, new in renumbering.items():
                        print(f"      {old} → {new}")
            
            # Show changes addressed
            if plan.concept_changes:
                print(f"   📌 Addresses {len(plan.concept_changes)} change(s):")
                for cc in plan.concept_changes:
                    print(f"      • {cc.concept}: {cc.change_type}")
            
            self.plans_generated += 1
            
            # ============================================================
            # PHASE 9: Content Generation
            # ============================================================
            print(f"\n📝 PHASE 9: Generating actual content with format matching...")
            
            try:
                generated_content = self.generator.generate_content(plan)
                self.content_generated += 1
                
                print(f"   ✅ Generated {len(generated_content.generated_text)} characters")
                print(f"   Format style: {generated_content.format_style}")
                print(f"   Confidence: {generated_content.generation_confidence:.2f}")
                
                # Display FULL generated content - NO TRUNCATION for validation!
                print(f"\n{'═'*70}")
                print(f"📄 COMPLETE GENERATED CONTENT:")
                print(f"{'═'*70}")
                print(generated_content.generated_text)
                print(f"{'═'*70}")
                
            except Exception as e:
                logger.error(f"Content generation failed: {e}", exc_info=True)
                print(f"   ❌ Generation failed: {e}")
                self.errors_encountered += 1
                continue
            
            # ============================================================
            # USER APPROVAL LOOP
            # ============================================================
            print(f"\n🤔 APPROVAL REQUIRED:")
            print(f"   This content will:")
            if generated_content.is_new_section:
                print(f"   • CREATE new section {generated_content.section_number}")
            else:
                print(f"   • UPDATE existing section {generated_content.section_number}")
            
            if generated_content.requires_renumbering:
                print(f"   • RENUMBER sections: {generated_content.renumbering_map}")
            
            print(f"\nOptions:")
            print(f"  [A] Approve and inject into graph")
            print(f"  [R] Reject (skip this change)")
            print(f"  [Q] Quit agent")
            
            while True:
                try:
                    choice = input("\nYour choice (A/R/Q): ").strip().upper()
                    
                    if choice == 'A':
                        # APPROVE
                        generated_content.status = "APPROVED"
                        self.content_approved += 1
                        
                        # ============================================================
                        # PHASE 10: Graph Injection
                        # ============================================================
                        print(f"\n💉 PHASE 10: Injecting into Neo4j graph...")
                        
                        try:
                            result = self.injector.inject_approved_content(
                                content=generated_content,
                                author="realtime_agent",
                                comment=f"Auto-update from {len(bundle.changes)} DB change(s)"
                            )
                            
                            print(f"\n{'═'*70}")
                            print(f"✅ INJECTION COMPLETE!")
                            print(f"{'═'*70}")
                            print(f"   Version: {result.version_created}")
                            print(f"   Sections created: {len(result.sections_created)}")
                            print(f"   Sections updated: {len(result.sections_updated)}")
                            print(f"   Sections renumbered: {len(result.sections_renumbered)}")
                            
                            if result.sections_renumbered:
                                print(f"\n   Renumbering details:")
                                for remap in result.sections_renumbered:
                                    print(f"      {remap}")
                            
                            if result.errors:
                                print(f"\n   ⚠️  Errors: {result.errors}")
                            
                            self.injections_completed += 1
                            
                        except Exception as e:
                            logger.error(f"Injection failed: {e}", exc_info=True)
                            print(f"   ❌ Injection failed: {e}")
                            self.errors_encountered += 1
                        
                        break
                    
                    elif choice == 'R':
                        # REJECT
                        generated_content.status = "REJECTED"
                        self.content_rejected += 1
                        print(f"   ⏭️  Rejected - skipping this change")
                        break
                    
                    elif choice == 'Q':
                        # QUIT
                        print(f"\n⚠️  User requested quit")
                        self.running = False
                        return
                    
                    else:
                        print(f"   Invalid choice. Please enter A, R, or Q")
                        continue
                
                except KeyboardInterrupt:
                    print(f"\n\n⚠️  Interrupted - moving to next cycle")
                    break
        
        print(f"\n🎉 Bundle processing complete!")
    
    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully."""
        print("\n\n⚠️  Shutdown signal received...")
        self.running = False
    
    def _print_summary(self):
        """Print session summary."""
        print("\n" + "="*80)
        print("📊 SESSION SUMMARY")
        print("="*80)
        print(f"Cycles run: {self.cycles_run}")
        print(f"Changes processed: {self.changes_processed}")
        print(f"Plans generated: {self.plans_generated}")
        print(f"Content generated: {self.content_generated}")
        print(f"Content approved: {self.content_approved}")
        print(f"Content rejected: {self.content_rejected}")
        print(f"Injections completed: {self.injections_completed}")
        print(f"Errors: {self.errors_encountered}")
        print("="*80)
        print("\n👋 Agent stopped\n")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Real-time dossier update agent"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Poll interval in seconds (default: 30)"
    )
    
    args = parser.parse_args()
    
    agent = RealtimeAgent(poll_interval=args.interval)
    agent.start()


if __name__ == "__main__":
    main()
