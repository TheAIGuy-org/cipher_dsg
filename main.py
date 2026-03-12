"""
main.py
--------
Entry point for the Cipher DSG (Dossier Structural Graph) builder.
LLM-powered dynamic system with zero hardcoded metadata.

Pipeline:
  1. Connect to Neo4j AuraDB
  2. Build schema (constraints + vector indexes) — idempotent
  3. For each dossier in the registry:
     a. Extract text from PDF
     b. Parse sections with embeddings (no regex classification)
     c. Load into Neo4j graph with full text templates
  4. System ready for LLM-powered queries

Features:
  - Semantic search via vector embeddings
  - Template-based content generation
  - Evidence-based section discovery
  - Works with ANY regulation (no code changes needed)

Usage:
  python main.py                  # Full build
  python main.py --clear          # Wipe existing data then rebuild
  python main.py --parse-only     # Parse only, no Neo4j writes
"""
import sys
import argparse
import traceback

from config.dossier_registry import DOSSIER_REGISTRY
from parsers.dossier_parser import parse_dossier_v2
from graph.neo4j_client import client
from graph.neo4j_schema import build_schema, clear_all_data
from graph.graph_loader import load_dossier
from utils.logger import get_logger

log = get_logger("main")


def build_dsg(clear: bool = False) -> None:
    """Full DSG build pipeline."""
    log.info("=" * 70)
    log.info("  CIPHER DSG BUILDER — LLM-Powered Dynamic System")
    log.info("=" * 70)

    client.connect()

    try:
        # Step 1: Schema
        build_schema(client)

        # Step 2: Optionally clear existing data
        if clear:
            clear_all_data(client)

        # Step 3: Process each dossier
        for manifest in DOSSIER_REGISTRY:
            log.info(f"\n📄 Processing: {manifest.product_name}")
            log.info(f"   PDF: {manifest.pdf_filename}")

            # Parse dossier with embeddings (no regex classification)
            dossier = parse_dossier_v2(manifest.pdf_path, manifest)
            
            if dossier:
                # Load into Neo4j with full text templates + embeddings
                load_dossier(dossier, client, skip_embeddings=False)
                log.info(f"   ✅ Loaded {len(dossier.sections)} sections")
            else:
                log.warning(f"   ⚠️  Failed to parse {manifest.product_name}")

        log.info("\n" + "=" * 70)
        log.info("✅ DSG BUILD COMPLETE")
        log.info("=" * 70)
        log.info("System ready for:")
        log.info("  • Semantic section discovery (vector search)")
        log.info("  • Template-based content generation (LLM)")
        log.info("  • Hierarchical placement decisions (graph + LLM)")
        log.info("\nUse validate_system.py to test LLM capabilities.")

    except Exception as e:
        log.error(f"Fatal error during DSG build: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        client.close()


def parse_only() -> None:
    """Parse all dossiers and print results without writing to Neo4j."""
    log.info("=" * 70)
    log.info("  PARSE-ONLY MODE — No Neo4j writes")
    log.info("=" * 70)
    
    for manifest in DOSSIER_REGISTRY:
        log.info(f"\n{'='*70}")
        log.info(f"  {manifest.product_name}")
        log.info(f"{'='*70}")
        
        dossier = parse_dossier_v2(manifest.pdf_path, manifest)
        
        if dossier:
            log.info(f"  ✅ Parsed {len(dossier.sections)} sections")
            for s in dossier.sections:
                log.info(
                    f"    {s.section_number:12s} {s.title[:50]:50s} "
                    f"[{s.content_format:10s}] "
                    f"text={len(s.full_text):4d} chars, "
                    f"embedding={len(s.embedding):4d} dims"
                )
        else:
            log.warning(f"  ⚠️  Failed to parse")
    
    log.info("\n✅ Parse-only complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Cipher DSG Builder - LLM-Powered Dynamic System"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear all existing DSG data before loading",
    )
    parser.add_argument(
        "--parse-only",
        action="store_true",
        help="Parse dossiers only — do not write to Neo4j",
    )

    args = parser.parse_args()

    if args.parse_only:
        parse_only()
    else:
        build_dsg(clear=args.clear)
