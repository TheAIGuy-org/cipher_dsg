"""
dossier_gen_engine/engine.py
-----------------------------
Single entry point for the dossier generation engine.

Orchestrates the full pipeline:
  1. Validate inputs (Pydantic)
  2. Run pdf_md.process_pdf()  → writes .md to markdown_output/
  3. Run md_pdf.md_to_pdf()    → writes .pdf to pdf_output/
  4. Return the output PDF path

Called from cipher_dsg after a content update is approved:

    from dossier_gen_engine.models import SectionUpdate, EngineManifest
    from dossier_gen_engine.engine import generate_updated_dossier
    from config.dossier_registry import DOSSIER_REGISTRY

    entry    = next(m for m in DOSSIER_REGISTRY if m.product_code == product_code)
    manifest = EngineManifest.from_registry(entry)
    update   = SectionUpdate(
        section = generated_content.section_number,
        title   = generated_content.section_title,
        content = generated_content.generated_text,
    )
    pdf_path = generate_updated_dossier(manifest, update)
"""

import logging
from datetime import datetime
from pathlib import Path

from dossier_gen_engine.models import EngineManifest, SectionUpdate
from dossier_gen_engine.pdf_md import process_pdf
from dossier_gen_engine.md_pdf import md_to_pdf

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Directory layout (relative to engine root)
# ─────────────────────────────────────────────
# These match your existing finals/ project structure.
# Override by passing explicit paths to generate_updated_dossier().

_CIPHER_DSG_ROOT = Path(__file__).parent.parent   # goes up from dossier_gen_engine/ to cipher_dsg/
_MD_OUTPUT_DIR   = _CIPHER_DSG_ROOT / "data" / "markdown_output"
_PDF_OUTPUT_DIR  = _CIPHER_DSG_ROOT / "data" / "pdf_output"


# ─────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────

def generate_updated_dossier(
    manifest:         EngineManifest,
    updated_sections: list[SectionUpdate],
    md_output_dir:    Path | None = None,
    pdf_output_dir:   Path | None = None,
) -> Path:
    """
    Full pipeline: source PDF + one or more section updates → single updated PDF.

    Args:
        manifest         : EngineManifest (built from DOSSIER_REGISTRY entry)
        updated_sections : list of SectionUpdate (all approved sections in the bundle)
        md_output_dir    : Override for markdown output folder (optional)
        pdf_output_dir   : Override for PDF output folder (optional)

    Returns:
        Path to the generated PDF file.
        e.g. data/pdf_output/1614322_20250322_143045.pdf

    Raises:
        ValueError       : if list is empty or Pydantic validation fails
        FileNotFoundError: if source PDF or logo is missing
        RuntimeError     : if pdf_md or md_pdf pipeline fails
    """
    if not updated_sections:
        raise ValueError("[engine] updated_sections list is empty — nothing to generate")

    md_dir  = md_output_dir  or _MD_OUTPUT_DIR
    pdf_dir = pdf_output_dir or _PDF_OUTPUT_DIR

    md_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem      = f"{manifest.product_code}_{timestamp}"

    md_path  = md_dir  / f"{stem}.md"
    pdf_path = pdf_dir / f"{stem}.pdf"

    section_labels = ", ".join(s.section for s in updated_sections)

    log.info("=" * 60)
    log.info(f"[engine] START  product={manifest.product_code}  sections=[{section_labels}]")
    log.info(f"[engine] source PDF : {manifest.pdf_path}")
    log.info(f"[engine] output MD  : {md_path}")
    log.info(f"[engine] output PDF : {pdf_path}")
    log.info("=" * 60)

    # ── STAGE 1: PDF → Markdown (with all section updates injected) ──────
    log.info("[engine] Stage 1: pdf_md.process_pdf()")
    try:
        process_pdf(
            pdf_path=str(manifest.pdf_path),
            updated_sections=[s.to_pipeline_dict() for s in updated_sections],
            output_md_path=str(md_path),
            manifest_metadata={
                "product":    manifest.product_name,
                "doc_code":   manifest.version_code,
                "reg_code":   manifest.regqual_code,
                "issue_date": manifest.issue_date,
            },
        )
    except Exception as e:
        raise RuntimeError(f"[engine] pdf_md stage failed: {e}") from e

    if not md_path.exists():
        raise RuntimeError(f"[engine] pdf_md did not produce expected file: {md_path}")

    log.info(f"[engine] Stage 1 complete — MD written: {md_path}")

    # ── STAGE 2: Markdown → PDF ──────────────────────────────────────────
    log.info("[engine] Stage 2: md_pdf.md_to_pdf()")
    try:
        md_to_pdf(md_path=md_path, output_path=pdf_path)
    except Exception as e:
        raise RuntimeError(f"[engine] md_pdf stage failed: {e}") from e

    if not pdf_path.exists():
        raise RuntimeError(f"[engine] md_pdf did not produce expected file: {pdf_path}")

    log.info(f"[engine] Stage 2 complete — PDF written: {pdf_path}")
    log.info(f"[engine] DONE  → {pdf_path}")

    return pdf_path