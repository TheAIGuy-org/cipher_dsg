"""
dossier_gen_engine/models.py
-----------------------------
Pydantic contracts for the dossier generation engine.

These are the ONLY two objects that cross the boundary between
cipher_dsg and dossier_gen_engine.

Usage from cipher_dsg (workflow.py):

    from dossier_gen_engine.models import SectionUpdate, EngineManifest
    from dossier_gen_engine.engine import generate_updated_dossier
    from config.dossier_registry import DOSSIER_REGISTRY

    registry_entry = next(m for m in DOSSIER_REGISTRY if m.product_code == product_code)

    manifest = EngineManifest.from_registry(registry_entry)
    update   = SectionUpdate(
        section = generated_content.section_number,   # e.g. "2.2.7"
        title   = generated_content.section_title,    # e.g. "Natural origin"
        content = generated_content.generated_text,   # raw text / markdown-ish string
    )

    pdf_path = generate_updated_dossier(manifest, update)
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, field_validator, model_validator

if TYPE_CHECKING:
    # Avoid hard import of cipher_dsg internals at module load time.
    # Only used for the from_registry() class method type hint.
    from config.dossier_registry import DossierManifest as RegistryManifest


# ─────────────────────────────────────────────
# SectionUpdate
# ─────────────────────────────────────────────

class SectionUpdate(BaseModel):
    """
    One section to be injected / updated in the dossier.

    Field notes
    -----------
    section : str
        Dotted section number, e.g. "2.2.7" or "2.2.2.1".
        Must match the pattern used in the original PDF.

    title : str
        Section heading exactly as it should appear in the document.
        The LLM title-similarity check in section_update.py uses this
        to decide REPLACE vs INSERT vs CONFLICT.

    content : str
        Raw content — can be plain prose, Markdown tables, or a mix.
        Newlines (\\n) and Markdown syntax pass through untouched.
        The LLM formatting step (Step 6 in pdf_md.py) converts this
        to clean Markdown before it reaches the PDF renderer.

        Example inputs that all work:
          - Plain text paragraphs separated by \\n\\n
          - Markdown tables (| col | col |\\n|---|---|\\n| val | val |)
          - Mixed prose + tables (exactly what generated_content returns)
    """

    section: str = Field(..., description="Dotted section number, e.g. '2.2.7'")
    title:   str = Field(..., description="Section heading text")
    content: str = Field(..., description="Raw section content (prose/markdown)")

    @field_validator("section")
    @classmethod
    def section_must_be_dotted(cls, v: str) -> str:
        v = v.strip()
        parts = v.split(".")
        if len(parts) < 2 or not all(p.isdigit() for p in parts):
            raise ValueError(
                f"section must be a dotted numeric string like '2.2.7', got: {v!r}"
            )
        return v

    @field_validator("title", "content")
    @classmethod
    def must_not_be_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("field must not be blank or whitespace-only")
        return v

    def to_pipeline_dict(self) -> dict:
        """
        Convert to the dict format expected by pdf_md.process_pdf()
        and section_update.apply_section_update().

        Returns:
            {"section": "2.2.7", "title": "Natural origin", "content": "..."}
        """
        return {
            "section": self.section,
            "title":   self.title,
            "content": self.content,
        }


# ─────────────────────────────────────────────
# EngineManifest
# ─────────────────────────────────────────────

class EngineManifest(BaseModel):
    """
    Ground-truth metadata for one dossier product.

    Mirrors config.dossier_registry.DossierManifest but as a Pydantic
    model so the engine has no hard dependency on cipher_dsg internals.

    Field mapping from DossierManifest
    ------------------------------------
    pdf_filename  → used to build pdf_path
    product_code  → used for output filenames
    product_name  → written into the MD front-matter header (product field)
    version_code  → written into the MD front-matter header (doc_code field)
    regqual_code  → written into the MD front-matter header (reg_code field)
    issue_date    → written into the MD front-matter header (issue_date field)
    dossier_dir   → base directory where pdf_filename lives
    """

    product_code: str = Field(..., description="e.g. '1614322'")
    product_name: str = Field(..., description="e.g. 'BEPANTHOL Face Day Cream'")
    version_code: str = Field(..., description="e.g. 'C.2.2-03'")
    regqual_code: str = Field(..., description="e.g. 'VV-REGQUAL-108834'")
    issue_date:   str = Field(..., description="ISO date string, e.g. '2022-03-24'")
    pdf_path:     Path = Field(..., description="Absolute path to the source PDF")

    model_config = {"arbitrary_types_allowed": True}

    @model_validator(mode="after")
    def pdf_must_exist(self) -> "EngineManifest":
        if not self.pdf_path.exists():
            raise ValueError(f"Source PDF not found: {self.pdf_path}")
        return self

    # ── Factory ───────────────────────────────────────────────────────────

    @classmethod
    def from_registry(cls, entry: "RegistryManifest") -> "EngineManifest":
        """
        Build an EngineManifest from a cipher_dsg DossierManifest entry.

        Example:
            from config.dossier_registry import DOSSIER_REGISTRY
            entry   = next(m for m in DOSSIER_REGISTRY if m.product_code == "1614322")
            manifest = EngineManifest.from_registry(entry)
        """
        return cls(
            product_code=entry.product_code,
            product_name=entry.product_name,
            version_code=entry.version_code,
            regqual_code=entry.regqual_code,
            issue_date=entry.issue_date,
            pdf_path=entry.pdf_path,
        )