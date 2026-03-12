"""
config/dossier_registry.py
--------------------------
Maps each dossier PDF to its product metadata.

This is the ONLY place you need to edit when a new dossier is added.
Everything else auto-discovers from this registry.

Why a registry instead of auto-discovery?
  Because product_code, version_code, and issue_date are embedded in
  dossier headers, not always in filenames. We parse them from the PDF
  but keep a manifest here as the authoritative ground truth to cross-
  validate against what the parser finds.
"""
from dataclasses import dataclass, field
from pathlib import Path
from config.settings import settings


@dataclass
class DossierManifest:
    """Authoritative ground-truth for one dossier."""
    pdf_filename: str            # filename inside data/dossiers/
    product_id: int              # Must match Products.ProductID in SQL DB
    product_code: str            # e.g. "1614322"
    product_name: str            # e.g. "BEPANTHOL Face Day Cream"
    version_code: str            # e.g. "C.2.2-03"
    regqual_code: str            # e.g. "VV-REGQUAL-108834"
    issue_date: str              # ISO date string "YYYY-MM-DD"
    expected_sections: list[str] = field(default_factory=list)

    @property
    def pdf_path(self) -> Path:
        return settings.DOSSIER_DIR / self.pdf_filename


# ── Registry ────────────────────────────────────────────────────────────────
DOSSIER_REGISTRY: list[DossierManifest] = [
    DossierManifest(
        pdf_filename="face_day_cream_1614322.pdf",
        product_id=1,
        product_code="1614322",
        product_name="BEPANTHOL Face Day Cream",
        version_code="C.2.2-03",
        regqual_code="VV-REGQUAL-108834",
        issue_date="2022-03-24",
        expected_sections=[
            "2.2.1", "2.2.2", "2.2.2.1", "2.2.2.2",
            "2.2.3", "2.2.4", "2.2.5", "2.2.6", "2.2.7",
        ],
    ),
    DossierManifest(
        pdf_filename="lipstick_1614557.pdf",
        product_id=2,
        product_code="1614557",
        product_name="BEPANTHOL Lipstick",
        version_code="C.2.2-02",
        regqual_code="VV-REGQUAL-206035",
        issue_date="2024-05-29",
        expected_sections=[
            "2.2.1", "2.2.2", "2.2.2.1", "2.2.2.2",
            "2.2.3", "2.2.4", "2.2.5", "2.2.6", "2.2.7",
        ],
    ),
    DossierManifest(
        pdf_filename="cream_1600188.pdf",
        product_id=3,
        product_code="1600188",
        product_name="BEPANTHOL Cream",
        version_code="C.2.2-01",
        regqual_code="VV-REGQUAL-191544",
        issue_date="2022-09-01",
        expected_sections=[
            "2.2.1", "2.2.2", "2.2.2.1", "2.2.2.2",
            "2.2.2.3", "2.2.3", "2.2.4", "2.2.5", "2.2.6",
        ],
    ),
]
