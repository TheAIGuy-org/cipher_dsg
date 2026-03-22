"""
dossier_gen_engine/md_pdf.py
-----------------------------
Markdown → PDF pipeline.

Changes from the original finals/md_pdf.py:
  1. md_to_pdf() now accepts an explicit output_path argument instead of
     deriving it from output_dir + filename. This is how the engine calls it.
  2. The batch main() function is UNCHANGED — still works as before.
  3. Logo path defaults to the logo/ folder inside this engine package.

Direct batch usage (unchanged):
    python md_pdf.py
"""

import logging
import re
from pathlib import Path
import markdown
import base64
from playwright.sync_api import sync_playwright

# =============================
# CONFIG
# =============================
_ENGINE_ROOT  = Path(__file__).parent
MD_FOLDER     = _ENGINE_ROOT / "data" / "markdown_output"
OUTPUT_FOLDER = _ENGINE_ROOT / "data" / "pdf_output"
LOGO_PATH     = _ENGINE_ROOT / "logo" / "gyansys-logo-black.png"

# =============================
# LOGGING
# =============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# =============================
# CSS
# =============================
CSS = """
body {
    font-family: "Times New Roman", Times, serif;
    font-size: 12px;
    line-height: 1.6;
}

h1, h2, h3 {
    page-break-after: avoid;
}

table {
    border-collapse: collapse;
    width: 100%;
    table-layout: fixed;
    font-size: 10px;
}

thead {
    display: table-header-group;
}

tr {
    break-inside: avoid;
}

th {
    font-weight: bold;
    font-size: 10px;
    background-color: #f0f0f0;
}

td {
    font-size: 10px;
}

th, td {
    border: 1px solid #333;
    padding: 4px;
    vertical-align: top;
    word-wrap: break-word;
    overflow-wrap: break-word;
}

pre {
    background: #f5f5f5;
    padding: 8px;
    white-space: pre-wrap;
    font-size: 10px;
}

img {
    max-width: 100%;
}

ul {
    margin: 6px 0 6px 20px;
    padding-left: 20px;
}

li {
    margin-bottom: 4px;
}
"""

# =============================
# HEADER PARSING (POSITIONAL — NOT YAML)
# =============================

PAGE_LINE_PATTERN = re.compile(r"^page\s+\d+\s+of\s+\d+$", re.IGNORECASE)
STOP_WORDS        = {"additional information"}


def parse_positional_header(md_text: str) -> tuple[dict, str]:
    """
    Reads the --- front matter block and treats lines as positional:
      Index 0 → product
      Index 1 → doc_code
      Index 2 → reg_code
      Index 3 → issue_date
    Stops at "page X of Y" or "Additional Information".

    Returns:
        metadata (dict), body (str — markdown without front matter)
    """
    fm_pattern = re.compile(r"^---\n([\s\S]*?)\n---\n", re.MULTILINE)
    match      = fm_pattern.match(md_text.strip())

    if not match:
        return {}, md_text

    raw_block = match.group(1)
    body      = md_text[match.end():]

    positional_lines = []
    for line in raw_block.split("\n"):
        stripped = line.strip()
        if not stripped:
            continue
        if PAGE_LINE_PATTERN.match(stripped):
            break
        if stripped.lower() in STOP_WORDS:
            break
        positional_lines.append(stripped)

    keys     = ["product", "doc_code", "reg_code", "issue_date"]
    metadata = {}
    for i, value in enumerate(positional_lines):
        if i < len(keys):
            metadata[keys[i]] = value

    return metadata, body


# =============================
# HEADER TEMPLATE BUILDER
# =============================

def build_header_template(
    metadata:  dict,
    logo_path: Path | str | None = None,
) -> str:
    product    = metadata.get("product", "")
    doc_code   = metadata.get("doc_code", "")
    reg_code   = metadata.get("reg_code", "")
    issue_date = metadata.get("issue_date", "")

    # Default to the logo bundled with this engine package
    if logo_path is None:
        logo_path = LOGO_PATH

    logo_b64 = ""
    try:
        with open(logo_path, "rb") as f:
            logo_b64 = base64.b64encode(f.read()).decode("utf-8")
    except FileNotFoundError:
        logging.warning(f"Logo not found at: {logo_path} — skipping logo in header")

    logo_html = (
        f'<img src="data:image/png;base64,{logo_b64}" style="height: 20px; object-fit: contain;">'
        if logo_b64 else ""
    )

    return f"""
    <div style="
        width: 100%;
        padding: 0 20mm;
        box-sizing: border-box;
        font-family: 'Times New Roman', serif;
        font-size: 9px;
        color: #333;
        border-bottom: 1px solid #aaa;
        padding-bottom: 3px;
    ">
        <div style="
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2px;
        ">
            <span style="font-weight: bold;">{product}</span>
            {logo_html}
        </div>
        <div style="
            display: flex;
            justify-content: space-between;
            align-items: center;
        ">
            <span style="flex: 1; text-align: left;">{doc_code}</span>
            <span style="flex: 1; text-align: center;">{reg_code}</span>
            <span style="flex: 1; text-align: right;">{issue_date}</span>
        </div>
    </div>
    """


# =============================
# MARKDOWN → HTML
# =============================

def md_to_html(md_text: str) -> str:
    """Convert markdown body to a full HTML document."""
    html_body = markdown.markdown(
        md_text,
        extensions=[
            "tables",
            "fenced_code",
            "toc",
            "sane_lists",
            "nl2br"
        ]
    )

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>{CSS}</style>
</head>
<body>
    {html_body}
</body>
</html>"""


# =============================
# HTML → PDF (Playwright)
# =============================

def html_to_pdf(html: str, header_template: str, output_path: str):
    """Render HTML to PDF with a repeating 2-line header on every page."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page    = browser.new_page()

        logging.info("    Loading HTML...")
        page.set_content(html, wait_until="networkidle")

        logging.info("    Generating PDF...")
        page.pdf(
            path=output_path,
            format="A4",
            print_background=True,
            display_header_footer=True,
            header_template=header_template,
            footer_template="""
                <div style="
                    width: 100%;
                    padding: 0 20mm;
                    box-sizing: border-box;
                    font-family: 'Times New Roman', serif;
                    font-size: 9px;
                    color: #888;
                    text-align: right;
                ">
                    Page <span class="pageNumber"></span> of <span class="totalPages"></span>
                </div>
            """,
            margin={
                "top":    "32mm",
                "bottom": "20mm",
                "left":   "20mm",
                "right":  "20mm"
            }
        )

        browser.close()


# =============================
# CORE FUNCTION: md_to_pdf
# =============================

def md_to_pdf(
    md_path:     Path,
    output_path: Path | None = None,
    output_dir:  Path | None = None,
) -> Path:
    """
    Convert one markdown file to a PDF.

    Two calling modes:
      1. engine mode  — pass output_path explicitly (engine.py uses this)
         md_to_pdf(md_path=Path("data/markdown_output/1614322_20250322.md"),
                   output_path=Path("data/pdf_output/1614322_20250322.pdf"))

      2. batch mode   — pass output_dir (main() uses this)
         md_to_pdf(md_path=Path("data/markdown_output/cream.md"),
                   output_dir=Path("data/pdf_output/"))

    Args:
        md_path     : Path to the markdown file.
        output_path : Explicit output PDF path. Takes priority over output_dir.
        output_dir  : Directory to write <stem>.pdf into (batch fallback).

    Returns:
        Path to the written PDF file.
    """
    logging.info(f"  Processing: {md_path.name}")

    md_text = md_path.read_text(encoding="utf-8")

    # Step 1: Parse positional header block
    metadata, body = parse_positional_header(md_text)
    logging.info(f"    Metadata extracted: {metadata}")

    # Step 2: Build Playwright-compatible header template
    header_template = build_header_template(metadata)

    # Step 3: Convert markdown body to HTML
    html = md_to_html(body)

    # Step 4: Resolve output path
    if output_path is not None:
        resolved_output = Path(output_path)
    elif output_dir is not None:
        resolved_output = Path(output_dir) / md_path.with_suffix(".pdf").name
    else:
        resolved_output = OUTPUT_FOLDER / md_path.with_suffix(".pdf").name

    resolved_output.parent.mkdir(parents=True, exist_ok=True)

    # Step 5: Render to PDF
    html_to_pdf(html, header_template, str(resolved_output))

    logging.info(f"    ✅ Saved: {resolved_output}")
    return resolved_output

