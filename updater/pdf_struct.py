import io
import json
import shutil
import time
import zipfile
from pathlib import Path
from typing import Any

import requests

from config.settings import settings

BASE_URL      = "https://pdf-services.adobe.io"
EXTRACTED_DIR = settings.PROJECT_ROOT / "data" / "extracted"


def pdf_to_struct(pdf_path: Path) -> dict[str, Any]:
    """
    Upload a PDF to Adobe PDF Extract API, overwrite its extracted folder
    in data/extracted/<stem>/, and return the structuredData.json as a dict.

    Every call re-extracts and replaces the previous result so the
    extracted data always reflects the current PDF.

    Parameters
    ----------
    pdf_path : Path to the dossier PDF, e.g. data/dossiers/lipstick_1614557.pdf

    Returns
    -------
    Parsed structuredData.json as a dict.
    """
    extract_dir = EXTRACTED_DIR / pdf_path.stem

    # Always start fresh — remove previous extraction if it exists
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True)

    headers = {
        "Authorization": f"Bearer {settings.ADOBE_ACCESS_TOKEN}",
        "x-api-key":     settings.ADOBE_CLIENT_ID,
        "Content-Type":  "application/json",
    }

    # 1. Create asset
    asset_data = requests.post(
        f"{BASE_URL}/assets",
        headers=headers,
        json={"mediaType": "application/pdf"},
    ).json()
    asset_id = asset_data["assetID"]
    upload_uri = asset_data["uploadUri"]

    # 2. Upload PDF
    with open(pdf_path, "rb") as f:
        requests.put(
            upload_uri,
            headers={"Content-Type": "application/pdf"},
            data=f,
        ).raise_for_status()

    # 3. Start extract job
    extract_resp = requests.post(
        f"{BASE_URL}/operation/extractpdf",
        headers=headers,
        json={
            "assetID":asset_id,
            "elementsToExtract": ["text", "tables"],
        },
    )
    status_url = extract_resp.headers["Location"]

    # 4. Poll until done
    while True:
        status_data = requests.get(status_url, headers=headers).json()
        if status_data["status"] == "done":
            download_url = status_data["resource"]["downloadUri"]
            break
        time.sleep(2)

    # 5. Unzip into data/extracted/<stem>/
    zip_bytes = requests.get(download_url).content
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            dest = extract_dir / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(zf.read(name))

    return json.loads((extract_dir / "structuredData.json").read_text(encoding="utf-8"))