# api/app.py
import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

# Import your existing architecture modules
from config.dossier_registry import DOSSIER_REGISTRY
from db.sql_client import get_sql_client
from db.poller import get_change_poller
from db.change_pipeline import ChangeDetectionPipeline
from dossier_gen_engine import generate_updated_dossier, SectionUpdate, EngineManifest
from graph.neo4j_client import client as neo4j_client
from db.dossier_injector import DossierInjector
from llm.content_generator import SectionContentGenerator
from utils.logger import get_logger

app = FastAPI(title="Cipher DSG Autonomous Agent API")

# ============================================================
# CACHE BUSTING MIDDLEWARE
# Strictly disables caching for development 
# ============================================================
@app.middleware("http")
async def add_no_cache_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

# Serve frontend static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Serve original dossier PDFs
app.mount("/dossiers", StaticFiles(directory="data/dossiers"), name="dossiers")

# Serve generated/updated PDFs
app.mount("/pdfs", StaticFiles(directory="data/pdf_output"), name="pdfs")

log = get_logger("api")

# --- Initialize Core Components ---
sql_client = get_sql_client()
poller     = get_change_poller()
pipeline   = ChangeDetectionPipeline()
generator  = SectionContentGenerator()
injector   = DossierInjector(neo4j_client=neo4j_client)


# ============================================================
# WEBSOCKET MANAGER
# ============================================================

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        # Iterate over a copy so we can safely remove dead connections mid-loop
        dead = []
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except (WebSocketDisconnect, RuntimeError) as e:
                log.warning(f"Dead WebSocket detected during broadcast, removing: {e}")
                dead.append(connection)
            except Exception as e:
                log.warning(f"Unexpected WebSocket error during broadcast: {e}")
                dead.append(connection)
        for conn in dead:
            self.disconnect(conn)

manager = ConnectionManager()

# --- Global State for HITL Pause ---
# Keyed by review_id (per-section) instead of run_id (per-bundle)
pending_reviews: Dict[str, asyncio.Event] = {}
review_decisions: Dict[str, str] = {}

# --- API Models ---
class ReviewDecision(BaseModel):
    review_id: str
    decision: str  # "APPROVE" or "REJECT"


# ============================================================
# HELPER: Serialize bundle.changes into a JSON-safe list
# ============================================================

def serialize_db_changes(bundle) -> List[Dict[str, Any]]:
    """
    Convert bundle.changes (List[DBChangeRecord]) into a list of
    plain dicts safe for JSON broadcast over WebSocket.
    """
    def safe_str(val) -> Optional[str]:
        if val is None:
            return None
        try:
            return str(val)
        except Exception:
            return None

    try:
        result = []
        for c in bundle.changes:
            log.debug(
                f"  DB change: {c.operation_type} {c.source_table}.{c.column_name} "
                f"| old={c.old_value!r} → new={c.new_value!r}"
            )
            result.append({
                "source_table":     safe_str(c.source_table),
                "operation_type":   safe_str(c.operation_type),
                "column_name":      safe_str(c.column_name),
                "old_value":        safe_str(c.old_value),
                "new_value":        safe_str(c.new_value),
                "changed_by":       safe_str(c.changed_by),
                "change_timestamp": safe_str(c.change_timestamp),
            })
        log.info(f"serialize_db_changes: produced {len(result)} record(s)")
        return result
    except Exception as e:
        log.error(f"serialize_db_changes failed unexpectedly: {e}", exc_info=True)
        return []


# ============================================================
# CONCEPT → TABLE TAXONOMY MAP
# ============================================================

CONCEPT_TO_TABLES: Dict[str, List[str]] = {
    "heavy metal":        ["RawMaterialTraces"],
    "cmr substance":      ["RawMaterialTraces"],
    "trace":              ["RawMaterialTraces"],
    "allergen":           ["RawMaterialAllergens"],
    "formulation":        ["RawMaterials", "ProductFormulation"],
    "natural origin":     ["RawMaterials", "ProductFormulation"],
    "supplier":           ["RawMaterials"],
    "composition":        ["RawMaterials", "ProductFormulation"],
    "ingredient":         ["RawMaterials"],
    "manufacturer":       ["RawMaterials"],
}


# ============================================================
# HELPER: Filter bundle changes relevant to a specific plan
# ============================================================

def get_relevant_changes(plan, all_changes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not all_changes:
        return []

    # Step 1 — collect implied tables from all concepts on this plan
    implied_tables: set = set()
    for cc in plan.concept_changes:
        concept_lower = cc.concept.lower()
        change_type_lower = cc.change_type.lower()
        search_text = f"{concept_lower} {change_type_lower}"

        for keyword, tables in CONCEPT_TO_TABLES.items():
            if keyword in search_text:
                implied_tables.update(tables)

    log.debug(
        f"get_relevant_changes: plan {plan.section_number} → "
        f"implied tables={implied_tables}"
    )

    # Step 2 — filter changes to implied tables
    if implied_tables:
        relevant = [
            c for c in all_changes
            if c.get("source_table") in implied_tables
        ]
        if relevant:
            log.info(
                f"get_relevant_changes: {plan.section_number} → "
                f"{len(relevant)}/{len(all_changes)} matched via taxonomy"
            )
            return relevant

    # Fallback — concept not in taxonomy or no records matched
    log.warning(
        f"get_relevant_changes: no taxonomy match for "
        f"{plan.section_number} concepts="
        f"{[cc.concept for cc in plan.concept_changes]}, "
        f"falling back to all {len(all_changes)}"
    )
    return all_changes


# ============================================================
# REST ENDPOINTS
# ============================================================

@app.get("/")
async def serve_ui():
    return FileResponse("static/index.html")


@app.get("/api/v1/dossiers")
async def get_dossiers():
    """Return available dossiers for View 1. Only includes entries whose PDF exists on disk."""
    result = []
    for d in DOSSIER_REGISTRY:
        if d.pdf_path.exists():
            result.append({
                "product_code": d.product_code,
                "name":         d.product_name,
                # FIX: Routed to the /view/ subdirectory
                "pdf_url":      f"/dossiers/view/{d.pdf_filename}" 
            })
        else:
            log.warning(f"Dossier PDF not found on disk, skipping: {d.pdf_path}")
    return JSONResponse(content=result, headers={"Cache-Control": "no-store"})


@app.post("/api/v1/workflow/review")
async def submit_review(decision: ReviewDecision):
    """Handle HITL Approve/Reject decisions."""
    review_id = decision.review_id
    if review_id in pending_reviews:
        review_decisions[review_id] = decision.decision
        pending_reviews[review_id].set()
        return {"status": "success", "message": f"Decision {decision.decision} recorded."}
    return {"status": "error", "message": "Review ID not found or already processed."}


# ============================================================
# BACKGROUND AUTONOMOUS AGENT LOOP
# ============================================================

async def _inject_section(run_id: str, gc, bundle_changes):
    """Background injection for an approved section. Runs concurrently with reviews."""
    try:
        await manager.broadcast({
            "type": "AGENT_STATE", "run_id": run_id, "state": "STORING"
        })
        result = await asyncio.to_thread(
            injector.inject_approved_content,
            content=gc,
            author="web_agent",
            comment=f"Approved via UI - {len(bundle_changes)} DB change(s)"
        )
        log.info(
            f"✅ Injected section {gc.section_number} "
            f"- Version {result.version_created}"
        )
        if result.errors:
            log.error(f"⚠️ Injection errors: {result.errors}")
    except Exception as e:
        log.error(f"Injection failed for {gc.section_number}: {e}", exc_info=True)


async def autonomous_agent_loop():
    """Runs continuously, polls DB, and drives the pipeline via WebSockets."""
    log.info("Starting autonomous agent loop...")

    while True:
        try:
            bundles = await asyncio.to_thread(poller.poll_once)

            if bundles:
                for bundle in bundles:
                    run_id = f"run_{bundle.product_code}_{int(asyncio.get_event_loop().time())}"

                    db_changes = serialize_db_changes(bundle)

                    # --- Impact Detected ---
                    await manager.broadcast({
                        "type":         "IMPACT_DETECTED",
                        "run_id":       run_id,
                        "product_code": bundle.product_code,
                        "change_count": bundle.get_change_count()
                    })

                    # --- Stream pipeline state steps for UI ---
                    states = ["POLLING", "INTERPRETING", "MAPPING", "GENERATING"]
                    for state in states:
                        await asyncio.sleep(1)
                        await manager.broadcast({
                            "type":   "AGENT_STATE",
                            "run_id": run_id,
                            "state":  state
                        })

                    # --- Process the bundle ---
                    plans = await asyncio.to_thread(pipeline.process_change_bundle, bundle)

                    if plans:
                        approved_sections = []
                        injection_tasks   = []
                        total_plans       = len(plans)

                        # ── STAGE A: Generate ALL content in parallel ──
                        await manager.broadcast({
                            "type": "AGENT_STATE", "run_id": run_id, "state": "GENERATING"
                        })

                        async def safe_generate(p):
                            try:
                                return await asyncio.to_thread(generator.generate_content, p)
                            except Exception as e:
                                log.error(f"Generation failed for {p.section_number}: {e}", exc_info=True)
                                return None

                        generated_results = await asyncio.gather(
                            *[safe_generate(p) for p in plans]
                        )

                        # Pair plans with their generated content, skip failures
                        review_queue = [
                            (plan, content)
                            for plan, content in zip(plans, generated_results)
                            if content is not None
                        ]

                        if not review_queue:
                            log.warning(f"All generations failed for run {run_id}")
                            await manager.broadcast({
                                "type": "WORKFLOW_ALL_REJECTED", "run_id": run_id,
                            })
                            continue  # skip to next bundle

                        # ── STAGE B: Feed reviews one-at-a-time (pipelined) ──
                        for idx, (plan, generated_content) in enumerate(review_queue, start=1):
                            review_id = f"{run_id}_{plan.section_number}"

                            plan_db_changes = get_relevant_changes(plan, db_changes)
                            log.info(
                                f"REVIEW_REQUIRED for {plan.section_number} | "
                                f"db_changes count={len(plan_db_changes)}"
                            )

                            await manager.broadcast({
                                "type":           "REVIEW_REQUIRED",
                                "run_id":         run_id,
                                "review_id":      review_id,
                                "section_number": plan.section_number,
                                "title":          plan.title,
                                "new_text":       generated_content.generated_text,
                                "reasoning":      plan.pattern_reasoning,
                                "db_changes":     plan_db_changes,
                                "review_current": idx,
                                "review_total":   len(review_queue),
                            })

                            # Wait for this specific review
                            review_event = asyncio.Event()
                            pending_reviews[review_id] = review_event

                            try:
                                await asyncio.wait_for(review_event.wait(), timeout=300.0)
                            except asyncio.TimeoutError:
                                log.warning(
                                    f"Review timed out for {review_id}. Auto-rejecting."
                                )
                                review_decisions[review_id] = "REJECT"
                                await manager.broadcast({
                                    "type":    "WORKFLOW_REJECTED",
                                    "run_id":  run_id,
                                    "message": (
                                        f"Review timed out after 5 minutes. "
                                        f"Section {plan.section_number} auto-rejected."
                                    )
                                })

                            decision = review_decisions.pop(review_id, "REJECT")
                            pending_reviews.pop(review_id, None)

                            if decision == "APPROVE":
                                generated_content.status = "APPROVED"

                                approved_sections.append(SectionUpdate(
                                    section=generated_content.section_number,
                                    title=generated_content.section_title,
                                    content=generated_content.generated_text,
                                ))

                                # Fire injection in background — next review is served immediately
                                injection_tasks.append(
                                    asyncio.create_task(
                                        _inject_section(run_id, generated_content, bundle.changes)
                                    )
                                )

                                log.info(f"Override Granted for {plan.section_number}. Injecting in background...")

                            else:
                                generated_content.status = "REJECTED"
                                await manager.broadcast({
                                    "type":    "WORKFLOW_REJECTED",
                                    "run_id":  run_id,
                                    "message": f"Update for {plan.section_number} was rejected by user."
                                })

                        # ── STAGE C: Wait for all background injections ──
                        if injection_tasks:
                            await asyncio.gather(*injection_tasks)

                        # --- All rejected: no sections were approved ---
                        if not approved_sections:
                            log.info(f"All plans rejected for run {run_id} — broadcasting WORKFLOW_ALL_REJECTED")
                            await manager.broadcast({
                                "type":   "WORKFLOW_ALL_REJECTED",
                                "run_id": run_id,
                            })

                        # --- Generate PDF if any sections were approved ---
                        if approved_sections:
                            try:
                                await manager.broadcast({
                                    "type":   "AGENT_STATE",
                                    "run_id": run_id,
                                    "state":  "COMPILING_PDF"
                                })

                                registry_entry = next(
                                    (m for m in DOSSIER_REGISTRY if m.product_code == bundle.product_code),
                                    None
                                )

                                if registry_entry:
                                    manifest = EngineManifest.from_registry(registry_entry)
                                    pdf_path = await asyncio.to_thread(
                                        generate_updated_dossier,
                                        manifest,
                                        approved_sections
                                    )

                                    log.info(f"✅ PDF generated: {pdf_path}")

                                    original_pdf_name = Path(manifest.pdf_path).name
                                    new_pdf_name      = pdf_path.name

                                    await manager.broadcast({
                                        "type":         "WORKFLOW_COMPLETE",
                                        "run_id":       run_id,
                                        "product_code": bundle.product_code,
                                        # FIX: Routed to the /view/ subdirectory
                                        "original_pdf": f"/dossiers/view/{original_pdf_name}", 
                                        "new_pdf":      f"/pdfs/{new_pdf_name}"
                                    })
                                else:
                                    log.error(f"Product {bundle.product_code} not found in registry")

                            except Exception as e:
                                log.error(f"PDF generation failed: {e}", exc_info=True)
                                await manager.broadcast({
                                    "type":         "WORKFLOW_COMPLETE",
                                    "run_id":       run_id,
                                    "product_code": bundle.product_code,
                                    "original_pdf": "",
                                    "new_pdf":      ""
                                })

            await asyncio.sleep(10)

        except Exception as e:
            log.error(f"Agent Loop Error: {e}")
            await asyncio.sleep(5)


# ============================================================
# STARTUP / SHUTDOWN
# ============================================================

@app.on_event("startup")
async def startup_event():
    log.info("=== Starting Cipher DSG API Server ===")
    try:
        log.info("Connecting to SQL Server...")
        sql_client.connect()
        log.info("✅ Connected to SQL Server")

        log.info("Connecting to Neo4j...")
        neo4j_client.connect()
        log.info("✅ Connected to Neo4j")

        log.info("Starting autonomous agent background task...")
        asyncio.create_task(autonomous_agent_loop())
        log.info("✅ Autonomous agent loop started")

    except Exception as e:
        log.error(f"❌ Startup failed: {e}", exc_info=True)
        raise


@app.on_event("shutdown")
async def shutdown_event():
    log.info("Shutting down Cipher DSG API Server...")
    try:
        sql_client.close()
        neo4j_client.close()
        log.info("✅ Connections closed")
    except Exception as e:
        log.error(f"Error during shutdown: {e}")


# ============================================================
# WEBSOCKET ENDPOINT
# ============================================================

@app.websocket("/api/v1/stream")
async def websocket_endpoint(websocket: WebSocket):
    """The live stream connecting the UI to the AI brain."""
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)