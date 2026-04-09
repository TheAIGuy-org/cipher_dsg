# api/app.py
import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
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

# --- Multi-product staggered pipeline state ---
product_states: Dict[str, str] = {}                # run_id -> GENERATING|AWAITING_REVIEW|IN_REVIEW|COMPLETE|REJECTED
foreground_gates: Dict[str, asyncio.Event] = {}    # run_id -> Event, set when user clicks [Next Product]
_background_generation: Dict[str, tuple] = {}      # product_code -> (review_queue, db_changes, run_id)

# --- State hydration: last REVIEW_REQUIRED payload for F5 recovery ---
_active_review_payload: Optional[dict] = None

# --- API Models ---
class ReviewDecision(BaseModel):
    review_id: str
    decision: str  # "APPROVE" or "REJECT"


# ============================================================
# HELPER: Serialize bundle.changes into a JSON-safe list
# ============================================================

def serialize_db_changes(bundle) -> List[Dict[str, Any]]:
    """Convert bundle.changes (List[DBChangeRecord]) into a list of plain dicts."""
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
                f"| old={c.old_value!r} -> new={c.new_value!r}"
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
# CONCEPT -> TABLE TAXONOMY MAP
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

    implied_tables: set = set()
    for cc in plan.concept_changes:
        concept_lower = cc.concept.lower()
        change_type_lower = cc.change_type.lower()
        search_text = f"{concept_lower} {change_type_lower}"

        for keyword, tables in CONCEPT_TO_TABLES.items():
            if keyword in search_text:
                implied_tables.update(tables)

    log.debug(
        f"get_relevant_changes: plan {plan.section_number} -> "
        f"implied tables={implied_tables}"
    )

    if implied_tables:
        relevant = [
            c for c in all_changes
            if c.get("source_table") in implied_tables
        ]
        if relevant:
            log.info(
                f"get_relevant_changes: {plan.section_number} -> "
                f"{len(relevant)}/{len(all_changes)} matched via taxonomy"
            )
            return relevant

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
    """Return available dossiers. Only includes entries whose PDF exists on disk."""
    result = []
    for d in DOSSIER_REGISTRY:
        if d.pdf_path.exists():
            result.append({
                "product_code": d.product_code,
                "name":         d.product_name,
                "pdf_url":      f"/dossiers/{d.pdf_filename}"
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


@app.post("/api/v1/workflow/advance")
async def advance_to_next_product():
    """User is done viewing the current product's PDF. Unblock the next product."""
    for run_id, event in foreground_gates.items():
        if not event.is_set():
            event.set()
            log.info(f"Advance signal received — unblocking after {run_id}")
            return {"status": "success", "message": "Advancing to next product."}
    return {"status": "noop", "message": "No product pending advance."}


# ============================================================
# BACKGROUND AUTONOMOUS AGENT LOOP
# ============================================================

async def _safe_generate(p):
    """Generate content for a single plan, returning None on failure."""
    try:
        return await asyncio.to_thread(generator.generate_content, p)
    except Exception as e:
        log.error(f"Generation failed for {p.section_number}: {e}", exc_info=True)
        return None


async def _generate_for_product(bundle, queue_position: int, total_products: int):
    """Run pipeline + LLM generation for a product in the background.

    Called while the user views the previous product's final PDF.
    Results are stored in _background_generation for the main loop to pick up.
    """
    run_id = f"run_{bundle.product_code}_{int(asyncio.get_running_loop().time())}"
    product_states[run_id] = "GENERATING"

    await manager.broadcast({
        "type":         "IMPACT_DETECTED",
        "run_id":       run_id,
        "product_code": bundle.product_code,
        "change_count": bundle.get_change_count(),
        "queue_position": queue_position,
        "total_products": total_products,
    })

    for state in ["POLLING", "INTERPRETING", "MAPPING", "GENERATING"]:
        await asyncio.sleep(1)
        await manager.broadcast({
            "type": "AGENT_STATE", "run_id": run_id, "state": state
        })

    plans = await asyncio.to_thread(pipeline.process_change_bundle, bundle)
    db_changes = serialize_db_changes(bundle)

    if plans:
        await manager.broadcast({
            "type": "AGENT_STATE", "run_id": run_id, "state": "GENERATING"
        })
        generated_results = await asyncio.gather(
            *[_safe_generate(p) for p in plans]
        )
        review_queue = [
            (plan, content)
            for plan, content in zip(plans, generated_results)
            if content is not None
        ]
        _background_generation[bundle.product_code] = (review_queue, db_changes, run_id)
    else:
        _background_generation[bundle.product_code] = ([], db_changes, run_id)

    product_states[run_id] = "AWAITING_REVIEW"
    await manager.broadcast({
        "type":         "PRODUCT_READY",
        "run_id":       run_id,
        "product_code": bundle.product_code,
        "queue_position": queue_position,
    })


async def _inject_all_sections(run_id: str, approved_contents: list, bundle_changes):
    """Silently inject all approved sections to Neo4j in the background.

    Runs AFTER PDF generation, while the user is examining the final PDFs.
    No WebSocket broadcasts — completely invisible to the UI.
    """
    for gc in approved_contents:
        try:
            result = await asyncio.to_thread(
                injector.inject_approved_content,
                content=gc,
                author="web_agent",
                comment=f"Approved via UI - {len(bundle_changes)} DB change(s)"
            )
            log.info(
                f"Injected section {gc.section_number} "
                f"- Version {result.version_created}"
            )
            if result.errors:
                log.error(f"Injection errors for {gc.section_number}: {result.errors}")
        except Exception as e:
            log.error(f"Injection failed for {gc.section_number}: {e}", exc_info=True)


async def autonomous_agent_loop():
    """Runs continuously, polls DB, and drives the pipeline via WebSockets.

    Multi-product support:
      - Single bundle  -> identical to original behavior
      - Multiple bundles -> staggered: Product N+1's LLM generation runs in the
        background while the user views Product N's final PDF.  Only one product's
        LLM calls run at a time (TPM-safe).
    """
    log.info("Starting autonomous agent loop...")

    while True:
        try:
            bundles = await asyncio.to_thread(poller.poll_once)

            if bundles:
                total = len(bundles)
                bg_task = None  # tracks the one background generation task

                # Announce multi-product mode to frontend
                if total > 1:
                    log.info(f"Multi-product batch: {total} bundles detected")
                    await manager.broadcast({
                        "type":          "MULTI_PRODUCT_DETECTED",
                        "product_codes": [b.product_code for b in bundles],
                        "count":         total,
                    })

                for idx, bundle in enumerate(bundles):
                    is_last = (idx == total - 1)
                    next_bundle = bundles[idx + 1] if not is_last else None

                    # -- Wait for background generation from previous iteration --
                    if bg_task is not None:
                        await bg_task
                        bg_task = None

                    # -- Check if background already produced results --
                    if bundle.product_code in _background_generation:
                        review_queue, db_changes, run_id = _background_generation.pop(bundle.product_code)
                        log.info(
                            f"Using pre-generated content for {bundle.product_code} "
                            f"(run_id={run_id}, sections={len(review_queue)})"
                        )
                    else:
                        # -- Full pipeline + generation (same as single-product) --
                        run_id = f"run_{bundle.product_code}_{int(asyncio.get_running_loop().time())}"

                        await manager.broadcast({
                            "type":         "IMPACT_DETECTED",
                            "run_id":       run_id,
                            "product_code": bundle.product_code,
                            "change_count": bundle.get_change_count(),
                            "queue_position": idx,
                            "total_products": total,
                        })

                        for state in ["POLLING", "INTERPRETING", "MAPPING", "GENERATING"]:
                            await asyncio.sleep(1)
                            await manager.broadcast({
                                "type": "AGENT_STATE", "run_id": run_id, "state": state
                            })

                        plans = await asyncio.to_thread(pipeline.process_change_bundle, bundle)
                        db_changes = serialize_db_changes(bundle)

                        if plans:
                            await manager.broadcast({
                                "type": "AGENT_STATE", "run_id": run_id, "state": "GENERATING"
                            })
                            generated_results = await asyncio.gather(
                                *[_safe_generate(p) for p in plans]
                            )
                            review_queue = [
                                (plan, content)
                                for plan, content in zip(plans, generated_results)
                                if content is not None
                            ]
                        else:
                            review_queue = []

                    # -- Handle empty review queue --
                    product_states[run_id] = "IN_REVIEW"

                    if not review_queue:
                        log.warning(f"No content generated for run {run_id}")
                        product_states[run_id] = "REJECTED"
                        await manager.broadcast({
                            "type": "WORKFLOW_ALL_REJECTED", "run_id": run_id,
                        })
                        continue  # next bundle processes from scratch

                    # -- STAGE B: Feed reviews one-at-a-time --
                    approved_sections = []
                    approved_contents = []

                    for rev_idx, (plan, generated_content) in enumerate(review_queue, start=1):
                        review_id = f"{run_id}_{plan.section_number}"

                        plan_db_changes = get_relevant_changes(plan, db_changes)
                        log.info(
                            f"REVIEW_REQUIRED for {plan.section_number} | "
                            f"db_changes count={len(plan_db_changes)}"
                        )

                        global _active_review_payload
                        _active_review_payload = {
                            "type":           "REVIEW_REQUIRED",
                            "run_id":         run_id,
                            "review_id":      review_id,
                            "section_number": plan.section_number,
                            "title":          plan.title,
                            "new_text":       generated_content.generated_text,
                            "reasoning":      plan.pattern_reasoning,
                            "db_changes":     plan_db_changes,
                            "review_current": rev_idx,
                            "review_total":   len(review_queue),
                        }
                        await manager.broadcast(_active_review_payload)

                        review_event = asyncio.Event()
                        pending_reviews[review_id] = review_event

                        try:
                            await asyncio.wait_for(review_event.wait(), timeout=300.0)
                        except asyncio.TimeoutError:
                            log.warning(f"Review timed out for {review_id}. Auto-rejecting.")
                            review_decisions[review_id] = "REJECT"
                            await manager.broadcast({
                                "type":    "PLAN_REJECTED",
                                "run_id":  run_id,
                                "section": plan.section_number,
                                "message": (
                                    f"Review timed out after 5 minutes. "
                                    f"Section {plan.section_number} auto-rejected."
                                )
                            })
                        finally:
                            pending_reviews.pop(review_id, None)

                        _active_review_payload = None
                        decision = review_decisions.pop(review_id, "REJECT")

                        if decision == "APPROVE":
                            generated_content.status = "APPROVED"
                            approved_sections.append(SectionUpdate(
                                section=generated_content.section_number,
                                title=generated_content.section_title,
                                content=generated_content.generated_text,
                            ))
                            approved_contents.append(generated_content)
                            await manager.broadcast({
                                "type":    "PLAN_APPROVED",
                                "run_id":  run_id,
                                "section": plan.section_number,
                                "message": f"Update for {plan.section_number} approved."
                            })
                            log.info(f"Override Granted for {plan.section_number}.")
                        else:
                            generated_content.status = "REJECTED"
                            await manager.broadcast({
                                "type":    "PLAN_REJECTED",
                                "run_id":  run_id,
                                "section": plan.section_number,
                                "message": f"Update for {plan.section_number} was rejected by user."
                            })

                    # -- STAGE C: All reviews done --
                    if not approved_sections:
                        log.info(f"All plans rejected for run {run_id}")
                        product_states[run_id] = "REJECTED"
                        await manager.broadcast({
                            "type": "WORKFLOW_ALL_REJECTED", "run_id": run_id,
                        })
                        continue  # next bundle processes from scratch

                    # -- STAGE D: Generate PDF --
                    try:
                        await manager.broadcast({
                            "type": "AGENT_STATE", "run_id": run_id, "state": "COMPILING_PDF"
                        })

                        registry_entry = next(
                            (m for m in DOSSIER_REGISTRY if m.product_code == bundle.product_code),
                            None
                        )

                        if registry_entry:
                            manifest = EngineManifest.from_registry(registry_entry)
                            pdf_path = await asyncio.to_thread(
                                generate_updated_dossier, manifest, approved_sections
                            )
                            log.info(f"PDF generated: {pdf_path}")

                            original_pdf_name = Path(manifest.pdf_path).name
                            new_pdf_name      = pdf_path.name

                            await manager.broadcast({
                                "type":         "WORKFLOW_COMPLETE",
                                "run_id":       run_id,
                                "product_code": bundle.product_code,
                                "original_pdf": f"/dossiers/{original_pdf_name}",
                                "new_pdf":      f"/pdfs/{new_pdf_name}"
                            })
                        else:
                            log.error(f"Product {bundle.product_code} not found in registry")
                            await manager.broadcast({
                                "type":         "WORKFLOW_COMPLETE",
                                "run_id":       run_id,
                                "product_code": bundle.product_code,
                                "original_pdf": "",
                                "new_pdf":      ""
                            })

                    except Exception as e:
                        log.error(f"PDF generation failed: {e}", exc_info=True)
                        await manager.broadcast({
                            "type":         "WORKFLOW_COMPLETE",
                            "run_id":       run_id,
                            "product_code": bundle.product_code,
                            "original_pdf": "",
                            "new_pdf":      ""
                        })

                    # -- STAGE E: Silent Neo4j injection (background) --
                    asyncio.create_task(
                        _inject_all_sections(run_id, approved_contents, bundle.changes)
                    )

                    # -- STAGE F: Multi-product stagger --
                    # Start next product's generation while user views this PDF,
                    # then gate the for-loop until user clicks [Next Product].
                    if total > 1 and next_bundle:
                        product_states[run_id] = "COMPLETE_PENDING_ADVANCE"
                        bg_task = asyncio.create_task(
                            _generate_for_product(next_bundle, idx + 1, total)
                        )
                        advance_event = asyncio.Event()
                        foreground_gates[run_id] = advance_event
                        await advance_event.wait()
                        foreground_gates.pop(run_id, None)
                        product_states[run_id] = "COMPLETE"

                # -- Cleanup multi-product state --
                if total > 1:
                    if bg_task is not None:
                        await bg_task
                    await manager.broadcast({"type": "ALL_PRODUCTS_COMPLETE"})
                    product_states.clear()
                    foreground_gates.clear()
                    _background_generation.clear()

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
        log.info("Connected to SQL Server")

        log.info("Connecting to Neo4j...")
        neo4j_client.connect()
        log.info("Connected to Neo4j")

        log.info("Starting autonomous agent background task...")
        asyncio.create_task(autonomous_agent_loop())
        log.info("Autonomous agent loop started")

    except Exception as e:
        log.error(f"Startup failed: {e}", exc_info=True)
        raise


@app.on_event("shutdown")
async def shutdown_event():
    log.info("Shutting down Cipher DSG API Server...")
    try:
        sql_client.disconnect()
        neo4j_client.close()
        log.info("Connections closed")
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
        # State hydration: if a review is pending, re-send it to the new client
        if _active_review_payload is not None:
            await websocket.send_json(_active_review_payload)
            log.info(f"Hydrated new WS client with pending review: {_active_review_payload.get('review_id')}")
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
