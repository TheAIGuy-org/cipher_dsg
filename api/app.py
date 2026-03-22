# api/app.py
import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, Any, List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
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

# Serve frontend static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Serve original dossier PDFs
app.mount("/dossiers", StaticFiles(directory="data/dossiers"), name="dossiers")

# Serve generated/updated PDFs
app.mount("/pdfs", StaticFiles(directory="data/pdf_output"), name="pdfs")

log = get_logger("api")

# --- Initialize Core Components (matching run_agent_realtime.py) ---
sql_client = get_sql_client()
poller = get_change_poller()
pipeline = ChangeDetectionPipeline()
generator = SectionContentGenerator()
injector = DossierInjector(neo4j_client=neo4j_client)

# --- WebSocket Manager ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# --- Global State for HITL Pause ---
# Stores events to pause/resume the pipeline during human review
pending_reviews: Dict[str, asyncio.Event] = {}
review_decisions: Dict[str, str] = {} # 'APPROVED' or 'REJECTED'

# --- API Models ---
class ReviewDecision(BaseModel):
    run_id: str
    decision: str  # "APPROVE" or "REJECT"

# --- REST Endpoints ---

@app.get("/")
async def serve_ui():
    return FileResponse("static/index.html")

@app.get("/api/v1/dossiers")
async def get_dossiers():
    """Return available dossiers for View 1."""
    return [{"product_code": d.product_code, "name": d.product_name} for d in DOSSIER_REGISTRY]

@app.post("/api/v1/workflow/review")
async def submit_review(decision: ReviewDecision):
    """Handle HITL Approve/Reject decisions."""
    run_id = decision.run_id
    if run_id in pending_reviews:
        review_decisions[run_id] = decision.decision
        pending_reviews[run_id].set()  # Unblock the waiting pipeline!
        return {"status": "success", "message": f"Decision {decision.decision} recorded."}
    return {"status": "error", "message": "Run ID not found or already processed."}

# --- Background Autonomous Agent Task ---

async def autonomous_agent_loop():
    """Runs continuously, polls DB, and drives the LangGraph via WebSockets."""
    log.info("Starting autonomous agent loop...")
    
    while True:
        try:
            # 1. Poll DB for changes (Running synchronous code in a thread)
            bundles = await asyncio.to_thread(poller.poll_once)
            
            if bundles:
                for bundle in bundles:
                    run_id = f"run_{bundle.product_code}_{int(asyncio.get_event_loop().time())}"
                    
                    # 2. Impact Detected - Broadcast to UI (Switches to View 2)
                    await manager.broadcast({
                        "type": "IMPACT_DETECTED",
                        "run_id": run_id,
                        "product_code": bundle.product_code,
                        "change_count": bundle.get_change_count()
                    })
                    
                    # 3. Stream Pipeline States (Simulating LangGraph steps for UI visibility)
                    states = ["POLLING", "INTERPRETING", "MAPPING", "GENERATING"]
                    for state in states:
                        await asyncio.sleep(1) # Visual pacing
                        await manager.broadcast({"type": "AGENT_STATE", "run_id": run_id, "state": state})
                        
                    # 4. Actually process the pipeline (Phases 3-7)
                    plans = await asyncio.to_thread(pipeline.process_change_bundle, bundle)
                    
                    if plans:
                        approved_sections = []  # Collect approved sections for PDF generation
                        
                        for plan in plans:
                            # Phase 9: Generate actual content
                            await manager.broadcast({"type": "AGENT_STATE", "run_id": run_id, "state": "GENERATING"})
                            
                            try:
                                generated_content = await asyncio.to_thread(generator.generate_content, plan)
                                
                                # 5. REVIEW STATE - Pause for Human-in-the-Loop
                                await manager.broadcast({
                                    "type": "REVIEW_REQUIRED",
                                    "run_id": run_id,
                                    "section_number": plan.section_number,
                                    "title": plan.title,
                                    "new_text": generated_content.generated_text,  # Full generated content
                                    "reasoning": plan.pattern_reasoning
                                })
                                
                                # Pause execution until REST API sets the event
                                review_event = asyncio.Event()
                                pending_reviews[run_id] = review_event
                                await review_event.wait() 
                                
                                # 6. Check Decision
                                decision = review_decisions.pop(run_id, "REJECT")
                                del pending_reviews[run_id]
                                
                                if decision == "APPROVE":
                                    # Phase 10: Inject to Neo4j
                                    await manager.broadcast({"type": "AGENT_STATE", "run_id": run_id, "state": "STORING"})
                                    
                                    # CRITICAL: Set status to APPROVED (matching run_agent_realtime.py)
                                    generated_content.status = "APPROVED"
                                    
                                    result = await asyncio.to_thread(
                                        injector.inject_approved_content,
                                        content=generated_content,
                                        author="web_agent",
                                        comment=f"Approved via UI - {len(bundle.changes)} DB change(s)"
                                    )
                                    
                                    log.info(f"✅ Injected section {generated_content.section_number} - Version {result.version_created}")
                                    
                                    # Log injection details for debugging
                                    if result.errors:
                                        log.error(f"⚠️ Injection errors: {result.errors}")
                                    else:
                                        log.info(f"   Sections created: {len(result.sections_created)}")
                                        log.info(f"   Sections updated: {len(result.sections_updated)}")
                                        log.info(f"   Sections renumbered: {len(result.sections_renumbered)}")
                                    
                                    # Queue for PDF generation
                                    approved_sections.append(SectionUpdate(
                                        section=generated_content.section_number,
                                        title=generated_content.section_title,
                                        content=generated_content.generated_text,
                                    ))
                                    
                                else:
                                    generated_content.status = "REJECTED"
                                    await manager.broadcast({
                                        "type": "WORKFLOW_REJECTED",
                                        "run_id": run_id,
                                        "message": f"Update for {plan.section_number} was rejected by user."
                                    })
                                    
                            except Exception as e:
                                log.error(f"Content generation/injection failed: {e}", exc_info=True)
                                await manager.broadcast({
                                    "type": "WORKFLOW_REJECTED",
                                    "run_id": run_id,
                                    "message": f"Error: {str(e)}"
                                })
                        
                        # Phase 11: Generate PDF if any sections were approved
                        if approved_sections:
                            try:
                                await manager.broadcast({"type": "AGENT_STATE", "run_id": run_id, "state": "COMPILING_PDF"})
                                
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
                                    
                                    # Get original PDF path from manifest
                                    original_pdf_name = Path(manifest.pdf_path).name
                                    new_pdf_name = pdf_path.name
                                    
                                    await manager.broadcast({
                                        "type": "WORKFLOW_COMPLETE",
                                        "run_id": run_id,
                                        "product_code": bundle.product_code,
                                        "original_pdf": f"/dossiers/{original_pdf_name}",
                                        "new_pdf": f"/pdfs/{new_pdf_name}"
                                    })
                                else:
                                    log.error(f"Product {bundle.product_code} not found in registry")
                                    
                            except Exception as e:
                                log.error(f"PDF generation failed: {e}", exc_info=True)
                                # Still consider it success since graph was updated
                                await manager.broadcast({
                                    "type": "WORKFLOW_COMPLETE",
                                    "run_id": run_id,
                                    "product_code": bundle.product_code,
                                    "original_pdf": "",
                                    "new_pdf": ""
                                })

            # Poll every 10 seconds
            await asyncio.sleep(10)
            
        except Exception as e:
            log.error(f"Agent Loop Error: {e}")
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    """Initialize connections and start the autonomous agent loop in the background."""
    log.info("=== Starting Cipher DSG API Server ===")
    
    try:
        # Connect to databases (matching run_agent_realtime.py startup)
        log.info("Connecting to SQL Server...")
        sql_client.connect()
        log.info("✅ Connected to SQL Server")
        
        log.info("Connecting to Neo4j...")
        neo4j_client.connect()
        log.info("✅ Connected to Neo4j")
        
        # Start the autonomous agent loop
        log.info("Starting autonomous agent background task...")
        asyncio.create_task(autonomous_agent_loop())
        log.info("✅ Autonomous agent loop started")
        
    except Exception as e:
        log.error(f"❌ Startup failed: {e}", exc_info=True)
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up connections on shutdown."""
    log.info("Shutting down Cipher DSG API Server...")
    try:
        sql_client.close()
        neo4j_client.close()
        log.info("✅ Connections closed")
    except Exception as e:
        log.error(f"Error during shutdown: {e}")

@app.websocket("/api/v1/stream")
async def websocket_endpoint(websocket: WebSocket):
    """The live stream connecting the UI to the AI brain."""
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text() # Keep connection alive
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)