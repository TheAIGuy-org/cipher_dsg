# api/app.py
 
import asyncio

import logging

from pathlib import Path

from typing import Dict, Any, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from fastapi.staticfiles import StaticFiles

from fastapi.responses import FileResponse

from pydantic import BaseModel

import uvicorn

import sys
 
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
 
from config.dossier_registry import DOSSIER_REGISTRY

from db.sql_client import get_sql_client

from db.poller import get_change_poller

from db.change_pipeline import ChangeDetectionPipeline

from graph.neo4j_client import client as neo4j_client

from db.dossier_injector import DossierInjector

from llm.content_generator import SectionContentGenerator

from updater.pdf_updater import run_prompt

from utils.logger import get_logger
 
BASE_DIR = Path(__file__).resolve().parent.parent
 
app = FastAPI(title="Cipher DSG Autonomous Agent API")
 
app.mount("/static",   StaticFiles(directory=str(BASE_DIR / "static")),               name="static")

app.mount("/dossiers", StaticFiles(directory=str(BASE_DIR / "data" / "dossiers")),    name="dossiers")

app.mount("/docx_outputs",  StaticFiles(directory=str(BASE_DIR / "data" / "docx_output")), name="docx_outputs")
 
log = get_logger("api")
 
# --- Initialize Core Components ---

sql_client = get_sql_client()

poller     = get_change_poller()

pipeline   = ChangeDetectionPipeline()

generator  = SectionContentGenerator()

injector   = DossierInjector(neo4j_client=neo4j_client)
 
 
# --- WebSocket Manager ---

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

        for connection in self.active_connections:

            try:

                await connection.send_json(message)

            except Exception:

                pass
 
 
manager = ConnectionManager()
 
# --- Global State for HITL Pause ---

pending_reviews: Dict[str, asyncio.Event] = {}

review_decisions: Dict[str, str] = {}
 
 
# --- API Models ---

class ReviewDecision(BaseModel):

    run_id: str

    decision: str  # "APPROVE" or "REJECT"
 
 
# --- REST Endpoints ---
 
@app.get("/")

async def serve_ui():

    return FileResponse(str(BASE_DIR / "static" / "index.html"))
 
 
@app.get("/api/v1/dossiers")

async def get_dossiers():

    return [

        {"product_code": d.product_code, "name": d.product_name}

        for d in DOSSIER_REGISTRY

    ]
 
 
@app.post("/api/v1/workflow/review")

async def submit_review(decision: ReviewDecision):

    run_id = decision.run_id

    if run_id in pending_reviews:

        review_decisions[run_id] = decision.decision

        pending_reviews[run_id].set()

        return {"status": "success", "message": f"Decision '{decision.decision}' recorded."}

    return {"status": "error", "message": "Run ID not found or already processed."}
 
 
# --- Background Autonomous Agent Task ---
 
async def autonomous_agent_loop():

    log.info("Starting autonomous agent loop...")
 
    while True:

        try:

            bundles = await asyncio.to_thread(poller.poll_once)
 
            if bundles:

                for bundle in bundles:

                    run_id = (

                        f"run_{bundle.product_code}_"

                        f"{int(asyncio.get_event_loop().time())}"

                    )
 
                    await manager.broadcast({

                        "type":         "IMPACT_DETECTED",

                        "run_id":       run_id,

                        "product_code": bundle.product_code,

                        "change_count": bundle.get_change_count(),

                    })
 
                    for state in ["POLLING", "INTERPRETING", "MAPPING"]:

                        await asyncio.sleep(1)

                        await manager.broadcast({

                            "type":   "AGENT_STATE",

                            "run_id": run_id,

                            "state":  state,

                        })
 
                    plans = await asyncio.to_thread(

                        pipeline.process_change_bundle, bundle

                    )
 
                    if not plans:

                        await manager.broadcast({

                            "type":         "WORKFLOW_COMPLETE",

                            "run_id":       run_id,

                            "product_code": bundle.product_code,

                            "original_pdf": "",

                            "new_docx":      "",

                            "docx_url":     "",

                            "message":      "No update plans were generated.",

                        })

                        continue
 
                    approved_contents: List[dict] = []

                    total_plans = len(plans)
 
                    for plan_idx, plan in enumerate(plans):

                        await manager.broadcast({

                            "type":   "AGENT_STATE",

                            "run_id": run_id,

                            "state":  "GENERATING",

                        })
 
                        try:

                            generated_content = await asyncio.to_thread(

                                generator.generate_content, plan

                            )

                        except Exception as e:

                            log.error(f"Content generation failed: {e}", exc_info=True)

                            # SECTION_ERROR: logs to console but does NOT reset UI to idle

                            await manager.broadcast({

                                "type":    "SECTION_ERROR",

                                "run_id":  run_id,

                                "message": f"Generation failed for {plan.section_number}: {e}",

                            })

                            continue
 
                        # HITL pause — send index/total so UI can show "2 of 5" progress

                        await manager.broadcast({

                            "type":           "REVIEW_REQUIRED",

                            "run_id":         run_id,

                            "section_number": plan.section_number,

                            "title":          plan.title,

                            "new_text":       generated_content.generated_text,

                            "reasoning":      plan.pattern_reasoning,

                            "plan_index":     plan_idx + 1,

                            "plan_total":     total_plans,

                        })
 
                        review_event = asyncio.Event()

                        pending_reviews[run_id] = review_event

                        await review_event.wait()
 
                        decision = review_decisions.pop(run_id, "REJECT")

                        del pending_reviews[run_id]
 
                        if decision == "APPROVE":

                            await manager.broadcast({

                                "type":   "AGENT_STATE",

                                "run_id": run_id,

                                "state":  "STORING",

                            })
 
                            generated_content.status = "APPROVED"
 
                            try:

                                result = await asyncio.to_thread(

                                    injector.inject_approved_content,

                                    content=generated_content,

                                    author="web_agent",

                                    comment=(

                                        f"Approved via UI – "

                                        f"{len(bundle.changes)} DB change(s)"

                                    ),

                                )

                                log.info(

                                    f"✅ Injected {generated_content.section_number} "

                                    f"– version {result.version_created}"

                                )

                                if result.errors:

                                    log.error(f"Injection errors: {result.errors}")
 
                            except Exception as e:

                                log.error(f"Injection failed: {e}", exc_info=True)

                                await manager.broadcast({

                                    "type":    "SECTION_ERROR",

                                    "run_id":  run_id,

                                    "message": f"Injection failed for {plan.section_number}: {e}",

                                })

                                continue
 
                            approved_contents.append({

                                "section_number": generated_content.section_number,

                                "section_title":  generated_content.section_title,

                                "product_code":   generated_content.product_code,

                                "generated_text": generated_content.generated_text,

                            })
 
                        else:

                            generated_content.status = "REJECTED"

                            # SECTION_REJECTED: logs to console, stays on workflow view,

                            # moves on to the next section — does NOT reset to idle

                            await manager.broadcast({

                                "type":    "SECTION_REJECTED",

                                "run_id":  run_id,

                                "message": f"Section {plan.section_number} rejected — moving to next.",

                            })
 
                    # --- Phase 11: DOCX generation (once per bundle) ---

                    if approved_contents:

                        await manager.broadcast({

                            "type":   "AGENT_STATE",

                            "run_id": run_id,

                            "state":  "COMPILING_PDF",

                        })
 
                        try:

                            await asyncio.to_thread(run_prompt, approved_contents)

                            log.info(

                                f"✅ DOCX generated for {bundle.product_code} "

                                f"({len(approved_contents)} section(s))"

                            )
 
                            await manager.broadcast({

                                "type":         "WORKFLOW_COMPLETE",

                                "run_id":       run_id,

                                "product_code": bundle.product_code,

                                # original_pdf + new_docx kept for app.js compatibility

                                "original_pdf": f"/dossiers/{bundle.product_code}.pdf",

                                "new_docx":      f"/docx_outputs/{bundle.product_code}_updated.docx",

                                "docx_url":     f"/docx_outputs/{bundle.product_code}_updated.docx",

                            })
 
                        except Exception as e:

                            log.error(f"DOCX generation failed: {e}", exc_info=True)

                            await manager.broadcast({

                                "type":         "WORKFLOW_COMPLETE",

                                "run_id":       run_id,

                                "product_code": bundle.product_code,

                                "original_pdf": "",

                                "new_docx":      "",

                                "docx_url":     "",

                                "warning":      f"DOCX generation failed: {e}",

                            })

                    else:

                        # Every section was rejected — end the run cleanly

                        await manager.broadcast({

                            "type":         "WORKFLOW_COMPLETE",

                            "run_id":       run_id,

                            "product_code": bundle.product_code,

                            "original_pdf": "",

                            "new_docx":      "",

                            "docx_url":     "",

                            "message":      "No sections approved — DOCX skipped.",

                        })
 
            await asyncio.sleep(10)
 
        except Exception as e:

            log.error(f"Agent loop error: {e}", exc_info=True)

            await asyncio.sleep(5)
 
 
@app.on_event("startup")

async def startup_event():

    log.info("=== Starting Cipher DSG API Server ===")

    try:

        sql_client.connect()

        log.info("✅ Connected to SQL Server")

        neo4j_client.connect()

        log.info("✅ Connected to Neo4j")

        asyncio.create_task(autonomous_agent_loop())

        log.info("✅ Autonomous agent loop started")

    except Exception as e:

        log.error(f"❌ Startup failed: {e}", exc_info=True)

        raise
 
 
@app.on_event("shutdown")

async def shutdown_event():

    try:

        sql_client.close()

        neo4j_client.close()

        log.info("✅ Connections closed")

    except Exception as e:

        log.error(f"Error during shutdown: {e}")
 
 
@app.websocket("/api/v1/stream")

async def websocket_endpoint(websocket: WebSocket):

    await manager.connect(websocket)

    try:

        while True:

            await websocket.receive_text()

    except WebSocketDisconnect:

        manager.disconnect(websocket)
 
 
if __name__ == "__main__":

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
 