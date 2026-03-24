// static/app.js

let currentRunId = null;
 
const views = {

    idle:     document.getElementById('view-idle'),

    workflow: document.getElementById('view-workflow'),

    review:   document.getElementById('view-review'),

    final:    document.getElementById('view-final')

};
 
const consoleOut  = document.getElementById('console-output');

const statusBadge = document.getElementById('status-badge');

const coreDot     = document.getElementById('core-status-dot');

const corePing    = document.getElementById('core-status-ping');
 
// --- Initialization ---

async function init() {

    const grid = document.getElementById('dossier-grid');
 
    grid.innerHTML = `
<div class="col-span-full flex flex-col items-center justify-center p-10 text-cyan-500/50">
<div class="w-8 h-8 border-2 border-cyan-500 border-t-transparent rounded-full animate-spin mb-4"></div>
<p class="font-mono text-xs tracking-widest uppercase">Establishing secure link to Registry...</p>
</div>

    `;
 
    try {

        const res = await fetch('/api/v1/dossiers');

        if (!res.ok) throw new Error(`API returned status: ${res.status}`);
 
        const dossiers = await res.json();

        grid.innerHTML = '';
 
        if (dossiers.length === 0) {

            grid.innerHTML = `<div class="col-span-full text-slate-500 font-mono text-sm">No dossiers found in registry.</div>`;

            return;

        }
 
        dossiers.forEach(d => {

            grid.innerHTML += `
<div class="glass-panel p-6 rounded-xl hover:bg-slate-800/50 transition-colors border-l-2 border-emerald-500/50 flex flex-col justify-between h-40 group relative overflow-hidden">
<div class="absolute inset-0 bg-gradient-to-r from-emerald-500/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity"></div>
<div class="relative z-10">
<div class="flex justify-between items-start mb-2">
<h3 class="font-bold text-white text-sm leading-tight group-hover:text-emerald-400 transition-colors">${d.name}</h3>
<svg class="w-5 h-5 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path></svg>
</div>
<p class="text-[10px] font-mono text-slate-400">ID: ${d.product_code}</p>
</div>
<div class="mt-4 flex items-center gap-2 relative z-10">
<span class="w-1.5 h-1.5 bg-emerald-400 rounded-full animate-pulse shadow-[0_0_5px_#34d399]"></span>
<span class="text-[10px] uppercase tracking-widest text-emerald-400/80 font-mono">Secured</span>
</div>
</div>

            `;

        });
 
        connectWebSocket();
 
    } catch (error) {

        console.error("Failed to load dossiers:", error);

        grid.innerHTML = `
<div class="col-span-full glass-panel border-rose-500/30 p-6 rounded-xl text-rose-400 font-mono text-sm flex flex-col items-center text-center">
<svg class="w-8 h-8 mb-3 text-rose-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>

                CONNECTION FAILED<br>
<span class="text-slate-400 text-xs mt-2 uppercase tracking-widest">Ensure you are accessing via http://localhost:8000 and not a local file:// path.</span>
</div>

        `;

    }

}
 
// --- WebSocket (extracted so it can be reconnected) ---

function connectWebSocket() {

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';

    const ws = new WebSocket(`${protocol}//${window.location.host}/api/v1/stream`);
 
    ws.onopen = () => {

        logToConsole('> [SYS] WebSocket connected. Listening for agent events...', 'text-emerald-400');

    };
 
    ws.onmessage = (event) => {

        const data = JSON.parse(event.data);

        handleAgentEvent(data);

    };
 
    ws.onerror = () => {

        logToConsole('> [SYS_ERR] WebSocket error. Attempting reconnect...', 'text-rose-500');

    };
 
    ws.onclose = () => {

        logToConsole('> [SYS] WebSocket closed. Reconnecting in 3s...', 'text-slate-500');

        setTimeout(connectWebSocket, 3000);

    };

}
 
// --- Event Handling ---

function handleAgentEvent(data) {
 
    // ── IMPACT_DETECTED ───────────────────────────────────────────────────────

    if (data.type === 'IMPACT_DETECTED') {

        currentRunId = data.run_id;
 
        setStatus('alert');

        document.getElementById('wf-product').innerText = data.product_code;

        document.getElementById('wf-trigger').innerText = `${data.change_count} DB UPDATE(s)`;

        logToConsole(`> [SYS] Threat radar triggered. ${data.change_count} change(s) detected for ${data.product_code}.`, 'text-rose-400');

        switchView('workflow');

    }
 
    // ── AGENT_STATE ───────────────────────────────────────────────────────────

    if (data.type === 'AGENT_STATE') {

        document.querySelectorAll('.step-indicator').forEach(el => {

            el.classList.remove('text-cyan-400', 'text-glow', 'opacity-100');

            el.classList.add('opacity-40');

        });
 
        const activeStep = document.getElementById(`step-${data.state}`);

        if (activeStep) {

            activeStep.classList.remove('opacity-40');

            activeStep.classList.add('text-cyan-400', 'text-glow', 'opacity-100');

        }
 
        logToConsole(`> [AGENT] Protocol: ${data.state}`, 'text-cyan-300');

    }
 
    // ── REVIEW_REQUIRED ───────────────────────────────────────────────────────

    if (data.type === 'REVIEW_REQUIRED') {

        currentRunId = data.run_id;
 
        document.getElementById('rev-section-name').innerText =

            `${data.section_number} — ${data.title}`;

        document.getElementById('rev-reasoning').innerText = data.reasoning || '—';

        document.getElementById('rev-new').innerText = data.new_text;
 
        // Show progress if multiple sections

        const progressEl = document.getElementById('rev-progress');

        if (progressEl && data.plan_total > 1) {

            progressEl.innerText = `Section ${data.plan_index} of ${data.plan_total}`;

            progressEl.classList.remove('hidden');

        }
 
        setStatus('paused');

        logToConsole(

            `> [HITL] Review required: ${data.section_number} (${data.plan_index}/${data.plan_total})`,

            'text-amber-400'

        );

        switchView('review');

    }
 
    // ── SECTION_REJECTED (single section, not fatal — stay on workflow) ───────

    if (data.type === 'SECTION_REJECTED') {

        logToConsole(`> [AUTH] Rejected: ${data.message}`, 'text-rose-400');

        // Stay on workflow view — next REVIEW_REQUIRED or WORKFLOW_COMPLETE will follow

        switchView('workflow');

    }
 
    // ── SECTION_ERROR (non-fatal error — log and continue) ───────────────────

    if (data.type === 'SECTION_ERROR') {

        logToConsole(`> [ERR] ${data.message}`, 'text-rose-500');

        switchView('workflow');

    }
 
    // ── WORKFLOW_COMPLETE ─────────────────────────────────────────────────────

    if (data.type === 'WORKFLOW_COMPLETE') {

        const downloadBtn = document.getElementById('download-btn');
 
        if (data.docx_url) {

            downloadBtn.href = data.docx_url;

            downloadBtn.innerHTML = `
<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"

                          d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"></path>
</svg>

                Download DOCX

            `;

            downloadBtn.classList.remove('hidden');

        } else {

            downloadBtn.classList.add('hidden');

        }
 
        // Iframes can't render DOCX — hide them and show a status message

        const origFrame = document.getElementById('pdf-orig');

        const newFrame  = document.getElementById('pdf-new');
 
        if (data.original_pdf) {

            origFrame.src = data.original_pdf;

            origFrame.classList.remove('hidden');

        } else {

            origFrame.classList.add('hidden');

        }
 
        // DOCX can't render in iframe — replace with a success card

        newFrame.classList.add('hidden');

        const newFrameParent = newFrame.parentElement;

        let successCard = newFrameParent.querySelector('.docx-success-card');

        if (!successCard) {

            successCard = document.createElement('div');

            successCard.className = 'docx-success-card flex-1 flex flex-col items-center justify-center gap-4 text-emerald-400';

            newFrameParent.appendChild(successCard);

        }

        successCard.innerHTML = data.docx_url

            ? `<svg class="w-16 h-16 text-emerald-400/50" fill="none" stroke="currentColor" viewBox="0 0 24 24">
<path stroke-linecap="round" stroke-linejoin="round" stroke-width="1"

                         d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
</svg>
<p class="font-mono text-sm text-emerald-400">DOCX compiled successfully</p>
<p class="font-mono text-xs text-slate-500">${data.product_code} — Use download button above</p>`

            : `<p class="font-mono text-sm text-slate-500">${data.message || 'No DOCX generated'}</p>`;
 
        if (data.warning) {

            logToConsole(`> [WARN] ${data.warning}`, 'text-amber-400');

        }
 
        const msg = data.docx_url

            ? `> [SYS] Dossier compiled. Download ready.`

            : `> [SYS] Run complete. ${data.message || ''}`;

        logToConsole(msg, 'text-cyan-400 font-bold');
 
        setStatus('success');

        switchView('final');

    }
 
    // ── WORKFLOW_REJECTED (fatal — something went very wrong, reset) ──────────

    if (data.type === 'WORKFLOW_REJECTED') {

        logToConsole(`> [FATAL] ${data.message}`, 'text-rose-500 font-bold');

        resetToIdle();

    }

}
 
// --- Actions ---

async function submitReview(decision) {

    if (!currentRunId) {

        logToConsole('> [ERR] No active run ID — cannot submit review.', 'text-rose-500');

        return;

    }
 
    const res = await fetch('/api/v1/workflow/review', {

        method:  'POST',

        headers: { 'Content-Type': 'application/json' },

        body:    JSON.stringify({ run_id: currentRunId, decision }),

    });
 
    if (res.ok) {

        const verb = decision === 'APPROVE' ? 'Approved' : 'Rejected';

        logToConsole(`> [AUTH] ${verb}. Resuming agent...`, 'text-emerald-400 font-bold');

        switchView('workflow');

    } else {

        logToConsole('> [ERR] Failed to submit review decision.', 'text-rose-500');

    }

}
 
// --- Status helpers ---

function setStatus(mode) {

    const modes = {

        idle: {

            badge: "font-mono px-4 py-1.5 bg-emerald-500/10 text-emerald-400 text-xs font-semibold rounded border border-emerald-500/30 shadow-[0_0_10px_rgba(16,185,129,0.1)] transition-all duration-300",

            text:  "[ SYS_IDLE : LISTENING_TELEMETRY ]",

            dot:   "relative w-3 h-3 bg-emerald-400 rounded-full shadow-[0_0_8px_#34d399]",

            ping:  "absolute w-full h-full bg-emerald-500 rounded-full animate-ping opacity-20",

        },

        alert: {

            badge: "font-mono px-4 py-1.5 bg-rose-500/10 text-rose-400 text-xs font-semibold rounded border border-rose-500/50 shadow-[0_0_15px_rgba(244,63,94,0.3)] transition-all duration-300",

            text:  "[ ALERT : DB_ANOMALY_DETECTED ]",

            dot:   "relative w-3 h-3 bg-rose-400 rounded-full shadow-[0_0_8px_#fb7185]",

            ping:  "absolute w-full h-full bg-rose-500 rounded-full animate-ping opacity-30",

        },

        paused: {

            badge: "font-mono px-4 py-1.5 bg-amber-500/10 text-amber-400 text-xs font-semibold rounded border border-amber-500/50 shadow-[0_0_15px_rgba(245,158,11,0.2)] transition-all duration-300",

            text:  "[ PAUSED : AWAITING_AUTHORIZATION ]",

            dot:   "relative w-3 h-3 bg-amber-400 rounded-full shadow-[0_0_8px_#fbbf24]",

            ping:  "absolute w-full h-full bg-amber-500 rounded-full animate-ping opacity-20",

        },

        success: {

            badge: "font-mono px-4 py-1.5 bg-cyan-500/10 text-cyan-400 text-xs font-semibold rounded border border-cyan-500/50 shadow-[0_0_15px_rgba(6,182,212,0.2)] transition-all duration-300",

            text:  "[ SUCCESS : DOSSIER_COMPILED ]",

            dot:   "relative w-3 h-3 bg-cyan-400 rounded-full shadow-[0_0_8px_#22d3ee]",

            ping:  "absolute w-full h-full bg-cyan-500 rounded-full animate-ping opacity-20",

        },

    };
 
    const m = modes[mode] || modes.idle;

    statusBadge.className = m.badge;

    statusBadge.innerText = m.text;

    coreDot.className     = m.dot;

    corePing.className    = m.ping;

}
 
// --- Utils ---

function switchView(viewName) {

    Object.keys(views).forEach(key => {

        const el = views[key];

        el.classList.add('opacity-0');

        setTimeout(() => {

            el.classList.add('hidden');

            el.classList.remove('translate-y-0');

            el.classList.add('translate-y-4');

        }, 300);

    });
 
    const target = views[viewName];

    setTimeout(() => {

        target.classList.remove('hidden');

        setTimeout(() => {

            target.classList.remove('opacity-0', 'translate-y-4');

            target.classList.add('translate-y-0');

        }, 50);

    }, 300);

}
 
function logToConsole(msg, colorClass = 'text-slate-300') {

    const div  = document.createElement('div');

    div.className = `console-line ${colorClass}`;

    const time = new Date().toLocaleTimeString('en-US', { hour12: false });

    div.innerHTML = `<span class="text-slate-600 mr-2">[${time}]</span> ${msg}`;

    consoleOut.appendChild(div);

    consoleOut.scrollTop = consoleOut.scrollHeight;

}
 
function resetToIdle() {

    setStatus('idle');

    switchView('idle');

}
 
function finishWorkflow() {

    logToConsole('> [USER] Workflow complete. Returning to monitoring mode...', 'text-cyan-400');

    resetToIdle();

}
 
// Start

init();
 