// static/app.js
let currentRunId = null;
let globalDossiers = []; // Store dossiers globally for dock switching
let logLines = [];       // Rolling terminal array

const views = {
    idle: document.getElementById('view-idle'),
    workflow: document.getElementById('view-workflow'),
    review: document.getElementById('view-review'),
    final: document.getElementById('view-final')
};

const consoleOut = document.getElementById('console-output');
const statusBadge = document.getElementById('status-badge');
const coreDot = document.getElementById('core-status-dot');
const corePing = document.getElementById('core-status-ping');

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
        
        globalDossiers = await res.json();
        grid.innerHTML = ''; 
        
        if (globalDossiers.length === 0) {
            grid.innerHTML = `<div class="col-span-full text-slate-500 font-mono text-sm">No dossiers found in registry.</div>`;
            return;
        }

        globalDossiers.forEach(d => {
            grid.innerHTML += `
                <div onclick="openDossierPreview('${d.product_code}')" class="cursor-pointer glass-panel p-6 rounded-xl hover:bg-slate-800/80 transition-all duration-300 border-l-2 border-emerald-500/50 flex flex-col justify-between h-40 group relative overflow-hidden">
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

        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const ws = new WebSocket(`${protocol}//${window.location.host}/api/v1/stream`);
        
        ws.onmessage = function(event) {
            const data = JSON.parse(event.data);
            handleAgentEvent(data);
        };

        ws.onerror = function() {
            logToConsole("> [SYS_ERR] WebSocket connection failed. Is the Python server running?", "text-rose-500");
        };

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

// --- Dossier Display Animations ---
function openDossierPreview(id) {
    const grid = document.getElementById('dossier-grid');
    const previewContainer = document.getElementById('dossier-preview');
    const dock = document.getElementById('dossier-dock');
    const iframe = document.getElementById('preview-iframe');
    const title = document.getElementById('preview-title');
    
    // Hide Grid smoothly
    grid.classList.add('opacity-0', 'translate-y-4', 'pointer-events-none');
    setTimeout(() => {
        grid.classList.add('hidden');
        grid.classList.remove('grid');
        
        // Show Preview
        previewContainer.classList.remove('hidden');
        setTimeout(() => {
            previewContainer.classList.remove('opacity-0', 'translate-y-4');
        }, 50);
    }, 300);

    // Populate Dock
    dock.innerHTML = '';
    globalDossiers.forEach(d => {
        const isSelected = d.product_code === id;
        if (isSelected) {
            iframe.src = d.pdf_url;
            title.innerText = d.name;
        }
        
        const borderClass = isSelected 
            ? 'border-emerald-500 shadow-[0_0_10px_rgba(16,185,129,0.2)] bg-slate-800/80' 
            : 'border-slate-500/30 opacity-60 hover:opacity-100 hover:bg-slate-800/50';
            
        dock.innerHTML += `
            <div onclick="openDossierPreview('${d.product_code}')" class="cursor-pointer glass-panel p-4 rounded-xl transition-all duration-300 border-l-2 ${borderClass} flex flex-col gap-1">
                <h3 class="font-bold text-white text-xs truncate">${d.name}</h3>
                <p class="text-[10px] font-mono text-slate-400">ID: ${d.product_code}</p>
            </div>
        `;
    });
}

function closeDossierPreview() {
    const grid = document.getElementById('dossier-grid');
    const previewContainer = document.getElementById('dossier-preview');
    const iframe = document.getElementById('preview-iframe');
    
    previewContainer.classList.add('opacity-0', 'translate-y-4');
    setTimeout(() => {
        previewContainer.classList.add('hidden');
        iframe.src = '';
        
        grid.classList.remove('hidden');
        grid.classList.add('grid');
        setTimeout(() => {
            grid.classList.remove('opacity-0', 'translate-y-4', 'pointer-events-none');
        }, 50);
    }, 300);
}

// --- Event Handling ---
function handleAgentEvent(data) {
    if (data.type === 'IMPACT_DETECTED') {
        currentRunId = data.run_id;
        
        statusBadge.className = "font-mono px-4 py-1.5 bg-rose-500/10 text-rose-400 text-xs font-semibold rounded border border-rose-500/50 shadow-[0_0_15px_rgba(244,63,94,0.3)] transition-all duration-300";
        statusBadge.innerText = "[ ALERT : DB_ANOMALY_DETECTED ]";
        coreDot.className = "relative w-3 h-3 bg-rose-400 rounded-full shadow-[0_0_8px_#fb7185]";
        corePing.className = "absolute w-full h-full bg-rose-500 rounded-full animate-ping opacity-30";

        document.getElementById('wf-product').innerText = data.product_code;
        document.getElementById('wf-trigger').innerText = `${data.change_count} DB UPDATE(s)`;
        
        logToConsole(`> [SYS] Threat radar triggered by SQL pipeline. Detected ${data.change_count} changes.`, 'text-rose-400');
        switchView('workflow');
    }
    
    if (data.type === 'AGENT_STATE') {
        document.querySelectorAll('.step-indicator').forEach(el => {
            el.classList.remove('text-cyan-400', 'text-glow', 'opacity-100');
            el.classList.add('opacity-40');
            el.querySelector('.indicator-ring')?.classList.remove('border-cyan-400', 'bg-cyan-500/20', 'shadow-[0_0_10px_rgba(6,182,212,0.5)]');
            el.querySelector('.indicator-ring')?.classList.add('bg-slate-800', 'border-slate-600');
        });
        
        const activeStep = document.getElementById(`step-${data.state}`);
        if(activeStep) {
            activeStep.classList.remove('opacity-40');
            activeStep.classList.add('text-cyan-400', 'text-glow', 'opacity-100');
            const ring = activeStep.querySelector('.indicator-ring');
            if (ring) {
                ring.classList.remove('bg-slate-800', 'border-slate-600');
                ring.classList.add('border-cyan-400', 'bg-cyan-500/20', 'shadow-[0_0_10px_rgba(6,182,212,0.5)]');
            }
        }

        logToConsole(`> [AGENT] Initializing Protocol: ${data.state}...`, 'text-cyan-300');
    }

    if (data.type === 'REVIEW_REQUIRED') {
        currentRunId = data.run_id;
        
        document.getElementById('rev-section-name').innerText = `${data.section_number} — ${data.title}`;
        document.getElementById('rev-reasoning').innerText = data.reasoning;
        
        // ADDED: Using marked.js to parse the raw markdown output into an HTML table
        document.getElementById('rev-new').innerHTML = marked.parse(data.new_text);
        
        statusBadge.className = "font-mono px-4 py-1.5 bg-amber-500/10 text-amber-400 text-xs font-semibold rounded border border-amber-500/50 shadow-[0_0_15px_rgba(245,158,11,0.2)] transition-all duration-300";
        statusBadge.innerText = "[ PAUSED : AWAITING_AUTHORIZATION ]";
        coreDot.className = "relative w-3 h-3 bg-amber-400 rounded-full shadow-[0_0_8px_#fbbf24]";
        corePing.className = "absolute w-full h-full bg-amber-500 rounded-full animate-ping opacity-20";
        
        switchView('review');
    }

    if (data.type === 'WORKFLOW_REJECTED') {
        logToConsole(`> [AUTH] Override Denied: ${data.message}`, 'text-rose-500 font-bold');
        resetToIdle();
    }

    if (data.type === 'WORKFLOW_COMPLETE') {
        document.getElementById('pdf-orig').src = data.original_pdf;
        document.getElementById('pdf-new').src = data.new_pdf;
        document.getElementById('download-btn').href = data.new_pdf;
        
        statusBadge.className = "font-mono px-4 py-1.5 bg-cyan-500/10 text-cyan-400 text-xs font-semibold rounded border border-cyan-500/50 shadow-[0_0_15px_rgba(6,182,212,0.2)] transition-all duration-300";
        statusBadge.innerText = "[ SUCCESS : DOSSIER_COMPILED ]";
        coreDot.className = "relative w-3 h-3 bg-cyan-400 rounded-full shadow-[0_0_8px_#22d3ee]";
        corePing.className = "absolute w-full h-full bg-cyan-500 rounded-full animate-ping opacity-20";
        
        switchView('final');
    }
}

// --- Actions ---
async function submitReview(decision) {
    const res = await fetch('/api/v1/workflow/review', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ run_id: currentRunId, decision: decision })
    });
    
    if (res.ok) {
        if (decision === 'APPROVE') {
            logToConsole(`> [AUTH] Override Granted. Resuming Graph Injection protocol...`, 'text-emerald-400 font-bold');
            switchView('workflow');
        } else {
            switchView('workflow'); 
        }
    }
}

// --- Rolling Console Logic ---
function logToConsole(msg, colorClass = 'text-slate-300') {
    const time = new Date().toLocaleTimeString('en-US', { hour12: false, hour: 'numeric', minute:'numeric', second:'numeric' });
    const logEntry = `<span class="text-slate-600 mr-2">[${time}]</span> <span class="${colorClass}">${msg}</span>`;
    
    logLines.unshift(logEntry); // Insert at beginning
    if (logLines.length > 5) logLines.pop(); // Keep only 5
    
    renderConsole();
}

function renderConsole() {
    consoleOut.innerHTML = '';
    // Iterate backwards so newest (index 0) is at the bottom
    for (let i = logLines.length - 1; i >= 0; i--) {
        const div = document.createElement('div');
        div.className = `log-line log-line-${i}`;
        div.innerHTML = logLines[i];
        consoleOut.appendChild(div);
    }
}

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

function resetToIdle() {
    statusBadge.className = "font-mono px-4 py-1.5 bg-emerald-500/10 text-emerald-400 text-xs font-semibold rounded border border-emerald-500/30 shadow-[0_0_10px_rgba(16,185,129,0.1)] transition-all duration-300";
    statusBadge.innerText = "[ SYS_IDLE : LISTENING_TELEMETRY ]";
    coreDot.className = "relative w-3 h-3 bg-emerald-400 rounded-full shadow-[0_0_8px_#34d399]";
    corePing.className = "absolute w-full h-full bg-emerald-500 rounded-full animate-ping opacity-20";
    
    switchView('idle');
}

function finishWorkflow() {
    logToConsole('> [USER] Workflow complete. Returning to monitoring mode...', 'text-cyan-400');
    resetToIdle();
}

init();