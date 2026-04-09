// static/app.js

let currentRunId       = null;
let currentReviewId    = null;    // Per-section review ID for pipelined HITL
let globalDossiers     = [];
let logLines           = [];
// Now stores objects: { time, msg, colorClass }
let reviewExpanded     = false;
// Right panel expand state
let evidenceOpen       = true;
// Level 1: entire evidence block
let evidenceGroupStates = {};     // Level 2: per operation type { UPDATE: bool, INSERT: bool, ... }
let currentViewName    = 'idle';
// Track the active view

// Multi-product state
let multiProductMode   = false;
let productQueue       = [];      // [{ run_id, product_code, product_name, state, queue_position }]
let activeProductIndex = 0;
let bufferedEvents     = {};      // run_id -> [event, ...] for non-active products

const views = {
    idle:     document.getElementById('view-idle'),
    workflow: document.getElementById('view-workflow'),
    review:   document.getElementById('view-review'),
    final:    document.getElementById('view-final')
};
const statusBadge = document.getElementById('status-badge');
const coreDot     = document.getElementById('core-status-dot');
const corePing    = document.getElementById('core-status-ping');

// ============================================================
// THEME TOGGLE
// ============================================================

function initTheme() {
    const htmlElement = document.documentElement;
    const savedTheme = localStorage.getItem('cipher-theme');

    if (savedTheme === 'light') {
        htmlElement.classList.remove('dark');
    } else {
        // Default to dark theme
        htmlElement.classList.add('dark');
        localStorage.setItem('cipher-theme', 'dark');
    }
    updateThemeIcon();
}

function toggleTheme() {
    const htmlElement = document.documentElement;
    if (htmlElement.classList.contains('dark')) {
        // Switch to light
        htmlElement.classList.remove('dark');
        localStorage.setItem('cipher-theme', 'light');
    } else {
        // Switch to dark
        htmlElement.classList.add('dark');
        localStorage.setItem('cipher-theme', 'dark');
    }
    updateThemeIcon();
}

function updateThemeIcon() {
    const sunIcon = document.getElementById('theme-icon-sun');
    const moonIcon = document.getElementById('theme-icon-moon');
    const isDark = document.documentElement.classList.contains('dark');

    if (isDark) {
        sunIcon?.classList.remove('hidden');
        sunIcon?.classList.add('block');
        moonIcon?.classList.add('hidden');
        moonIcon?.classList.remove('block');
    } else {
        sunIcon?.classList.add('hidden');
        sunIcon?.classList.remove('block');
        moonIcon?.classList.remove('hidden');
        moonIcon?.classList.add('block');
    }
}


// ============================================================
// SVG ICON CONSTANTS
// ============================================================

const ICON_EXPAND = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="expand-icon"><polyline points="15 3 21 3 21 9"/><polyline points="9 21 3 21 3 15"/><polyline points="21 3 14 10"/><polyline points="3 21 10 14"/></svg>`;
const ICON_COMPRESS = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#94a3b8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="expand-icon"><polyline points="4 14 10 14 10 20"/><polyline points="20 10 14 10 14 4"/><polyline points="10 14 3 21"/><polyline points="14 10 21 3"/></svg>`;

// ============================================================
// WEBSOCKET WITH AUTO-RECONNECT
// ============================================================

let _wsReconnectDelay = 1000;

function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${protocol}//${window.location.host}/api/v1/stream`);

    ws.onopen = function() {
        _wsReconnectDelay = 1000; // Reset backoff on successful connect
        logToConsole("WebSocket connected to backend server", "text-emerald-400");
    };

    ws.onmessage = function(event) {
        try {
            const data = JSON.parse(event.data);
            handleAgentEvent(data, false);
        } catch(e) {
            console.error("Failed to parse WebSocket message:", e, event.data);
        }
    };

    ws.onerror = function(error) {
        console.error("WebSocket error:", error);
    };

    ws.onclose = function() {
        logToConsole(`Connection lost. Reconnecting in ${Math.round(_wsReconnectDelay/1000)}s...`, "text-amber-400");
        setTimeout(connectWebSocket, _wsReconnectDelay);
        _wsReconnectDelay = Math.min(_wsReconnectDelay * 1.5, 10000); // Backoff up to 10s
    };
}

// ============================================================
// INITIALIZATION
// ============================================================

async function init() {
    initTheme();

    const expandBtn = document.getElementById('expand-btn');
    if (expandBtn) {
        expandBtn.innerHTML = ICON_EXPAND;
        expandBtn.title     = 'Expand panel';
    }

    const grid = document.getElementById('dossier-grid');
    grid.innerHTML = `
        <div class="col-span-full flex flex-col items-center justify-center p-10 text-cyan-600 dark:text-cyan-500/50">
            <div class="w-8 h-8 border-2 border-cyan-600 dark:border-cyan-500 border-t-transparent rounded-full animate-spin mb-4"></div>
            <p class="font-mono text-xs tracking-widest uppercase">Establishing secure link to Registry...</p>
        </div>
    `;

    try {
        const res = await fetch('/api/v1/dossiers', { cache: 'no-store' });
        if (!res.ok) throw new Error(`API returned status: ${res.status}`);

        globalDossiers = await res.json();
        grid.innerHTML = '';

        if (globalDossiers.length === 0) {
            grid.innerHTML = `<div class="col-span-full text-slate-500 font-mono text-sm">No dossiers found in registry.</div>`;
            return;
        }

        globalDossiers.forEach(d => {
            grid.innerHTML += `
                <div onclick="openDossierPreview('${d.product_code}')" class="cursor-pointer glass-panel p-6 rounded-xl hover:bg-slate-50 dark:hover:bg-slate-800/80 transition-all duration-300 border-l-2 border-emerald-500/50 dark:border-emerald-500/50 flex flex-col justify-between h-40 group relative overflow-hidden">
                    <div class="absolute inset-0 bg-gradient-to-r from-emerald-500/5 dark:from-emerald-500/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity"></div>
                    <div class="relative z-10">
                        <div class="flex justify-between items-start mb-2">
                            <h3 class="font-bold text-slate-900 dark:text-white text-sm leading-tight group-hover:text-emerald-600 dark:group-hover:text-emerald-400 transition-colors">${d.name}</h3>
                            <svg class="w-5 h-5 text-slate-400 dark:text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path></svg>
                        </div>
                        <p class="text-[10px] font-mono text-slate-500 dark:text-slate-400">ID: ${d.product_code}</p>
                    </div>
                    <div class="mt-4 flex items-center gap-2 relative z-10">
                        <span class="w-1.5 h-1.5 bg-emerald-500 dark:bg-emerald-400 rounded-full animate-pulse shadow-[0_0_5px_#34d399]"></span>
                        <span class="text-[10px] uppercase tracking-widest text-emerald-600 dark:text-emerald-400/80 font-mono">Secured</span>
                    </div>
                </div>
            `;
        });

        connectWebSocket();

    } catch (error) {
        console.error("Failed to load dossiers:", error);
        grid.innerHTML = `
            <div class="col-span-full glass-panel border-rose-500/30 dark:border-rose-500/30 p-6 rounded-xl text-rose-600 dark:text-rose-400 font-mono text-sm flex flex-col items-center text-center">
                <svg class="w-8 h-8 mb-3 text-rose-600 dark:text-rose-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>
                CONNECTION FAILED<br>
                <span class="text-slate-600 dark:text-slate-400 text-xs mt-2 uppercase tracking-widest">Ensure you are accessing via http://localhost:8000 and not a local file:// path.</span>
            </div>
        `;
    }
}


// ============================================================
// ROLLING CONSOLE LOGIC (Vertical Slot Machine UX)
// ============================================================

function logToConsole(msg, colorClass = 'text-cyan-300') {
    const time = new Date().toLocaleTimeString('en-US', {
        hour12: false, hour: 'numeric', minute: 'numeric', second: 'numeric'
    });
    logLines.push({ time, msg, colorClass });

    if (logLines.length > 5) {
        logLines.shift();
    }

    renderConsole();
}

function renderConsole() {
    const consoleOut = document.getElementById('console-output');
    if (!consoleOut) return;
    let html = '';

    const opacities = ['opacity-20', 'opacity-40', 'opacity-60', 'opacity-80', 'opacity-100'];
    const offset = 5 - logLines.length;

    logLines.forEach((log, index) => {
        const op = opacities[index + offset];
        html += `
            <div class="flex items-center w-full ${op} transition-all duration-300 mb-2.5">
                <span class="text-slate-600 mr-3 flex-shrink-0 font-mono tracking-wider">[${log.time}]</span>
                <span class="text-slate-500 mr-3 flex-shrink-0 font-mono">[AGENT]</span>
                <span class="${log.colorClass} flex-1 truncate font-mono" title="${log.msg.replace(/"/g, '&quot;')}">${log.msg}</span>
            </div>
        `;
    });

    const futures = [
        { op: 'opacity-40', txt: '{ ... awaiting_telemetry }' },
        { op: 'opacity-20', txt: '{ ... pipeline_idle }' },
        { op: 'opacity-5',  txt: '[+] PDF generation stage' }
    ];

    futures.forEach(f => {
        html += `
            <div class="flex items-center w-full ${f.op} transition-all duration-300 mb-2 pl-1 border-l-2 border-slate-700/30 border-dashed ml-1.5">
                <span class="text-slate-600/50 mr-3 flex-shrink-0 font-mono blur-[1px]">[XX:XX:XX]</span>
                <span class="text-slate-600/50 mr-3 flex-shrink-0 font-mono blur-[1px]">[AGENT]</span>
                <span class="text-slate-600/50 flex-1 truncate font-mono blur-[1px]">${f.txt}</span>
            </div>
        `;
    });

    consoleOut.innerHTML = html;
}


// ============================================================
// EVENT HANDLING & MULTI-PRODUCT ORCHESTRATION
// ============================================================

function handleAgentEvent(data, bypassBuffer = false) {

    // -- MULTI-PRODUCT SYSTEM EVENT ROUTING --

    if (data.type === 'MULTI_PRODUCT_DETECTED') {
        multiProductMode = true;
        productQueue = data.product_codes.map((code, idx) => ({
            run_id: null,
            product_code: code,
            product_name: (globalDossiers.find(d => d.product_code === code) || {}).name || code,
            state: 'PENDING',
            queue_position: idx,
        }));
        activeProductIndex = 0;
        bufferedEvents = {};
        renderProductIndicator();
        logToConsole(`Multi-product batch: ${data.count} products detected.`, 'text-cyan-400');
        return;
    }

    if (data.type === 'ALL_PRODUCTS_COMPLETE') {
        logToConsole('All products processed.', 'text-emerald-400');
        multiProductMode = false;
        productQueue = [];
        activeProductIndex = 0;
        bufferedEvents = {};
        hideProductIndicator();
        return;
    }

    if (data.type === 'AUTO_ADVANCE') {
        // Backend auto-advances when a product is fully rejected but bg_task
        // for the next product is already running.  Switch UI context so the
        // user isn't stuck on the rejected product's view.
        if (multiProductMode) {
            const nextIdx = productQueue.findIndex(
                (p, i) => i > activeProductIndex && !['COMPLETE', 'REJECTED'].includes(p.state)
            );
            if (nextIdx !== -1) {
                logToConsole('Auto-advancing to next product...', 'text-cyan-400');
                switchToProduct(nextIdx);
            }
        }
        return;
    }

    if (data.type === 'REVIEW_PHASE_COMPLETE') {
        updateProductState(data);
        renderProductIndicator();
        logToConsole(`Review complete. Background compilation started.`, 'text-emerald-400');
        // Briefly switch back to workflow to show transition to next product
        switchView('workflow');
        return;
    }

    if (data.type === 'ALL_REVIEWS_COMPLETE') {
        logToConsole('All sections reviewed. Compiling dossiers...', 'text-emerald-400');
        switchView('workflow');
        return;
    }

    if (data.type === 'DISPLAY_PRODUCT') {
        if (multiProductMode) {
            const targetIdx = productQueue.findIndex(p => p.product_code === data.product_code);
            if (targetIdx !== -1) {
                activeProductIndex = targetIdx;
                const product = productQueue[targetIdx];
                const targetNameEl = document.getElementById('target-dossier-name');
                if (targetNameEl) targetNameEl.innerText = product.product_name || product.product_code;
                const wfProduct = document.getElementById('wf-product');
                if (wfProduct) wfProduct.innerText = product.product_code;
                renderProductIndicator();
            }
        }
        // Show all pipeline steps as completed during display phase
        const allSteps = ['POLLING', 'INTERPRETING', 'MAPPING', 'GENERATING', 'GRAPH_INJECTION', 'DOSSIER_GENERATION'];
        allSteps.forEach(stepName => {
            const stepEl = document.getElementById(`step-${stepName}`);
            if (!stepEl) return;
            const ring = stepEl.querySelector('.indicator-ring');
            const textEl = stepEl.querySelector('span');
            stepEl.className = "flex items-center gap-4 step-indicator opacity-100 transition-all duration-500";
            if (textEl) textEl.className = "font-mono text-xs tracking-wider text-emerald-600 dark:text-emerald-400 transition-colors duration-300";
            if (ring) ring.className = "w-4 h-4 rounded-full bg-emerald-500 border-2 border-emerald-500 dark:border-emerald-400 shadow-[0_0_10px_rgba(16,185,129,0.4)] z-10 indicator-ring transition-all duration-300";
        });
        switchView('workflow');
        logToConsole(`Preparing dossier for ${data.product_code}...`, 'text-cyan-400');
        return;
    }

    if (data.type === 'PRODUCT_READY') {
        updateProductState(data);
        renderProductIndicator();
        return;
    }

    // Route by run_id in multi-product mode
    if (multiProductMode && data.run_id) {
        // Link run_id to product on first IMPACT_DETECTED
        if (data.type === 'IMPACT_DETECTED') {
            const match = productQueue.find(p => p.product_code === data.product_code);
            if (match) match.run_id = data.run_id;
        }

        updateProductState(data);
        renderProductIndicator();

        const active = productQueue[activeProductIndex];

        // Buffer events for non-active products silently
        if (!bypassBuffer && active && data.run_id !== active.run_id) {
            if (!bufferedEvents[data.run_id]) bufferedEvents[data.run_id] = [];
            bufferedEvents[data.run_id].push(data);
            return;
        }
    }

    // -- STANDARD EVENT HANDLING --

    // --- IMPACT DETECTED ---
    if (data.type === 'IMPACT_DETECTED') {
        currentRunId = data.run_id;

        statusBadge.className = "font-mono px-4 py-1.5 bg-rose-500/10 text-rose-600 dark:text-rose-400 text-xs font-semibold rounded border border-rose-500/50 shadow-[0_0_15px_rgba(244,63,94,0.3)] transition-all duration-300";
        statusBadge.innerText = "[ ALERT : DB_ANOMALY_DETECTED ]";
        coreDot.className     = "relative w-3 h-3 bg-rose-500 dark:bg-rose-400 rounded-full shadow-[0_0_8px_#fb7185]";
        corePing.className    = "absolute w-full h-full bg-rose-500 rounded-full animate-ping opacity-30";

        const productName = (globalDossiers.find(d => d.product_code === data.product_code) || {}).name || data.product_code;

        const wfProduct = document.getElementById('wf-product');
        if (wfProduct) wfProduct.innerText = data.product_code;

        const targetDossierName = document.getElementById('target-dossier-name');
        if (targetDossierName) targetDossierName.innerText = productName;

        document.getElementById('wf-trigger').innerText = `${data.change_count} DB UPDATE(s)`;

        logToConsole(`Threat radar triggered. Detected ${data.change_count} regulatory shifts.`, 'text-rose-400');
        logToConsole(`Executing pipeline extraction on product: '${data.product_code}'`, 'text-slate-400');

        switchView('workflow');
    }

    // --- AGENT STATE ---
    if (data.type === 'AGENT_STATE') {

        const stepsOrder = ['POLLING', 'INTERPRETING', 'MAPPING', 'GENERATING', 'GRAPH_INJECTION', 'DOSSIER_GENERATION'];
        const currentIndex = stepsOrder.indexOf(data.state);

        stepsOrder.forEach((stepName, index) => {
            const stepEl = document.getElementById(`step-${stepName}`);
            if (!stepEl) return;

            const ring = stepEl.querySelector('.indicator-ring');
            const textEl = stepEl.querySelector('span');

            stepEl.className = "flex items-center gap-4 step-indicator transition-all duration-500";
            if (ring) ring.className = "w-4 h-4 rounded-full z-10 indicator-ring transition-all duration-300";
            if (textEl) textEl.className = "font-mono text-xs tracking-wider transition-colors duration-300";

            if (index < currentIndex) {
                stepEl.classList.add('opacity-100');
                if (textEl) textEl.classList.add('text-emerald-600', 'dark:text-emerald-400');
                if (ring) ring.classList.add('bg-emerald-500', 'border-2', 'border-emerald-500', 'dark:border-emerald-400', 'shadow-[0_0_10px_rgba(16,185,129,0.4)]');
            } else if (index === currentIndex) {
                stepEl.classList.add('opacity-100', 'text-glow');
                if (textEl) textEl.classList.add('text-cyan-600', 'dark:text-cyan-400', 'font-bold');
                if (ring) ring.classList.add('bg-white', 'dark:bg-slate-900', 'border-2', 'border-cyan-500', 'dark:border-cyan-400', 'shadow-[0_0_15px_rgba(6,182,212,0.6)]', 'animate-pulse');
            } else {
                stepEl.classList.add('opacity-40');
                if (textEl) textEl.classList.add('text-slate-500', 'dark:text-slate-400');
                if (ring) ring.classList.add('bg-white', 'dark:bg-slate-800', 'border-2', 'border-slate-300', 'dark:border-slate-600');
            }
        });

        const stateMessages = {
            POLLING:            'Scanning SQL change log for new events...',
            INTERPRETING:       'Interpreting DB changes into regulatory concepts...',
            MAPPING:            'Mapping concepts to affected dossier sections...',
            GENERATING:         'Generating updated section content...',
            GRAPH_INJECTION:    'Injecting approved content into knowledge graph...',
            DOSSIER_GENERATION: 'Compiling updated dossier PDF...',
        };

        const msg = stateMessages[data.state] || `Protocol: ${data.state}...`;
        logToConsole(msg, 'text-cyan-300');

        if (data.state === 'GRAPH_INJECTION' && currentViewName === 'review') {
            switchView('workflow');
        }
    }

    // --- REVIEW REQUIRED ---
    if (data.type === 'REVIEW_REQUIRED') {
        _reviewSubmitting = false;

        const reviewNumber = data.review_current ? ` (${data.review_current}/${data.review_total})` : '';
        logToConsole(`Section ${data.section_number} queued — awaiting human authorization${reviewNumber}.`, 'text-amber-400');
        currentRunId    = data.run_id;
        currentReviewId = data.review_id;

        const expandBtn = document.getElementById('expand-btn');
        if (expandBtn) {
            expandBtn.innerHTML = ICON_EXPAND;
            expandBtn.title     = 'Expand panel';
        }

        const reviewBadge = (data.review_total > 1)
            ? ` <span class="ml-3 inline-block px-2 py-0.5 bg-amber-500/10 dark:bg-amber-500/20 border border-amber-500/50 rounded text-amber-600 dark:text-amber-400 text-xs font-mono">${data.review_current}/${data.review_total}</span>`
            : '';

        document.getElementById('rev-section-name').innerHTML = `${data.section_number} — ${data.title}${reviewBadge}`;
        document.getElementById('rev-reasoning').innerText    = data.reasoning;

        renderDbEvidence(data.db_changes || []);

        const cleanedText = (data.new_text || '')
            .replace(/([^\n])[•●]\s*/g, '$1\n- ')
            .replace(/^\s*[•●]\s*/gm, '- ');

        document.getElementById('rev-new').innerHTML = marked.parse(cleanedText);

        statusBadge.className = "font-mono px-4 py-1.5 bg-amber-500/10 text-amber-600 dark:text-amber-400 text-xs font-semibold rounded border border-amber-500/50 shadow-[0_0_15px_rgba(245,158,11,0.2)] transition-all duration-300";
        statusBadge.innerText = "[ PAUSED : AWAITING_AUTHORIZATION ]";
        coreDot.className     = "relative w-3 h-3 bg-amber-500 dark:bg-amber-400 rounded-full shadow-[0_0_8px_#fbbf24]";
        corePing.className    = "absolute w-full h-full bg-amber-500 rounded-full animate-ping opacity-20";

        switchView('review');
    }

    // --- PLAN APPROVED ---
    if (data.type === 'PLAN_APPROVED') {
        logToConsole(`Approved: ${data.section} — ${data.message}`, 'text-emerald-400');
        statusBadge.className = "font-mono px-4 py-1.5 bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 text-xs font-semibold rounded border border-emerald-500/50 shadow-[0_0_15px_rgba(16,185,129,0.2)] transition-all duration-300";
        statusBadge.innerText = "[ APPROVED : SECTION_QUEUED ]";
    }

    // --- PLAN REJECTED ---
    if (data.type === 'PLAN_REJECTED') {
        logToConsole(`Rejected: ${data.section} — ${data.message}`, 'text-rose-400');
    }

    // --- WORKFLOW COMPLETE ---
    if (data.type === 'WORKFLOW_COMPLETE') {
        _reviewSubmitting = false;
        logToConsole(`PDF compiled for ${data.product_code}. Download ready.`, 'text-cyan-400');

        const loadingOverlay = document.getElementById('pdf-loading-overlay');
        if (loadingOverlay) {
            loadingOverlay.classList.remove('opacity-0', 'pointer-events-none');
            loadingOverlay.classList.add('opacity-100');
        }

        const pdfOrig = document.getElementById('pdf-orig');
        const pdfNew = document.getElementById('pdf-new');
        const downloadBtn = document.getElementById('download-btn');

        const hideLoadingOverlay = () => {
            if (loadingOverlay && pdfOrig.src && pdfNew.src && pdfOrig.offsetHeight > 0 && pdfNew.offsetHeight > 0) {
                setTimeout(() => {
                    loadingOverlay.classList.add('opacity-0', 'pointer-events-none');
                    loadingOverlay.classList.remove('opacity-100');
                }, 300);
            }
        };

        pdfOrig.onload = hideLoadingOverlay;
        pdfNew.onload = hideLoadingOverlay;

        pdfOrig.src = data.original_pdf;
        pdfNew.src = data.new_pdf;
        downloadBtn.href = data.new_pdf;

        setTimeout(() => {
            if (loadingOverlay) {
                loadingOverlay.classList.add('opacity-0', 'pointer-events-none');
                loadingOverlay.classList.remove('opacity-100');
            }
        }, 8000);

        statusBadge.className = "font-mono px-4 py-1.5 bg-cyan-500/10 text-cyan-700 dark:text-cyan-400 text-xs font-semibold rounded border border-cyan-500/50 shadow-[0_0_15px_rgba(6,182,212,0.2)] transition-all duration-300";
        statusBadge.innerText = "[ SUCCESS : DOSSIER_COMPILED ]";
        coreDot.className     = "relative w-3 h-3 bg-cyan-500 dark:bg-cyan-400 rounded-full shadow-[0_0_8px_#22d3ee]";
        corePing.className    = "absolute w-full h-full bg-cyan-500 rounded-full animate-ping opacity-20";

        switchView('final');

        // Multi-product: change [Home] button to [Next Product] if more products remain
        if (multiProductMode) {
            const homeBtn = document.querySelector('#view-final button[onclick="finishWorkflow()"]');
            if (homeBtn) {
                const remaining = productQueue.filter(p => !['COMPLETE', 'REJECTED'].includes(p.state));
                homeBtn.innerText = remaining.length > 0 ? '[ Next Product ]' : '[ Home ]';
            }
        }
    }

    // --- WORKFLOW ALL REJECTED ---
    if (data.type === 'WORKFLOW_ALL_REJECTED') {
        _reviewSubmitting = false;
        if (currentViewName === 'final') return;
        logToConsole(`All sections rejected. No changes committed.`, 'text-rose-400');
        triggerRejectionOverlay();
    }
}


// ============================================================
// CONTEXT SWITCHING FOR MULTI-PRODUCT
// ============================================================

function switchToProduct(index) {
    if (index === -1 || index >= productQueue.length) return;

    activeProductIndex = index;
    const activeProduct = productQueue[index];

    // Update header with new product context
    const targetNameEl = document.getElementById('target-dossier-name');
    if (targetNameEl) targetNameEl.innerText = activeProduct.product_name || activeProduct.product_code;

    const wfProduct = document.getElementById('wf-product');
    if (wfProduct) wfProduct.innerText = activeProduct.product_code;

    logLines = [];
    logToConsole(`Context Shift -> Focused on ${activeProduct.product_code}`, 'text-amber-400');
    renderProductIndicator();

    // Replay buffered events — calculate final state, render once (avoids animation flicker)
    if (activeProduct.run_id && bufferedEvents[activeProduct.run_id]) {
        const eventsToReplay = bufferedEvents[activeProduct.run_id];
        bufferedEvents[activeProduct.run_id] = [];

        // Determine the final view from buffered events without touching DOM
        let targetView = 'workflow';
        eventsToReplay.forEach(ev => {
            updateProductState(ev);
            if (ev.type === 'AGENT_STATE') {
                logToConsole(`Fast-forward: ${ev.state}...`, 'text-slate-500');
            }
            if (ev.type === 'REVIEW_REQUIRED') targetView = 'review';
            if (ev.type === 'WORKFLOW_COMPLETE') targetView = 'final';
        });

        // Only process the last meaningful event through the full DOM path
        if (targetView === 'review') {
            const lastReview = [...eventsToReplay].reverse().find(e => e.type === 'REVIEW_REQUIRED');
            if (lastReview) {
                handleAgentEvent(lastReview, true);
            } else {
                switchView('review');
            }
        } else if (targetView === 'final') {
            const lastComplete = [...eventsToReplay].reverse().find(e => e.type === 'WORKFLOW_COMPLETE');
            if (lastComplete) {
                handleAgentEvent(lastComplete, true);
            } else {
                switchView('final');
            }
        } else {
            // Still on workflow — apply last AGENT_STATE for step indicator
            const lastState = [...eventsToReplay].reverse().find(e => e.type === 'AGENT_STATE');
            if (lastState) handleAgentEvent(lastState, true);
            else switchView('workflow');
        }
    } else {
        switchView('workflow');
    }
}


// ============================================================
// ACTIONS
// ============================================================

let _reviewSubmitting = false;

async function submitReview(decision) {
    if (_reviewSubmitting) return;
    _reviewSubmitting = true;

    try {
        const res = await fetch('/api/v1/workflow/review', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ review_id: currentReviewId, decision: decision })
        });

        if (res.ok) {
            resetReviewExpand();
            if (decision === 'APPROVE') {
                logToConsole(`Override Granted. Section queued for compilation.`, 'text-emerald-400 font-bold');
            } else {
                logToConsole(`Section rejected.`, 'text-rose-400');
            }
            // Safety: unlock after 10s if no WebSocket event resets the flag
            setTimeout(() => { _reviewSubmitting = false; }, 10000);
        } else {
            logToConsole(`ERROR: Failed to submit decision.`, 'text-rose-500');
            _reviewSubmitting = false;
        }
    } catch (error) {
        logToConsole(`NETWORK ERROR: Cannot reach server. Try again.`, 'text-rose-500');
        _reviewSubmitting = false;
    }
}


// ============================================================
// DOSSIER DISPLAY ANIMATIONS
// ============================================================

function openDossierPreview(id) {
    const grid             = document.getElementById('dossier-grid');
    const previewContainer = document.getElementById('dossier-preview');
    const dock             = document.getElementById('dossier-dock');
    const iframe           = document.getElementById('preview-iframe');
    const title            = document.getElementById('preview-title');

    grid.classList.add('opacity-0', 'translate-y-4', 'pointer-events-none');

    setTimeout(() => {
        grid.classList.add('hidden');
        grid.classList.remove('grid');

        previewContainer.classList.remove('hidden');
        setTimeout(() => {
            previewContainer.classList.remove('opacity-0', 'translate-y-4');
        }, 50);
    }, 300);

    dock.innerHTML = '';
    globalDossiers.forEach(d => {
        const isSelected = d.product_code === id;
        if (isSelected) {
            iframe.src      = d.pdf_url;
            title.innerText = d.name;
        }

        const borderClass = isSelected
            ? 'border-emerald-500 shadow-[0_0_10px_rgba(16,185,129,0.2)] bg-slate-100 dark:bg-slate-800/80'
            : 'border-slate-300 dark:border-slate-500/30 opacity-60 hover:opacity-100 hover:bg-slate-50 dark:hover:bg-slate-800/50';

        dock.innerHTML += `
            <div onclick="openDossierPreview('${d.product_code}')" class="cursor-pointer glass-panel p-4 rounded-xl transition-all duration-300 border-l-2 ${borderClass} flex flex-col gap-1 dossier-list-item">
                <h3 class="font-bold text-slate-900 dark:text-white text-xs truncate">${d.name}</h3>
                <p class="text-[10px] font-mono text-slate-500 dark:text-slate-400">ID: ${d.product_code}</p>
            </div>
        `;
    });
}

function closeDossierPreview() {
    const grid             = document.getElementById('dossier-grid');
    const previewContainer = document.getElementById('dossier-preview');
    const iframe           = document.getElementById('preview-iframe');

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


// ============================================================
// REVIEW PANEL — EXPAND / SHRINK TOGGLE
// ============================================================

function toggleExpandReview() {
    const reasoningPanel = document.getElementById('rev-reasoning-panel');
    const contentPanel   = document.getElementById('rev-content-panel');
    const expandBtn      = document.getElementById('expand-btn');

    reviewExpanded = !reviewExpanded;

    if (reviewExpanded) {
        reasoningPanel.classList.add('review-panel-hidden');
        contentPanel.classList.add('review-panel-expand');
        expandBtn.innerHTML = ICON_COMPRESS;
        expandBtn.title     = 'Compress panel';
    } else {
        reasoningPanel.classList.remove('review-panel-hidden');
        contentPanel.classList.remove('review-panel-expand');
        expandBtn.innerHTML = ICON_EXPAND;
        expandBtn.title     = 'Expand panel';
    }
}

function resetReviewExpand() {
    if (reviewExpanded) {
        const reasoningPanel = document.getElementById('rev-reasoning-panel');
        const contentPanel   = document.getElementById('rev-content-panel');
        const expandBtn      = document.getElementById('expand-btn');

        reasoningPanel.classList.remove('review-panel-hidden');
        contentPanel.classList.remove('review-panel-expand');

        if (expandBtn) {
            expandBtn.innerHTML = ICON_EXPAND;
            expandBtn.title     = 'Expand panel';
        }
        reviewExpanded = false;
    }
}


// ============================================================
// DB EVIDENCE TOGGLES & RENDERER
// ============================================================

function toggleEvidenceBlock() {
    evidenceOpen = !evidenceOpen;
    const container = document.getElementById('evidence-cards-container');
    const chevron   = document.getElementById('evidence-chevron');

    if (!container) return;
    if (evidenceOpen) {
        container.classList.remove('evidence-block-hidden');
        if (chevron) chevron.style.transform = 'rotate(0deg)';
    } else {
        container.classList.add('evidence-block-hidden');
        if (chevron) chevron.style.transform = 'rotate(-90deg)';
    }
}

function toggleEvidenceGroup(opType) {
    evidenceGroupStates[opType] = !evidenceGroupStates[opType];
    const body    = document.getElementById(`evidence-group-body-${opType}`);
    const chevron = document.getElementById(`evidence-group-chevron-${opType}`);

    if (!body) return;

    if (evidenceGroupStates[opType]) {
        body.classList.remove('evidence-block-hidden');
        if (chevron) chevron.style.transform = 'rotate(0deg)';
    } else {
        body.classList.add('evidence-block-hidden');
        if (chevron) chevron.style.transform = 'rotate(-90deg)';
    }
}

function renderDbEvidence(dbChanges) {
    evidenceGroupStates = {};
    evidenceOpen        = true;

    const countBadge = document.getElementById('evidence-count-badge');
    const container  = document.getElementById('evidence-cards-container');
    const chevron    = document.getElementById('evidence-chevron');

    if (!container) return;

    if (chevron) chevron.style.transform = 'rotate(0deg)';
    container.innerHTML = '';
    container.classList.remove('evidence-block-hidden');

    if (!dbChanges || dbChanges.length === 0) {
        if (countBadge) countBadge.textContent = '0';
        container.innerHTML = `<p class="text-[11px] font-mono text-slate-600 italic mt-2 px-1">No raw DB change records available.</p>`;
        return;
    }

    const meaningful = dbChanges.filter(c => {
        const o = c.old_value !== null && c.old_value !== undefined ? String(c.old_value) : null;
        const n = c.new_value !== null && c.new_value !== undefined ? String(c.new_value) : null;
        return o !== n;
    });

    if (countBadge) countBadge.textContent = meaningful.length;

    if (meaningful.length === 0) {
        container.innerHTML = `<p class="text-[11px] font-mono text-slate-600 italic mt-2 px-1">No meaningful changes detected (all values unchanged).</p>`;
        return;
    }

    const ORDER = ['UPDATE', 'INSERT', 'DELETE'];
    const groups = {};
    meaningful.forEach(c => {
        const op = c.operation_type || 'OTHER';
        if (!groups[op]) groups[op] = [];
        groups[op].push(c);
    });

    const sortedOps = Object.keys(groups).sort((a, b) => {
        const ai = ORDER.indexOf(a);
        const bi = ORDER.indexOf(b);
        return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
    });

    const opStyles = {
        'INSERT': { header: 'bg-emerald-50 dark:bg-emerald-500/10 border-emerald-200 dark:border-emerald-500/20 hover:bg-emerald-100 dark:hover:bg-emerald-500/15', badge: 'bg-emerald-100 dark:bg-emerald-500/20 text-emerald-700 dark:text-emerald-400 border-emerald-300 dark:border-emerald-500/40', count: 'text-emerald-600 dark:text-emerald-500/70' },
        'UPDATE': { header: 'bg-amber-50 dark:bg-amber-500/10 border-amber-200 dark:border-amber-500/20 hover:bg-amber-100 dark:hover:bg-amber-500/15', badge: 'bg-amber-100 dark:bg-amber-500/20 text-amber-700 dark:text-amber-400 border-amber-300 dark:border-amber-500/40', count: 'text-amber-600 dark:text-amber-500/70' },
        'DELETE': { header: 'bg-rose-50 dark:bg-rose-500/10 border-rose-200 dark:border-rose-500/20 hover:bg-rose-100 dark:hover:bg-rose-500/15', badge: 'bg-rose-100 dark:bg-rose-500/20 text-rose-700 dark:text-rose-400 border-rose-300 dark:border-rose-500/40', count: 'text-rose-600 dark:text-rose-500/70' },
    };
    const defaultStyle = { header: 'bg-slate-50 dark:bg-slate-700/30 border-slate-200 dark:border-slate-600/20 hover:bg-slate-100 dark:hover:bg-slate-700/50', badge: 'bg-slate-200 dark:bg-slate-500/20 text-slate-700 dark:text-slate-400 border-slate-300 dark:border-slate-500/40', count: 'text-slate-600 dark:text-slate-500/70' };

    function esc(str) {
        const d = document.createElement('div');
        d.textContent = str;
        return d.innerHTML;
    }

    sortedOps.forEach(opType => {
        const records = groups[opType];
        const style   = opStyles[opType] || defaultStyle;

        evidenceGroupStates[opType] = true;

        let rowsHtml = '';
        records.forEach((change, ri) => {
            const ts = change.change_timestamp ? String(change.change_timestamp).split('T').pop().split('.')[0] : '—';
            const oldDisplay = (change.old_value !== null && change.old_value !== undefined) ? `<span class="text-rose-600 dark:text-rose-300/80 font-medium">${esc(String(change.old_value))}</span>` : `<span class="text-slate-400 dark:text-slate-600 italic">NULL</span>`;
            const newDisplay = (change.new_value !== null && change.new_value !== undefined) ? `<span class="text-emerald-600 dark:text-emerald-300/90 font-medium">${esc(String(change.new_value))}</span>` : `<span class="text-slate-400 dark:text-slate-600 italic">NULL</span>`;
            const divider = ri > 0 ? `<div class="border-t border-slate-200 dark:border-white/5 mx-3"></div>` : '';

            rowsHtml += `
                ${divider}
                <div class="px-3 py-2.5 flex flex-col gap-1.5">
                    <div class="text-[10px] font-mono text-slate-500 dark:text-slate-400 tracking-wide mb-0.5">${esc(change.source_table || '')}</div>
                    <div class="flex items-start gap-2">
                        <span class="text-[10px] font-mono text-slate-400 dark:text-slate-600 uppercase tracking-widest w-14 flex-shrink-0 pt-px">COLUMN</span>
                        <span class="text-[11px] font-mono text-slate-800 dark:text-slate-300 break-all">${esc(change.column_name || '—')}</span>
                    </div>
                    <div class="flex items-start gap-2">
                        <span class="text-[10px] font-mono text-slate-400 dark:text-slate-600 uppercase tracking-widest w-14 flex-shrink-0 pt-px">OLD</span>
                        <span class="text-[11px] font-mono break-all">${oldDisplay}</span>
                    </div>
                    <div class="flex items-start gap-2">
                        <span class="text-[10px] font-mono text-slate-400 dark:text-slate-600 uppercase tracking-widest w-14 flex-shrink-0 pt-px">NEW</span>
                        <span class="text-[11px] font-mono break-all">${newDisplay}</span>
                    </div>
                    <div class="flex items-center justify-between mt-0.5">
                        <span class="text-[10px] font-mono text-slate-500 dark:text-slate-600">by&nbsp;${esc(change.changed_by || 'system')}</span>
                        <span class="text-[10px] font-mono text-slate-500 dark:text-slate-600">${ts}</span>
                    </div>
                </div>
            `;
        });

        container.innerHTML += `
            <div class="evidence-group mt-2 rounded-lg border overflow-hidden ${style.header.split(' ')[1]}">
                <div onclick="toggleEvidenceGroup('${opType}')" class="flex items-center justify-between px-3 py-2 ${style.header} cursor-pointer transition-colors duration-200 select-none">
                    <div class="flex items-center gap-2">
                        <span class="text-[10px] font-mono font-bold px-1.5 py-0.5 rounded border ${style.badge} flex-shrink-0 uppercase tracking-wider">${opType}</span>
                        <span class="text-[10px] font-mono ${style.count}">(${records.length})</span>
                    </div>
                    <svg id="evidence-group-chevron-${opType}" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" class="text-slate-500 dark:text-slate-400 flex-shrink-0 transition-transform duration-300" style="transform: rotate(0deg)"><polyline points="6 9 12 15 18 9"/></svg>
                </div>
                <div id="evidence-group-body-${opType}" class="evidence-group-body bg-white dark:bg-slate-900/40">
                    ${rowsHtml}
                </div>
            </div>
        `;
    });
}


// ============================================================
// VIEW SWITCHING & OVERLAYS
// ============================================================

let _switchViewTimer = null;

function switchView(viewName) {
    if (_switchViewTimer) {
        clearTimeout(_switchViewTimer);
        _switchViewTimer = null;
    }

    currentViewName = viewName;
    if (viewName !== 'review') resetReviewExpand();

    Object.keys(views).forEach(key => {
        const el = views[key];
        el.classList.add('hidden', 'opacity-0', 'translate-y-4');
        el.classList.remove('translate-y-0');
    });

    const target = views[viewName];
    target.classList.remove('hidden');

    _switchViewTimer = setTimeout(() => {
        target.classList.remove('opacity-0', 'translate-y-4');
        target.classList.add('translate-y-0');
        _switchViewTimer = null;
    }, 50);
}

function resetToIdle() {
    statusBadge.className = "font-mono px-4 py-1.5 bg-emerald-500/10 text-emerald-600 dark:text-emerald-400 text-xs font-semibold rounded border border-emerald-500/30 shadow-[0_0_10px_rgba(16,185,129,0.1)] transition-all duration-300";
    statusBadge.innerText = "[ SYS_IDLE : LISTENING_TELEMETRY ]";
    coreDot.className     = "relative w-3 h-3 bg-emerald-500 dark:bg-emerald-400 rounded-full shadow-[0_0_8px_#34d399]";
    corePing.className    = "absolute w-full h-full bg-emerald-500 rounded-full animate-ping opacity-20";

    const pdfOrig = document.getElementById('pdf-orig');
    const pdfNew = document.getElementById('pdf-new');
    const loadingOverlay = document.getElementById('pdf-loading-overlay');

    if (pdfOrig) pdfOrig.src = '';
    if (pdfNew) pdfNew.src = '';

    if (loadingOverlay) {
        loadingOverlay.classList.remove('opacity-0', 'pointer-events-none');
        loadingOverlay.classList.add('opacity-100');
    }

    // Reset header
    const targetNameEl = document.getElementById('target-dossier-name');
    if (targetNameEl) targetNameEl.innerText = 'Listening...';

    switchView('idle');
}

function finishWorkflow() {
    if (multiProductMode) {
        const remaining = productQueue.filter(p => !['COMPLETE', 'REJECTED'].includes(p.state));
        if (remaining.length > 0) {
            logToConsole('Advancing to next dossier...', 'text-cyan-400');
            // Tell backend to unblock the next product in display phase
            fetch('/api/v1/workflow/advance', { method: 'POST' })
                .catch(() => logToConsole('Error advancing workflow. Try again.', 'text-rose-500'));
            // Show workflow/console view while backend sends DISPLAY_PRODUCT for next
            switchView('workflow');
            return;
        }
    }
    logToConsole('Workflow complete. Returning to monitoring mode...', 'text-cyan-400');
    resetToIdle();
}

function triggerRejectionOverlay() {
    const overlay   = document.getElementById('rejection-overlay');
    const textBlock = document.getElementById('rejection-overlay-text');

    if (!overlay || !textBlock) {
        if (!multiProductMode) resetToIdle();
        return;
    }

    overlay.classList.remove('overlay-entering', 'overlay-exiting');
    textBlock.classList.remove('overlay-text-visible');
    textBlock.style.opacity = '0';

    overlay.classList.remove('hidden');
    overlay.classList.add('flex');
    requestAnimationFrame(() => {
        requestAnimationFrame(() => {
            overlay.classList.add('overlay-entering');
        });
    });

    setTimeout(() => {
        textBlock.classList.add('overlay-text-visible');
    }, 700);

    setTimeout(() => {
        // In multi-product mode, don't reset to idle — the next product's
        // IMPACT_DETECTED event will drive the view transition.
        if (!multiProductMode) resetToIdle();
    }, 2000);

    setTimeout(() => {
        overlay.classList.remove('overlay-entering');
        overlay.classList.add('overlay-exiting');
    }, 2500);

    setTimeout(() => {
        overlay.classList.add('hidden');
        overlay.classList.remove('flex', 'overlay-exiting');
        textBlock.classList.remove('overlay-text-visible');
        textBlock.style.opacity = '0';
    }, 3200);
}

// ============================================================
// MULTI-PRODUCT HELPERS
// ============================================================

function updateProductState(data) {
    const p = productQueue.find(p => p.run_id === data.run_id || p.product_code === data.product_code);
    if (!p) return;
    if (data.type === 'IMPACT_DETECTED')       p.state = 'PROCESSING';
    if (data.type === 'PRODUCT_READY')         p.state = 'AWAITING_REVIEW';
    if (data.type === 'REVIEW_REQUIRED')       p.state = 'IN_REVIEW';
    if (data.type === 'REVIEW_PHASE_COMPLETE') p.state = 'REVIEW_DONE';
    if (data.type === 'WORKFLOW_COMPLETE')      p.state = 'COMPLETE';
    if (data.type === 'WORKFLOW_ALL_REJECTED')  p.state = 'REJECTED';
}

function renderProductIndicator() {
    const el = document.getElementById('product-indicator');
    if (!el || !multiProductMode || productQueue.length === 0) {
        if (el) el.classList.add('hidden');
        return;
    }
    el.classList.remove('hidden');

    const stateIcon = {
        'PENDING':           '<span class="text-slate-500">&#x25CB;</span>',
        'PROCESSING':        '<span class="text-cyan-400 animate-pulse">&#x25CF;</span>',
        'AWAITING_REVIEW':   '<span class="text-amber-400">&#x25CF;</span>',
        'IN_REVIEW':         '<span class="text-amber-400 animate-pulse">&#x25CF;</span>',
        'REVIEW_DONE':       '<span class="text-emerald-400 opacity-60">&#x2713;</span>',
        'COMPLETE':          '<span class="text-emerald-400">&#x2713;</span>',
        'COMPLETE_PENDING_ADVANCE': '<span class="text-emerald-400">&#x2713;</span>',
        'REJECTED':          '<span class="text-rose-400">&#x2717;</span>',
    };

    el.innerHTML = productQueue.map((p, idx) => {
        const isActive = (idx === activeProductIndex);
        const icon = stateIcon[p.state] || stateIcon['PENDING'];
        const activeClass = isActive
            ? 'bg-slate-800/80 border-cyan-500/50 shadow-[0_0_8px_rgba(6,182,212,0.2)]'
            : 'border-transparent opacity-60';
        const nameClass = isActive ? 'text-white font-bold' : 'text-slate-400';
        return `<div class="flex items-center gap-1.5 px-2.5 py-1 rounded border ${activeClass} transition-all duration-300">
            ${icon}
            <span class="tracking-wider uppercase text-[10px] ${nameClass} font-mono">${p.product_code}</span>
        </div>`;
    }).join('<span class="text-slate-600 text-[10px]">&#x25B8;</span>');
}

function hideProductIndicator() {
    const el = document.getElementById('product-indicator');
    if (el) el.classList.add('hidden');
}

// Start
init();