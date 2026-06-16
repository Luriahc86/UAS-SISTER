/**
 * app.js — Pub-Sub Log Aggregator Dashboard
 * 
 * Terintegrasi langsung dengan FastAPI aggregator.
 * API calls menggunakan relative path (same origin, no CORS).
 */

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const State = {
    refreshTimer: null,
    refreshInterval: 5, // seconds
    chartData: { unique: [], duplicates: [], labels: [], maxPoints: 30 },
    isOnline: false,
};

// ==========================================================================
// INIT
// ==========================================================================
document.addEventListener('DOMContentLoaded', () => {
    initParticles();
    initNavigation();
    initPublishForm();
    initDedupDemo();
    initRefreshButton();
    generateUUID('input-event-id');
    generateTimestamp();
    fetchHealthAndStats();
    startAutoRefresh();
});

// ==========================================================================
// PARTICLES
// ==========================================================================
function initParticles() {
    const canvas = $('#particles-canvas');
    const ctx = canvas.getContext('2d');
    let particles = [];

    function resize() { canvas.width = window.innerWidth; canvas.height = window.innerHeight; }
    resize();
    window.addEventListener('resize', resize);

    class Particle {
        constructor() { this.reset(); }
        reset() {
            this.x = Math.random() * canvas.width;
            this.y = Math.random() * canvas.height;
            this.vx = (Math.random() - 0.5) * 0.3;
            this.vy = (Math.random() - 0.5) * 0.3;
            this.radius = Math.random() * 1.5 + 0.5;
            this.alpha = Math.random() * 0.4 + 0.1;
        }
        update() {
            this.x += this.vx; this.y += this.vy;
            if (this.x < 0 || this.x > canvas.width) this.vx *= -1;
            if (this.y < 0 || this.y > canvas.height) this.vy *= -1;
        }
        draw() {
            ctx.beginPath();
            ctx.arc(this.x, this.y, this.radius, 0, Math.PI * 2);
            ctx.fillStyle = `rgba(91, 140, 255, ${this.alpha})`;
            ctx.fill();
        }
    }

    for (let i = 0; i < 45; i++) particles.push(new Particle());

    function animate() {
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        particles.forEach(p => { p.update(); p.draw(); });
        // connections
        for (let i = 0; i < particles.length; i++) {
            for (let j = i + 1; j < particles.length; j++) {
                const dx = particles[i].x - particles[j].x;
                const dy = particles[i].y - particles[j].y;
                const dist = Math.sqrt(dx * dx + dy * dy);
                if (dist < 150) {
                    ctx.beginPath();
                    ctx.moveTo(particles[i].x, particles[i].y);
                    ctx.lineTo(particles[j].x, particles[j].y);
                    ctx.strokeStyle = `rgba(91, 140, 255, ${0.06 * (1 - dist / 150)})`;
                    ctx.lineWidth = 0.5;
                    ctx.stroke();
                }
            }
        }
        requestAnimationFrame(animate);
    }
    animate();
}

// ==========================================================================
// NAVIGATION
// ==========================================================================
function initNavigation() {
    $$('.nav-item').forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            switchSection(item.dataset.section);
            $('#sidebar').classList.remove('open');
        });
    });
    $('#menu-toggle').addEventListener('click', () => $('#sidebar').classList.toggle('open'));
    document.addEventListener('click', (e) => {
        if (window.innerWidth <= 900 && !$('#sidebar').contains(e.target) && e.target !== $('#menu-toggle'))
            $('#sidebar').classList.remove('open');
    });
}

function switchSection(id) {
    $$('.nav-item').forEach(n => n.classList.remove('active'));
    $(`.nav-item[data-section="${id}"]`).classList.add('active');
    $$('.section').forEach(s => s.classList.remove('active'));
    $(`#section-${id}`).classList.add('active');

    const titles = {
        overview: ['Overview', 'Real-time system metrics & monitoring'],
        publish: ['Publish Event', 'Send events to the aggregator via HTTP API'],
        events: ['Event Log', 'Browse processed events from PostgreSQL'],
        dedup: ['Demo Dedup', 'Demonstrate deduplication & idempotency'],
        architecture: ['Arsitektur', 'System architecture & design overview'],
    };
    const [t, s] = titles[id] || ['', ''];
    $('#page-title').textContent = t;
    $('#page-subtitle').textContent = s;
}

// ==========================================================================
// API — uses relative paths (same origin as FastAPI)
// ==========================================================================
async function apiFetch(endpoint, options = {}) {
    try {
        const res = await fetch(endpoint, {
            ...options,
            headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
        });
        const data = await res.json();
        return { ok: res.ok, status: res.status, data };
    } catch (err) {
        return { ok: false, status: 0, data: null, error: err.message };
    }
}

// ==========================================================================
// HEALTH & STATS
// ==========================================================================
async function fetchHealthAndStats() {
    const health = await apiFetch('/health');
    updateHealthBanner(health);
    const stats = await apiFetch('/stats');
    updateStats(stats);
}

function updateHealthBanner(result) {
    const banner = $('#health-banner');
    const connDot = $('#connection-status .status-dot');
    const connText = $('#connection-status .status-text');

    if (result.ok && result.data) {
        const d = result.data;
        const ok = d.status === 'ok';
        banner.className = `health-banner ${ok ? 'healthy' : 'degraded'}`;
        $('#health-title').textContent = ok ? '✅ System Healthy' : '⚠️ System Degraded';
        $('#health-detail').textContent = ok ? 'All components operational' : 'Some components not responding';
        $('#db-status').textContent = d.database || '—';
        $('#broker-status').textContent = d.broker || '—';
        State.isOnline = true;
        connDot.className = 'status-dot online';
        connText.textContent = 'Connected';
    } else {
        banner.className = 'health-banner degraded';
        $('#health-title').textContent = '❌ Cannot connect to Aggregator';
        $('#health-detail').textContent = 'Pastikan docker compose sudah berjalan';
        $('#db-status').textContent = '—';
        $('#broker-status').textContent = '—';
        State.isOnline = false;
        connDot.className = 'status-dot offline';
        connText.textContent = 'Disconnected';
    }
}

function updateStats(result) {
    if (!result.ok || !result.data) return;
    const d = result.data;

    animateNumber('stat-received', d.received_total);
    animateNumber('stat-queued', d.queued_total);
    animateNumber('stat-unique', d.unique_processed);
    animateNumber('stat-duplicates', d.duplicates_dropped);

    const max = Math.max(d.received_total, 1);
    $('#bar-received').style.width = '100%';
    $('#bar-queued').style.width = `${(d.queued_total / max) * 100}%`;
    $('#bar-unique').style.width = `${(d.unique_processed / max) * 100}%`;
    $('#bar-duplicates').style.width = `${(d.duplicates_dropped / max) * 100}%`;

    $('#metric-throughput').textContent = d.throughput_events_per_second?.toFixed(1) ?? '—';
    $('#metric-duprate').textContent = d.duplicate_rate != null ? (d.duplicate_rate * 100).toFixed(1) + '%' : '—';
    $('#metric-topics').textContent = d.topic_count ?? '—';
    $('#metric-uptime').textContent = d.uptime_seconds != null ? formatUptime(d.uptime_seconds) : '—';
    $('#metric-failed').textContent = d.failed_total ?? '—';

    updateChart(d.unique_processed, d.duplicates_dropped);
}

function animateNumber(id, target) {
    const el = $(`#${id}`);
    const cur = parseInt(el.textContent.replace(/,/g, '')) || 0;
    const diff = target - cur;
    if (diff === 0) { el.textContent = target.toLocaleString(); return; }
    const start = performance.now();
    function step(ts) {
        const p = Math.min((ts - start) / 600, 1);
        const eased = 1 - Math.pow(1 - p, 3);
        el.textContent = Math.round(cur + diff * eased).toLocaleString();
        if (p < 1) requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
}

function formatUptime(s) {
    if (s < 60) return `${s.toFixed(0)}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${Math.floor(s % 60)}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
}

// ==========================================================================
// CHART
// ==========================================================================
function updateChart(unique, duplicates) {
    const cd = State.chartData;
    cd.labels.push(new Date().toLocaleTimeString());
    cd.unique.push(unique);
    cd.duplicates.push(duplicates);
    if (cd.labels.length > cd.maxPoints) { cd.labels.shift(); cd.unique.shift(); cd.duplicates.shift(); }
    drawChart();
}

function drawChart() {
    const canvas = $('#live-chart');
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);
    const W = rect.width, H = rect.height;
    const cd = State.chartData;
    ctx.clearRect(0, 0, W, H);

    if (cd.unique.length < 2) {
        ctx.fillStyle = 'rgba(139,149,176,0.3)';
        ctx.font = '14px Inter, sans-serif';
        ctx.textAlign = 'center';
        ctx.fillText('Waiting for data...', W / 2, H / 2);
        return;
    }

    const pad = { top: 20, right: 20, bottom: 30, left: 60 };
    const cW = W - pad.left - pad.right, cH = H - pad.top - pad.bottom;
    const maxVal = Math.max(...cd.unique, ...cd.duplicates, 1);

    // grid
    ctx.strokeStyle = 'rgba(99,115,170,0.08)';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i++) {
        const y = pad.top + (cH / 4) * i;
        ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(W - pad.right, y); ctx.stroke();
        ctx.fillStyle = 'rgba(139,149,176,0.5)'; ctx.font = '10px JetBrains Mono, monospace'; ctx.textAlign = 'right';
        ctx.fillText(fmtNum(maxVal - (maxVal / 4) * i), pad.left - 8, y + 4);
    }

    function drawLine(data, color, glowColor) {
        const step = cW / (data.length - 1);
        const toY = v => pad.top + cH - (v / maxVal) * cH;

        ctx.beginPath();
        ctx.moveTo(pad.left, toY(data[0]));
        for (let i = 1; i < data.length; i++) ctx.lineTo(pad.left + step * i, toY(data[i]));
        ctx.lineTo(pad.left + step * (data.length - 1), pad.top + cH);
        ctx.lineTo(pad.left, pad.top + cH);
        ctx.closePath();
        const grad = ctx.createLinearGradient(0, pad.top, 0, pad.top + cH);
        grad.addColorStop(0, glowColor); grad.addColorStop(1, 'transparent');
        ctx.fillStyle = grad; ctx.fill();

        ctx.beginPath();
        ctx.moveTo(pad.left, toY(data[0]));
        for (let i = 1; i < data.length; i++) ctx.lineTo(pad.left + step * i, toY(data[i]));
        ctx.strokeStyle = color; ctx.lineWidth = 2; ctx.lineJoin = 'round'; ctx.stroke();

        // dot
        const lx = pad.left + step * (data.length - 1), ly = toY(data[data.length - 1]);
        ctx.beginPath(); ctx.arc(lx, ly, 4, 0, Math.PI * 2); ctx.fillStyle = color; ctx.fill();
    }

    drawLine(cd.unique, '#3ddc97', 'rgba(61,220,151,0.1)');
    drawLine(cd.duplicates, '#ffaa5c', 'rgba(255,170,92,0.08)');

    // x labels
    const labelStep = Math.max(1, Math.floor(cd.labels.length / 6));
    const xStep = cW / (cd.labels.length - 1);
    ctx.fillStyle = 'rgba(139,149,176,0.4)'; ctx.font = '9px JetBrains Mono, monospace'; ctx.textAlign = 'center';
    for (let i = 0; i < cd.labels.length; i += labelStep) ctx.fillText(cd.labels[i], pad.left + xStep * i, H - 8);
}

function fmtNum(n) { return n >= 1e6 ? (n/1e6).toFixed(1)+'M' : n >= 1e3 ? (n/1e3).toFixed(1)+'K' : Math.round(n).toString(); }

// ==========================================================================
// AUTO-REFRESH
// ==========================================================================
function startAutoRefresh() {
    if (State.refreshTimer) clearInterval(State.refreshTimer);
    if (State.refreshInterval > 0)
        State.refreshTimer = setInterval(fetchHealthAndStats, State.refreshInterval * 1000);
}

function initRefreshButton() {
    $('#btn-refresh').addEventListener('click', async () => {
        const btn = $('#btn-refresh');
        btn.classList.add('spinning');
        await fetchHealthAndStats();
        setTimeout(() => btn.classList.remove('spinning'), 600);
        showToast('Data refreshed', 'success');
    });
}

// ==========================================================================
// PUBLISH
// ==========================================================================
function initPublishForm() {
    $('#btn-gen-uuid').addEventListener('click', () => generateUUID('input-event-id'));
    $('#btn-gen-timestamp').addEventListener('click', generateTimestamp);
    $('#publish-form').addEventListener('submit', async (e) => { e.preventDefault(); await publishSingle(); });
    $('#btn-publish-batch').addEventListener('click', publishBatch);
}

function generateUUID(targetId) {
    const uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
        const r = Math.random() * 16 | 0;
        return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });
    $(`#${targetId}`).value = uuid;
    return uuid;
}

function generateTimestamp() { $('#input-timestamp').value = new Date().toISOString(); }

async function publishSingle() {
    const btn = $('#btn-publish');
    btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> Publishing...';

    let payload;
    try { payload = JSON.parse($('#input-payload').value); } catch { showToast('Invalid JSON payload', 'error'); resetPublishBtn(); return; }

    const event = {
        topic: $('#input-topic').value.trim(),
        event_id: $('#input-event-id').value.trim(),
        timestamp: $('#input-timestamp').value.trim(),
        source: $('#input-source').value.trim(),
        payload
    };

    const r = await apiFetch('/publish', { method: 'POST', body: JSON.stringify(event) });
    showResponse(r);
    resetPublishBtn();

    if (r.ok) {
        showToast(`✅ Event published (accepted: ${r.data.accepted})`, 'success');
        generateUUID('input-event-id');
        generateTimestamp();
        fetchHealthAndStats();
    } else {
        showToast(`❌ Failed: ${r.error || r.data?.detail || 'Unknown'}`, 'error');
    }
}

async function publishBatch() {
    const btn = $('#btn-publish-batch');
    btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> Sending...';

    const topics = ['payment.created', 'auth.login', 'order.placed', 'user.signup', 'payment.refund'];
    const events = [];
    for (let i = 0; i < 5; i++) {
        events.push({
            topic: topics[i], event_id: generateUUID('input-event-id'),
            timestamp: new Date().toISOString(), source: 'demo-dashboard',
            payload: { batch_index: i, amount: Math.floor(Math.random() * 500000) + 10000, currency: 'IDR' }
        });
    }

    const r = await apiFetch('/publish', { method: 'POST', body: JSON.stringify(events) });
    showResponse(r);
    btn.disabled = false;
    btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg> Batch (5 events)';

    if (r.ok) { showToast(`✅ Batch sent (accepted: ${r.data.accepted})`, 'success'); fetchHealthAndStats(); }
    else showToast(`❌ Batch failed`, 'error');
}

function resetPublishBtn() {
    const btn = $('#btn-publish');
    btn.disabled = false;
    btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 2L11 13"/><path d="M22 2l-7 20-4-9-9-4z"/></svg> Publish Event';
}

function showResponse(result) {
    const c = $('#publish-response'); c.style.display = 'block';
    $('#publish-response-body').textContent = JSON.stringify(result.data || { error: result.error }, null, 2);
}

// ==========================================================================
// EVENTS
// ==========================================================================
$('#btn-fetch-events').addEventListener('click', fetchEvents);

async function fetchEvents() {
    const btn = $('#btn-fetch-events');
    btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> Loading...';

    const topic = $('#filter-topic').value.trim();
    let ep = `/events?limit=${$('#filter-limit').value}`;
    if (topic) ep += `&topic=${encodeURIComponent(topic)}`;

    const r = await apiFetch(ep);
    btn.disabled = false; btn.innerHTML = '🔍 Fetch';

    if (!r.ok || !r.data) { showToast('❌ Failed to fetch', 'error'); return; }

    const { events, count, topic: rt } = r.data;
    $('#events-count-banner').style.display = 'block';
    $('#events-count-text').textContent = `📦 Showing ${events.length} of ${count} event(s) — Topic: ${rt}`;

    const tbody = $('#events-tbody');
    if (events.length === 0) {
        tbody.innerHTML = '<tr class="empty-row"><td colspan="7">Tidak ada event</td></tr>';
        return;
    }

    tbody.innerHTML = events.map((e, i) => `
        <tr>
            <td>${i + 1}</td>
            <td><span class="topic-badge">${esc(e.topic)}</span></td>
            <td class="event-id-cell" title="${esc(e.event_id)}">${esc(e.event_id)}</td>
            <td>${esc(e.source)}</td>
            <td class="time-cell">${fmtTime(e.timestamp)}</td>
            <td class="time-cell">${fmtTime(e.processed_at)}</td>
            <td class="payload-cell" title='${esc(JSON.stringify(e.payload))}'>${esc(JSON.stringify(e.payload))}</td>
        </tr>
    `).join('');

    showToast(`📋 Loaded ${events.length} event(s)`, 'info');
}

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function fmtTime(s) { if (!s) return '—'; try { return new Date(s).toLocaleString('id-ID', { dateStyle: 'short', timeStyle: 'medium' }); } catch { return s; } }

// ==========================================================================
// DEDUP DEMO
// ==========================================================================
function initDedupDemo() {
    $('#btn-gen-dedup-id').addEventListener('click', () => {
        $('#dedup-event-id').value = `dedup-${generateUUID('dedup-event-id').slice(0, 8)}`;
    });
    $('#btn-dedup-demo').addEventListener('click', runDedupDemo);
}

async function runDedupDemo() {
    const btn = $('#btn-dedup-demo');
    const eventId = $('#dedup-event-id').value.trim();
    const count = parseInt($('#dedup-count').value);
    if (!eventId) { showToast('Masukkan Event ID', 'error'); return; }

    btn.disabled = true; btn.innerHTML = '<span class="spinner"></span> Running...';
    const resDiv = $('#dedup-results'); resDiv.style.display = 'block';
    const progDiv = $('#dedup-progress'); progDiv.innerHTML = '';
    const sumDiv = $('#dedup-summary'); sumDiv.innerHTML = '';
    const verDiv = $('#dedup-verify'); verDiv.innerHTML = '';

    const before = await apiFetch('/stats');
    const dupsBefore = before.ok ? before.data.duplicates_dropped : 0;
    const uniqBefore = before.ok ? before.data.unique_processed : 0;

    const event = {
        topic: 'dedup.test', event_id: eventId,
        timestamp: new Date().toISOString(), source: 'dedup-demo',
        payload: { demo: true, message: 'Deduplication test event' }
    };

    for (let i = 0; i < count; i++) {
        await sleep(400);
        const r = await apiFetch('/publish', { method: 'POST', body: JSON.stringify(event) });
        const step = document.createElement('div');
        step.className = `dedup-step ${i === 0 ? 'success' : 'duplicate'}`;
        step.innerHTML = `<span>${i === 0 ? '✅' : '🔄'}</span><span>Request #${i + 1}: POST /publish — event_id="${eventId}" — ${r.ok ? `accepted: ${r.data.accepted}` : 'failed'}</span>`;
        progDiv.appendChild(step);
        progDiv.scrollTop = progDiv.scrollHeight;
    }

    await sleep(1500);

    const after = await apiFetch('/stats');
    const newUniq = (after.ok ? after.data.unique_processed : 0) - uniqBefore;
    const newDups = (after.ok ? after.data.duplicates_dropped : 0) - dupsBefore;

    sumDiv.innerHTML = `
        <h4>📊 Ringkasan</h4>
        <div class="summary-grid">
            <div class="summary-item"><div class="summary-value" style="color:var(--accent-blue)">${count}</div><div class="summary-label">Total Dikirim</div></div>
            <div class="summary-item"><div class="summary-value" style="color:var(--accent-green)">${newUniq}</div><div class="summary-label">Unique Processed</div></div>
            <div class="summary-item"><div class="summary-value" style="color:var(--accent-orange)">${newDups}</div><div class="summary-label">Duplicates Dropped</div></div>
        </div>`;

    const ok = newUniq <= 1 && newDups >= count - 1;
    verDiv.innerHTML = `<strong>${ok ? '✅ BERHASIL' : '⏳ Masih diproses'}</strong> — ${ok ? `Event "${eventId}" hanya diproses 1x meskipun dikirim ${count}x. Deduplication bekerja!` : 'Worker mungkin masih memproses. Refresh untuk melihat hasil.'}`;

    btn.disabled = false; btn.innerHTML = '▶ Mulai Demo Dedup';
    fetchHealthAndStats();
    showToast(`Demo selesai: ${count} sent → ${newUniq} unique, ${newDups} duplicates`, 'success');
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ==========================================================================
// TOAST
// ==========================================================================
function showToast(msg, type = 'info') {
    const c = $('#toast-container');
    const t = document.createElement('div');
    const icons = { success: '✅', error: '❌', info: 'ℹ️' };
    t.className = `toast ${type}`;
    t.innerHTML = `<span>${icons[type] || 'ℹ️'}</span><span>${msg}</span>`;
    c.appendChild(t);
    setTimeout(() => { t.classList.add('hiding'); setTimeout(() => t.remove(), 300); }, 3500);
}
