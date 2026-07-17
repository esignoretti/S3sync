document.addEventListener('alpine:init', () => {
    Alpine.data('setupWizard', () => ({
        step: 1,
        session: '',
        error: '',
        loading: false,
        f: defaultForm(),
        async submit(role) {
            if (this.loading) return;
            this.loading = true;
            this.error = '';
            let headers = { 'Content-Type': 'application/json' };
            if (this.session) headers['X-Setup-Session'] = this.session;

            let body = {};
            if (role === 'source') {
                body = {
                    name: this.f.name, endpoint: this.f.endpoint, region: this.f.region,
                    bucket_name: this.f.bucket_name, access_key: this.f.access_key,
                    secret_key: this.f.secret_key,
                };
            } else if (role === 'target') {
                body = {
                    name: this.f.name, endpoint: this.f.endpoint, region: this.f.region,
                    bucket_name: this.f.bucket_name, access_key: this.f.access_key,
                    secret_key: this.f.secret_key, versioning: this.f.versioning,
                    object_lock: this.f.object_lock, retention_mode: this.f.retention_mode,
                    retention_days: parseInt(this.f.retention_days) || 365,
                };
            } else {
                body = {
                    pair_name: this.f.pair_name,
                    sync_interval: parseInt(this.f.sync_interval) || 300,
                    worker_count: parseInt(this.f.worker_count) || 10,
                    max_get_ops_per_minute: parseInt(this.f.max_get_ops_per_minute) || 600,
                    delete_propagation: this.f.delete_propagation,
                    target_storage_class: this.f.target_storage_class,
                };
            }

            try {
                let res = await fetch('/api/setup', { method: 'POST', headers, body: JSON.stringify(body) });
                let json = await res.json();
                if (!res.ok) {
                    this.error = json.error || 'Request failed';
                    this.loading = false;
                    return;
                }
                let d = json.data || json;
                this.session = d.session;
                this.step = d.step_num;
                if (d.step_num === 2) this.f = defaultForm();
                if (d.done) { this.step = 4; }
            } catch(e) {
                this.error = 'Network error';
            }
            this.loading = false;
        }
    }));
});

function defaultForm() {
    return {
        name: '', endpoint: 'https://s3.cubbit.eu', region: 'eu-west-1',
        bucket_name: '', access_key: '', secret_key: '',
        versioning: false, object_lock: false,
        retention_mode: 'GOVERNANCE', retention_days: 365,
        pair_name: '', sync_interval: 300, worker_count: 10,
        max_get_ops_per_minute: 600, delete_propagation: true,
        target_storage_class: '',
    };
}

// Dashboard polling
async function pollStatus() {
    try {
        let res = await fetch('/api/sync-pairs');
        let json = await res.json();
        let grid = document.getElementById('pair-grid');
        if (!grid || !json.data) return;
        grid.innerHTML = '';
        if (json.data.length === 0) {
            grid.innerHTML = '<div class="empty-state"><p>No sync pairs configured.</p><a href="/setup" class="btn btn-primary">Run Setup Wizard</a></div>';
            return;
        }
        json.data.forEach(p => {
            let card = document.createElement('div');
            card.className = 'pair-card';
            card.dataset.pairId = p.id;
            let status = p.running ? 'running' : (p.last_sync_status || 'never');
            let statusClass = status === 'ok' ? 'synced' : status === 'error' ? 'error' : status === 'running' ? 'running' : 'idle';
            let lastSync = p.last_sync_at ? new Date(p.last_sync_at).toLocaleString() : '—';
            let prog = p.progress || {};
            let pct = prog.total > 0 ? Math.round(prog.completed / prog.total * 100) : 0;
            let progressHTML = '';
            if (prog.total > 0) {
                if (p.running) statusClass = 'running';
                progressHTML = `<div class="sync-progress ${p.running ? 'running' : 'synced'}"><div class="sync-bar" style="width:${pct}%"></div></div>
                    <div class="stat"><span class="stat-label">Progress</span><span class="stat-value">${prog.completed}/${prog.total} (${pct}%)</span></div>`;
            } else if (p.last_error) {
                progressHTML = `<div class="stat error-detail"><span class="stat-label">Error</span><span class="stat-value">${p.last_error}</span></div>`;
            } else if (!p.running) {
                progressHTML = `<div class="sync-progress idle"><div class="sync-bar"></div></div>`;
            }
            card.innerHTML = `
                <div class="pair-header">
                    <span class="status-pill status-${statusClass}">${status}</span>
                    <h2>${p.name}</h2>
                </div>
                <div class="pair-stats">
                    <div class="stat"><span class="stat-label">Source</span><span class="stat-value">${p.source_url || p.source_name || p.source_bucket_id.slice(0,8)}</span></div>
                    <div class="stat"><span class="stat-label">Target</span><span class="stat-value">${p.target_url || p.target_name || p.target_bucket_id.slice(0,8)}</span></div>
                    <div class="stat"><span class="stat-label">Interval</span><span class="stat-value" data-field="sync_interval">${p.sync_interval}s</span></div>
                    <div class="stat"><span class="stat-label">Workers</span><span class="stat-value" data-field="worker_count">${p.worker_count}</span></div>
                    <div class="stat"><span class="stat-label">Last Sync</span><span class="stat-value">${lastSync}</span></div>
                    <div class="stat"><span class="stat-label">Errors</span><span class="stat-value">${p.consecutive_errors || 0}</span></div>
                    ${progressHTML}
                </div>
                <div class="pair-actions">
                    <button class="btn btn-sm btn-primary" data-action="sync" data-id="${p.id}">Sync Now</button>
                    <button class="btn btn-sm ${p.enabled ? 'btn-secondary' : 'btn-primary'}" data-action="toggle" data-id="${p.id}">${p.enabled ? 'Pause' : 'Resume'}</button>
                    <button class="btn btn-sm btn-secondary" data-action="edit" data-id="${p.id}" data-interval="${p.sync_interval}" data-workers="${p.worker_count}" data-max-ops="${p.max_get_ops_per_minute}" data-webhook-url="${p.webhook_url || ''}" data-webhook-events="${p.webhook_events || ''}" data-dry-run="${p.dry_run || false}">Edit</button>
                    <button class="btn btn-sm btn-secondary" data-action="reset" data-id="${p.id}">Reset</button>
                    <button class="btn btn-sm btn-danger" data-action="delete" data-id="${p.id}">Delete</button>
                </div>
            `;
            grid.appendChild(card);
        });
        grid.querySelectorAll('[data-action="sync"]').forEach(btn => {
            btn.addEventListener('click', async () => {
                btn.disabled = true; btn.textContent = 'Syncing...';
                try { await fetch('/api/sync-pairs/' + btn.dataset.id + '/sync', { method: 'POST' }); } catch(e) {}
                setTimeout(pollStatus, 200); // immediate re-poll
            });
        });
        grid.querySelectorAll('[data-action="delete"]').forEach(btn => {
            btn.addEventListener('click', async () => {
                if (!confirm('Delete this sync pair?')) return;
                btn.disabled = true; btn.textContent = 'Deleting...';
                try { await fetch('/api/sync-pairs/' + btn.dataset.id, { method: 'DELETE' }); } catch(e) {}
                pollStatus();
            });
        });
        grid.querySelectorAll('[data-action="toggle"]').forEach(btn => {
            btn.addEventListener('click', async () => {
                btn.disabled = true; btn.textContent = '...';
                try { await fetch('/api/sync-pairs/' + btn.dataset.id + '/disable', { method: 'POST' }); } catch(e) {}
                pollStatus();
            });
        });
        grid.querySelectorAll('[data-action="edit"]').forEach(btn => {
            btn.addEventListener('click', async () => {
                openEditModal(btn.dataset.id, btn.dataset.interval, btn.dataset.workers, btn.dataset.maxOps, btn.dataset.webhookUrl, btn.dataset.webhookEvents, btn.dataset.dryRun);
            });
        });
        grid.querySelectorAll('[data-action="reset"]').forEach(btn => {
            btn.addEventListener('click', async () => {
                if (!confirm('Reset sync pair? This clears cache and status, then restarts from scratch.')) return;
                btn.disabled = true; btn.textContent = 'Resetting...';
                try { await fetch('/api/sync-pairs/' + btn.dataset.id + '/reset', { method: 'POST' }); } catch(e) {}
                pollStatus();
            });
        });
    } catch(e) {
        console.error('poll failed', e);
    }
}

let editPairId = null;
function openEditModal(pairId, interval, workers, maxOps, webhookUrl, webhookEvents, dryRun) {
    editPairId = pairId;
    document.getElementById('edit-interval').value = interval || '';
    document.getElementById('edit-workers').value = workers || '';
    document.getElementById('edit-max-ops').value = maxOps || '';
    document.getElementById('edit-webhook-url').value = webhookUrl || '';
    document.getElementById('edit-webhook-events').value = webhookEvents || '';
    document.getElementById('edit-dry-run').checked = dryRun === 'true';
    document.getElementById('edit-modal').style.display = 'flex';
}
function closeEditModal() {
    document.getElementById('edit-modal').style.display = 'none';
    editPairId = null;
}
async function saveEdit() {
    let interval = parseInt(document.getElementById('edit-interval').value);
    let workers = parseInt(document.getElementById('edit-workers').value);
    let maxOps = parseInt(document.getElementById('edit-max-ops').value);
    let webhookUrl = document.getElementById('edit-webhook-url').value;
    let webhookEvents = document.getElementById('edit-webhook-events').value;
    let dryRun = document.getElementById('edit-dry-run').checked;
    let body = {};
    if (!isNaN(interval)) body.sync_interval = interval;
    if (!isNaN(workers)) body.worker_count = workers;
    if (!isNaN(maxOps)) body.max_get_ops_per_minute = maxOps;
    if (webhookUrl) body.webhook_url = webhookUrl;
    if (webhookEvents) body.webhook_events = webhookEvents;
    body.dry_run = dryRun;
    try {
        await fetch('/api/sync-pairs/' + editPairId, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
    } catch(e) {}
    closeEditModal();
    pollStatus();
}

document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('pair-grid')) {
        pollStatus();
        setInterval(pollStatus, 5000);
    }
});
