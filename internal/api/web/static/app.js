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
                    retention_days: this.f.retention_days,
                };
            } else {
                body = {
                    pair_name: this.f.pair_name, sync_interval: this.f.sync_interval,
                    worker_count: this.f.worker_count, max_get_ops_per_minute: this.f.max_get_ops_per_minute,
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
            let status = p.last_sync_status || 'never';
            let statusClass = status === 'ok' ? 'synced' : status === 'error' ? 'error' : status === 'running' ? 'running' : 'idle';
            let lastSync = p.last_sync_at ? new Date(p.last_sync_at).toLocaleString() : '—';
            card.innerHTML = `
                <div class="pair-header">
                    <span class="status-pill status-${statusClass}">${status}</span>
                    <h2>${p.name}</h2>
                </div>
                <div class="pair-stats">
                    <div class="stat"><span class="stat-label">Source</span><span class="stat-value">${p.source_bucket_id.slice(0,8)}</span></div>
                    <div class="stat"><span class="stat-label">Target</span><span class="stat-value">${p.target_bucket_id.slice(0,8)}</span></div>
                    <div class="stat"><span class="stat-label">Interval</span><span class="stat-value">${p.sync_interval}s</span></div>
                    <div class="stat"><span class="stat-label">Last Sync</span><span class="stat-value">${lastSync}</span></div>
                </div>
                <div class="pair-actions">
                    <button class="btn btn-sm btn-primary" data-action="sync" data-id="${p.id}">Sync Now</button>
                    <button class="btn btn-sm ${p.enabled ? 'btn-danger' : 'btn-secondary'}" data-action="toggle" data-id="${p.id}">${p.enabled ? 'Stop' : 'Start'}</button>
                    <button class="btn btn-sm btn-danger" data-action="delete" data-id="${p.id}">Delete</button>
                </div>
            `;
            grid.appendChild(card);
        });
        grid.querySelectorAll('[data-action="sync"]').forEach(btn => {
            btn.addEventListener('click', async () => {
                btn.disabled = true; btn.textContent = 'Syncing...';
                try { await fetch('/api/sync-pairs/' + btn.dataset.id + '/sync', { method: 'POST' }); } catch(e) {}
                pollStatus();
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
    } catch(e) {
        console.error('poll failed', e);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('pair-grid')) {
        pollStatus();
        setInterval(pollStatus, 5000);
    }
});
