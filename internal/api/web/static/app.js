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
                    replicate_attributes: this.f.replicate_attributes,
                    target_expire_at: this.f.target_expire_at,
                    noncurrent_expire_days: parseInt(this.f.noncurrent_expire_days) || 0,
                    newer_noncurrent_versions: parseInt(this.f.newer_noncurrent_versions) || 0,
                    require_object_lock: this.f.require_object_lock,
                    on_lock_missing: this.f.on_lock_missing,
                    accept_compliance: this.f.accept_compliance,
                    attrs_reconcile_interval: parseInt(this.f.attrs_reconcile_interval) || 0,
                    recreate_confirm: this.f.recreate_confirm,
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
        replicate_attributes: false, target_expire_at: '',
        noncurrent_expire_days: 0, newer_noncurrent_versions: 0,
        require_object_lock: false, on_lock_missing: 'fail',
        accept_compliance: false, attrs_reconcile_interval: 0,
        recreate_confirm: '',
    };
}

function txt(el, s) {
    el.textContent = s;
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
            let progressValue, progressClass;

            if (p.running) {
                progressClass = 'running';
                if (prog.total > 0) {
                    progressValue = `${prog.completed}/${prog.total} (${pct}%)`;
                } else if (prog.completed > 0) {
                    progressValue = `${prog.completed} copied`;
                } else {
                    progressValue = 'scanning...';
                }
            } else if (prog.total > 0) {
                progressClass = 'synced';
                progressValue = `${prog.completed}/${prog.total} (${pct}%)`;
            } else {
                progressClass = 'idle';
                progressValue = '—';
            }

            // Header
            let header = card.appendChild(document.createElement('div'));
            header.className = 'pair-header';

            let pill = header.appendChild(document.createElement('span'));
            pill.className = 'status-pill status-' + statusClass;
            txt(pill, status);

            let h2 = header.appendChild(document.createElement('h2'));
            txt(h2, p.name);

            // Badges: the settings that change what the target bucket does are
            // worth seeing without opening the edit modal.
            function addBadge(label, title, cls) {
                let b = header.appendChild(document.createElement('span'));
                b.className = 'badge' + (cls ? ' ' + cls : '');
                b.textContent = label;
                if (title) b.title = title;
                return b;
            }
            if (p.require_object_lock) {
                addBadge('lock', 'Object lock is required on the target bucket');
            }
            if (p.replicate_attributes) {
                addBadge('attrs', 'Object metadata, tags, retention and legal hold are replicated');
            }
            if (p.target_expire_at) {
                let when = new Date(p.target_expire_at);
                addBadge('expires ' + when.toISOString().slice(0, 10),
                    'The target bucket expires every object at this date', p.expired ? 'badge-danger' : '');
            }
            if (p.attrs_pending > 0) {
                addBadge(p.attrs_pending + ' attrs pending',
                    'Objects whose characteristics were only partially applied; the next cycle retries them',
                    'badge-warn');
            }
            if (p.expired) {
                addBadge('expired', 'The target expiry date has passed; this pair will not run until it is moved forward',
                    'badge-danger');
            }

            let editBtn = header.appendChild(document.createElement('button'));
            editBtn.className = 'btn btn-sm btn-secondary header-edit';
            editBtn.textContent = 'Edit';
            editBtn.dataset.action = 'edit';
            editBtn.dataset.id = p.id;
            editBtn.dataset.interval = p.sync_interval;
            editBtn.dataset.workers = p.worker_count;
            editBtn.dataset.maxOps = p.max_get_ops_per_minute;
            editBtn.dataset.webhookUrl = p.webhook_url || '';
            editBtn.dataset.webhookEvents = p.webhook_events || '';
            editBtn.dataset.dryRun = p.dry_run || false;
            editBtn.dataset.replicateAttributes = p.replicate_attributes || false;
            editBtn.dataset.attrsReconcileInterval = p.attrs_reconcile_interval || 0;
            editBtn.dataset.targetExpireAt = p.target_expire_at ? p.target_expire_at.slice(0, 10) : '';
            editBtn.dataset.noncurrentExpireDays = p.noncurrent_expire_days || 0;
            editBtn.dataset.newerNoncurrentVersions = p.newer_noncurrent_versions || 0;
            editBtn.dataset.targetBucket = p.target_bucket || '';
            editBtn.addEventListener('click', () => openEditModal(editBtn.dataset));

            // Stats
            let stats = card.appendChild(document.createElement('div'));
            stats.className = 'pair-stats';

            function addStat(label, value) {
                let row = stats.appendChild(document.createElement('div'));
                row.className = 'stat';
                let l = row.appendChild(document.createElement('span'));
                l.className = 'stat-label';
                txt(l, label);
                let v = row.appendChild(document.createElement('span'));
                v.className = 'stat-value';
                txt(v, value);
            }

            addStat('Source', p.source_url || p.source_name || p.source_bucket_id.slice(0,8));
            addStat('Target', p.target_url || p.target_name || p.target_bucket_id.slice(0,8));
            addStat('Interval', p.sync_interval + 's');
            addStat('Workers', p.worker_count);
            addStat('Last Sync', lastSync);
            addStat('Errors', p.consecutive_errors || 0);

            // Progress bar
            let progBar = stats.appendChild(document.createElement('div'));
            progBar.className = 'sync-progress ' + progressClass;
            let progFill = progBar.appendChild(document.createElement('div'));
            progFill.className = 'sync-bar';
            if (pct > 0) progFill.style.width = pct + '%';

            let progRow = stats.appendChild(document.createElement('div'));
            progRow.className = 'stat';
            let progLabel = progRow.appendChild(document.createElement('span'));
            progLabel.className = 'stat-label';
            txt(progLabel, 'Progress');
            let progVal = progRow.appendChild(document.createElement('span'));
            progVal.className = 'stat-value';
            txt(progVal, progressValue);

            // Actions
            let actions = card.appendChild(document.createElement('div'));
            actions.className = 'pair-actions';

            function addActionBtn(text, className, action, id, extra) {
                let btn = actions.appendChild(document.createElement('button'));
                btn.className = 'btn btn-sm ' + className;
                btn.textContent = text;
                btn.dataset.action = action;
                btn.dataset.id = id;
                if (extra) Object.assign(btn.dataset, extra);
                return btn;
            }

            addActionBtn('Sync Now', 'btn-primary', 'sync', p.id).addEventListener('click', async function() {
                this.disabled = true; this.textContent = 'Syncing...';
                try { await fetch('/api/sync-pairs/' + this.dataset.id + '/sync', { method: 'POST' }); } catch(e) {}
                setTimeout(pollStatus, 200);
            });

            addActionBtn(p.enabled ? 'Pause' : 'Resume', p.enabled ? 'btn-secondary' : 'btn-primary', 'toggle', p.id).addEventListener('click', async function() {
                this.disabled = true; this.textContent = '...';
                try { await fetch('/api/sync-pairs/' + this.dataset.id + '/disable', { method: 'POST' }); } catch(e) {}
                pollStatus();
            });

            addActionBtn('Reset', 'btn-secondary', 'reset', p.id).addEventListener('click', async function() {
                if (!confirm('Reset sync pair? This clears cache and status, then restarts from scratch.')) return;
                this.disabled = true; this.textContent = 'Resetting...';
                try { await fetch('/api/sync-pairs/' + this.dataset.id + '/reset', { method: 'POST' }); } catch(e) {}
                pollStatus();
            });

            addActionBtn('History', 'btn-secondary', 'history', p.id).addEventListener('click', () => {
                window.location.href = '/sync-pairs/' + p.id + '/history';
            });

            addActionBtn('Errors', 'btn-warning', 'errors', p.id, {name: p.name}).addEventListener('click', function() {
                showErrorModal(this.dataset.id, this.dataset.name);
            });

            addActionBtn('Delete', 'btn-danger', 'delete', p.id).addEventListener('click', async function() {
                if (!confirm('Delete this sync pair?')) return;
                this.disabled = true; this.textContent = 'Deleting...';
                try { await fetch('/api/sync-pairs/' + this.dataset.id, { method: 'DELETE' }); } catch(e) {}
                pollStatus();
            });

            grid.appendChild(card);
        });
    } catch(e) {
        console.error('poll failed', e);
    }
}

let editPairId = null;
// editState keeps the values the confirmations compare against: the expiry the
// pair has now, and the target bucket name a recreate must be spelled out.
let editState = { previousExpireAt: '', targetBucket: '' };
function openEditModal(d) {
    editPairId = d.id;
    editState.previousExpireAt = d.targetExpireAt || '';
    editState.targetBucket = d.targetBucket || '';
    document.getElementById('edit-interval').value = d.interval || '';
    document.getElementById('edit-workers').value = d.workers || '';
    document.getElementById('edit-max-ops').value = d.maxOps || '';
    document.getElementById('edit-webhook-url').value = d.webhookUrl || '';
    document.getElementById('edit-webhook-events').value = d.webhookEvents || '';
    document.getElementById('edit-dry-run').checked = d.dryRun === 'true';
    document.getElementById('edit-replicate-attributes').checked = d.replicateAttributes === 'true';
    document.getElementById('edit-attrs-reconcile-interval').value = d.attrsReconcileInterval || '';
    document.getElementById('edit-target-expire-at').value = d.targetExpireAt || '';
    document.getElementById('edit-noncurrent-expire-days').value = d.noncurrentExpireDays || '';
    document.getElementById('edit-newer-noncurrent-versions').value = d.newerNoncurrentVersions || '';
    document.getElementById('edit-accept-compliance').checked = false;
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
    let replicateAttributes = document.getElementById('edit-replicate-attributes').checked;
    let attrsInterval = parseInt(document.getElementById('edit-attrs-reconcile-interval').value);
    let expireAt = document.getElementById('edit-target-expire-at').value;
    let noncurrentDays = parseInt(document.getElementById('edit-noncurrent-expire-days').value);
    let newerVersions = parseInt(document.getElementById('edit-newer-noncurrent-versions').value);
    let acceptCompliance = document.getElementById('edit-accept-compliance').checked;

    // Moving the expiry earlier deletes data sooner than promised, so it takes an
    // explicit confirmation that names both dates. The server enforces the same
    // rule; this is the readable version of it.
    let confirmEarlier = false;
    if (expireAt && editState.previousExpireAt && expireAt < editState.previousExpireAt) {
        confirmEarlier = confirm(
            'Move the target expiry earlier?\n\n' +
            'from: ' + editState.previousExpireAt + '\n' +
            'to:   ' + expireAt + '\n\n' +
            'Objects already copied will be deleted sooner than planned.');
        if (!confirmEarlier) return;
    }

    let body = {};
    body.replicate_attributes = replicateAttributes;
    body.accept_compliance = acceptCompliance;
    body.confirm_expire_earlier = confirmEarlier;
    body.target_expire_at = expireAt;
    if (!isNaN(noncurrentDays)) body.noncurrent_expire_days = noncurrentDays;
    if (!isNaN(newerVersions)) body.newer_noncurrent_versions = newerVersions;
    if (!isNaN(attrsInterval)) body.attrs_reconcile_interval = attrsInterval;
    if (!isNaN(interval)) body.sync_interval = interval;
    if (!isNaN(workers)) body.worker_count = workers;
    if (!isNaN(maxOps)) body.max_get_ops_per_minute = maxOps;
    if (webhookUrl) body.webhook_url = webhookUrl;
    if (webhookEvents) body.webhook_events = webhookEvents;
    body.dry_run = dryRun;
    try {
        let res = await fetch('/api/sync-pairs/' + editPairId, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
        });
        let json = await res.json();
        if (!res.ok) {
            // Closing on failure would leave the operator believing the change
            // was saved when the preflight refused it.
            alert((json.error || 'Update failed') + warningsText(json.warnings));
            return;
        }
        let warns = warningsText(json.warnings);
        if (warns) alert(warns);
    } catch(e) {
        alert('Network error');
        return;
    }
    closeEditModal();
    pollStatus();
}

function showErrorModal(pairId, pairName) {
    document.getElementById('error-modal-name').textContent = pairName;
    let el = document.getElementById('error-list');
    el.textContent = 'Loading...';
    document.getElementById('error-modal').style.display = 'flex';
    (async () => {
        try {
            let res = await fetch('/api/sync-pairs/' + pairId + '/logs');
            let json = await res.json();
            let logs = json.data || json || [];
            let errors = logs.filter(l => l.status === 'error' || l.error_msg);
            el.innerHTML = '';
            if (errors.length === 0) {
                el.textContent = 'No errors.';
                return;
            }
            errors.forEach(l => {
                let d = document.createElement('div');
                d.style.cssText = 'padding:8px 0;border-bottom:1px solid var(--hairline);';
                let t = l.completed_at ? new Date(l.completed_at).toLocaleString() : '—';
                let timeDiv = d.appendChild(document.createElement('div'));
                timeDiv.style.cssText = 'color:var(--red);font-weight:600';
                timeDiv.textContent = t;
                let msgDiv = d.appendChild(document.createElement('div'));
                msgDiv.textContent = l.error_msg || 'Unknown error';
                el.appendChild(d);
            });
        } catch(e) {
            el.textContent = 'Failed to load errors.';
        }
    })();
}
function closeErrorModal() {
    document.getElementById('error-modal').style.display = 'none';
}

document.addEventListener('DOMContentLoaded', () => {
    if (document.getElementById('pair-grid')) {
        pollStatus();
        setInterval(pollStatus, 5000);
    }
});

// warningsText renders advisory warnings from a create/update response. They are
// never errors: the change succeeded, but something about it will surprise the
// operator later.
function warningsText(warnings) {
    if (!warnings || warnings.length === 0) return '';
    return '\n\nWarnings:\n' + warnings.map(w => '- ' + w.message).join('\n');
}
