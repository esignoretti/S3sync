// Poll sync pair status every 5s
async function pollStatus() {
    try {
        const res = await fetch('/api/sync-pairs');
        const json = await res.json();
        const grid = document.getElementById('pair-grid');
        if (json.data && grid) {
            // Simple update — clear and rebuild
            grid.innerHTML = '';
            json.data.forEach(pair => {
                const card = document.createElement('div');
                card.className = 'pair-card';
                const status = pair.last_sync_status || 'never';
                card.innerHTML = `
                    <div class="pair-header">
                        <span class="status-pill status-${status === 'ok' ? 'synced' : status}">${status}</span>
                        <h2>${pair.name}</h2>
                    </div>
                    <div class="pair-stats">
                        <div class="stat">
                            <span class="stat-label">Source</span>
                            <span class="stat-value">${pair.source_bucket_id.slice(0, 8)}</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Target</span>
                            <span class="stat-value">${pair.target_bucket_id.slice(0, 8)}</span>
                        </div>
                        <div class="stat">
                            <span class="stat-label">Last Sync</span>
                            <span class="stat-value">${pair.last_sync_at || '—'}</span>
                        </div>
                    </div>
                `;
                grid.appendChild(card);
            });
        }
    } catch(e) {
        console.error('poll failed', e);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    pollStatus();
    setInterval(pollStatus, 5000);
});
