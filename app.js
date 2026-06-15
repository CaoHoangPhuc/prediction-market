// Shared client-side utilities for the Prediction Market app.
// Loaded by both /worldcup and / (markets) pages. Provides auth state, an
// auth-aware fetch wrapper, login/register/logout UI, and a few small helpers
// (esc, toast) that were previously duplicated across both HTML pages.

const PM = (() => {
  const TOKEN_KEY = 'pm-auth-token';
  const USER_KEY  = 'pm-user';

  let user = null;
  let token = null;
  let _authListeners = [];

  // ── State ────────────────────────────────────────────────────────────────
  function load() {
    token = localStorage.getItem(TOKEN_KEY) || null;
    try { user = JSON.parse(localStorage.getItem(USER_KEY) || 'null'); } catch { user = null; }
  }
  function save(t, u) {
    token = t; user = u;
    if (t) localStorage.setItem(TOKEN_KEY, t); else localStorage.removeItem(TOKEN_KEY);
    if (u) localStorage.setItem(USER_KEY, JSON.stringify(u)); else localStorage.removeItem(USER_KEY);
    _authListeners.forEach(fn => { try { fn(user); } catch (e) { console.error(e); } });
  }
  function getUser() { return user; }
  function getToken() { return token; }
  function isAdmin() { return !!(user && user.is_admin); }
  function onAuthChange(fn) { _authListeners.push(fn); }

  // ── API ──────────────────────────────────────────────────────────────────
  async function api(path, opts = {}) {
    const headers = Object.assign(
      { 'Content-Type': 'application/json' },
      opts.headers || {}
    );
    if (token) headers['Authorization'] = 'Bearer ' + token;
    const r = await fetch(path, Object.assign({}, opts, { headers }));
    if (r.status === 401) {
      // Stale or invalid token — clear the cached session. If we were
      // actively logged in (had a cached user), surface a toast so the user
      // knows WHY they got logged out — otherwise it looks like their
      // login was rejected.
      const wasAuthed = !!user;
      save(null, null);
      const err = await r.json().catch(() => ({ detail: 'Unauthorized' }));
      if (wasAuthed) {
        // Defer the toast so it appears AFTER any in-flight error rendering.
        setTimeout(() => toast('Session expired — please log in again', 'error'), 100);
      }
      throw Object.assign(new Error(err.detail || 'Unauthorized'), { status: 401 });
    }
    if (!r.ok) {
      const err = await r.json().catch(() => ({ detail: r.statusText }));
      throw Object.assign(new Error(err.detail || 'API error'), { status: r.status });
    }
    return r.json();
  }

  // ── Auth actions ─────────────────────────────────────────────────────────
  async function register(name, password, email) {
    const r = await api('/api/auth/register', {
      method: 'POST',
      body: JSON.stringify({ name, password, email: email || null }),
    });
    save(r.token, r.user);
    return r;
  }
  async function login(name, password) {
    const r = await api('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify({ name, password }),
    });
    save(r.token, r.user);
    return r;
  }
  async function logout() {
    try { await api('/api/auth/logout', { method: 'POST' }); } catch (_) {}
    save(null, null);
  }
  async function refreshMe() {
    if (!token) { save(null, null); return null; }
    try {
      const u = await api('/api/auth/me');
      save(token, u);
      return u;
    } catch (_) { save(null, null); return null; }
  }

  // ── UI: modals ───────────────────────────────────────────────────────────
  function injectModal() {
    if (document.getElementById('pm-auth-modal')) return;
    const div = document.createElement('div');
    div.id = 'pm-auth-modal';
    div.className = 'modal-overlay';
    div.innerHTML = `
      <div class="modal" style="max-width:380px">
        <h3 id="pm-auth-title">Login</h3>
        <div class="field">
          <label id="pm-auth-name-label">Username</label>
          <input type="text" id="pm-auth-name" autocomplete="username">
        </div>
        <div class="field" id="pm-auth-email-field" style="display:none">
          <label>Email (optional)</label>
          <input type="email" id="pm-auth-email" autocomplete="email">
        </div>
        <div class="field">
          <label>Password</label>
          <input type="password" id="pm-auth-password" autocomplete="current-password">
        </div>
        <div id="pm-auth-error" style="color:var(--red);font-size:12px;margin-bottom:8px;min-height:14px"></div>
        <div class="actions">
          <button onclick="PM.hideAuth()" class="outline">Cancel</button>
          <button id="pm-auth-submit" onclick="PM.submitAuth()" class="gold">Login</button>
        </div>
        <div style="text-align:center;margin-top:10px;font-size:12px;color:var(--text-dim)">
          <a href="#" id="pm-auth-toggle" onclick="event.preventDefault();PM.toggleAuthMode()" style="color:var(--accent)">Need an account? Register</a>
        </div>
      </div>
    `;
    document.body.appendChild(div);
    div.addEventListener('click', (e) => { if (e.target === div) PM.hideAuth(); });
    document.getElementById('pm-auth-password').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') PM.submitAuth();
    });
    document.getElementById('pm-auth-name').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') document.getElementById('pm-auth-password').focus();
    });
  }
  let _authMode = 'login';
  function showLogin() {
    injectModal();
    _authMode = 'login';
    document.getElementById('pm-auth-title').textContent = 'Login';
    document.getElementById('pm-auth-submit').textContent = 'Login';
    document.getElementById('pm-auth-email-field').style.display = 'none';
    document.getElementById('pm-auth-toggle').textContent = 'Need an account? Register';
    document.getElementById('pm-auth-error').textContent = '';
    document.getElementById('pm-auth-name').value = '';
    document.getElementById('pm-auth-password').value = '';
    document.getElementById('pm-auth-modal').classList.add('show');
    setTimeout(() => document.getElementById('pm-auth-name').focus(), 50);
  }
  function showRegister() {
    injectModal();
    _authMode = 'register';
    document.getElementById('pm-auth-title').textContent = 'Create account';
    document.getElementById('pm-auth-submit').textContent = 'Create';
    document.getElementById('pm-auth-email-field').style.display = 'block';
    document.getElementById('pm-auth-toggle').textContent = 'Already have an account? Login';
    document.getElementById('pm-auth-error').textContent = '';
    document.getElementById('pm-auth-name').value = '';
    document.getElementById('pm-auth-email').value = '';
    document.getElementById('pm-auth-password').value = '';
    document.getElementById('pm-auth-modal').classList.add('show');
    setTimeout(() => document.getElementById('pm-auth-name').focus(), 50);
  }
  function toggleAuthMode() { _authMode === 'login' ? showRegister() : showLogin(); }
  function hideAuth() { document.getElementById('pm-auth-modal')?.classList.remove('show'); }
  async function submitAuth() {
    const name = document.getElementById('pm-auth-name').value.trim();
    const password = document.getElementById('pm-auth-password').value;
    const email = document.getElementById('pm-auth-email')?.value.trim();
    const errEl = document.getElementById('pm-auth-error');
    errEl.textContent = '';
    if (!name || !password) { errEl.textContent = 'Name and password required'; return; }
    try {
      if (_authMode === 'login') await login(name, password);
      else await register(name, password, email);
      hideAuth();
      toast('Welcome, ' + user.name + '!', 'success');
    } catch (e) { errEl.textContent = e.message; }
  }

  // ── User-bar renderer (call from page to set up header) ────────────────
  function renderUserBar(container) {
    if (!container) return;
    if (user) {
      container.innerHTML = `
        <span style="font-size:13px;color:var(--text-dim)">Logged in as <b style="color:var(--text)">${esc(user.name)}</b> · ${user.points.toFixed(0)} pts</span>
        <button class="sm outline" onclick="PM.logout().then(()=>location.reload())">Logout</button>
      `;
    } else {
      container.innerHTML = `
        <button class="sm" onclick="PM.showLogin()">Login</button>
        <button class="sm outline" onclick="PM.showRegister()">Register</button>
      `;
    }
  }

  // ── Helpers ──────────────────────────────────────────────────────────────
  function esc(s) {
    const d = document.createElement('div'); d.textContent = s == null ? '' : s; return d.innerHTML;
  }
  function toast(msg, type) {
    // Reuse the page's #toast element (which has the .toast CSS) so we get
    // proper styling. Fall back to creating a new element if absent.
    let t = document.getElementById('toast') || document.getElementById('pm-toast');
    if (!t) {
      t = document.createElement('div');
      t.id = 'toast';
      t.className = 'toast';
      document.body.appendChild(t);
    }
    t.textContent = msg;
    t.className = 'toast ' + (type || '') + ' show';
    clearTimeout(PM._toastTimer);
    PM._toastTimer = setTimeout(() => t.classList.remove('show'), 3000);
  }

  // ── Init ─────────────────────────────────────────────────────────────────
  load();
  return {
    api, esc, toast, onAuthChange,
    getUser, getToken, isAdmin,
    login, register, logout, refreshMe,
    showLogin, showRegister, toggleAuthMode, hideAuth, submitAuth, renderUserBar,
  };
})();

// Auto-init: refresh me on page load. Reuse the page's #toast if present.
(async () => {
  if (!document.getElementById('toast') && !document.getElementById('pm-toast')) {
    const t = document.createElement('div');
    t.id = 'toast';
    t.className = 'toast';
    document.body.appendChild(t);
  }
  // Refresh /api/auth/me to validate token + get fresh user data
  await PM.refreshMe();
})();
