/** Same-origin transport. Session credentials remain in HttpOnly cookies. */
export const session = {user: null, csrf: null, grafanaUrl: null};
let refreshFlight = null;

export class ApiError extends Error {
  constructor(message, status, code, correlation) {
    super(message); this.status = status; this.code = code; this.correlation = correlation;
  }
}

function cookie(name) {
  const entry = document.cookie.split('; ').find(part => part.startsWith(`${name}=`));
  if (!entry) return null;
  try { return decodeURIComponent(entry.slice(name.length + 1)); } catch { return null; }
}

export function rememberSession(data) {
  session.user = data.user; session.csrf = data.csrf_token || cookie('gs_csrf');
  if (data.grafana_url) session.grafanaUrl = data.grafana_url;
}

async function transport(path, options) {
  if (!/^\/[a-zA-Z0-9_/?=&.:%+\-]*$/.test(path) || path.startsWith('//')) throw new Error('Invalid API path');
  const method = options.method || 'GET';
  const headers = {'Accept': 'application/json', 'X-Correlation-ID': crypto.randomUUID()};
  if (method !== 'GET') {
    headers['Content-Type'] = 'application/json';
    const csrf = session.csrf || cookie('gs_csrf');
    if (csrf) headers['X-CSRF-Token'] = csrf;
    if (!path.startsWith('/auth/')) headers['Idempotency-Key'] = options.key || crypto.randomUUID();
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 25000);
  let response;
  try {
    response = await fetch(`/api/v1${path}`, {method, credentials: 'same-origin', headers,
      signal: controller.signal, body: method === 'GET' ? undefined : JSON.stringify(options.body || {})});
  } catch (error) {
    throw new ApiError(error.name === 'AbortError' ? 'The request timed out. Refresh to try again.' : 'The service could not be reached. Check your connection and try again.', 0, 'CONNECTION_ERROR');
  } finally { clearTimeout(timer); }
  let envelope;
  try { envelope = await response.json(); } catch {
    throw new ApiError(`The service returned an unreadable response (HTTP ${response.status}).`, response.status, 'INVALID_RESPONSE');
  }
  if (!response.ok || !envelope.success) {
    throw new ApiError(envelope.error?.message || `Request failed (HTTP ${response.status}).`, response.status,
      envelope.error?.code || 'REQUEST_FAILED', envelope.meta?.correlation_id);
  }
  return envelope.data;
}

export async function api(path, options = {}) {
  try { return await transport(path, options); } catch (error) {
    if (error.status === 401 && !path.startsWith('/auth/login') && !path.startsWith('/auth/refresh') && !options.refreshed && cookie('gs_csrf')) {
      if (!refreshFlight) refreshFlight = transport('/auth/refresh', {method: 'POST'}).then(rememberSession).finally(() => { refreshFlight = null; });
      try { await refreshFlight; return await api(path, {...options, refreshed: true}); } catch (refreshError) {
        session.user = null; session.csrf = null;
        document.dispatchEvent(new CustomEvent('session-expired')); throw refreshError;
      }
    }
    if (error.status === 401 && !path.startsWith('/auth/login')) {
      session.user = null; session.csrf = null; document.dispatchEvent(new CustomEvent('session-expired'));
    }
    throw error;
  }
}

export const can = permission => Boolean(session.user?.permissions?.includes(permission));
export function query(values = {}) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) if (value !== '' && value !== null && value !== undefined) params.set(key, value);
  return params.size ? `?${params}` : '';
}
