/** Safe DOM primitives shared by every view. Untrusted values are text nodes. */
export function el(tag, attributes = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attributes)) {
    if (key === 'on') { for (const [event, handler] of Object.entries(value)) node.addEventListener(event, handler); }
    else if (value !== false && value !== null && value !== undefined) {
      if (key === 'value') node.value = value;
      else node.setAttribute(key === 'className' ? 'class' : key, value === true ? '' : String(value));
    }
  }
  for (const child of children.flat(Infinity)) if (child !== null && child !== undefined) node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  return node;
}

const paths = {
  grid:'<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/>',
  meter:'<rect x="5" y="3" width="14" height="18" rx="3"/><path d="M8 7h8M8 17h8M12 9l-3 5h4l-1 3 4-6h-4z"/>',
  pulse:'<path d="M2 12h4l3-7 5 14 3-7h5"/>', network:'<rect x="9" y="2" width="6" height="6" rx="1"/><rect x="2" y="16" width="6" height="6" rx="1"/><rect x="16" y="16" width="6" height="6" rx="1"/><path d="M12 8v4H5v4m7-4h7v4"/>',
  map:'<path d="M3 5l6-2 6 2 6-2v16l-6 2-6-2-6 2zM9 3v16M15 5v16"/>',
  alert:'<path d="M10.3 4.2L2.7 17.5A2 2 0 004.4 21h15.2a2 2 0 001.7-3.5L13.7 4.2a2 2 0 00-3.4 0zM12 9v4M12 17h.01"/>',
  quality:'<path d="M9 3H5a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-4M8 12l4 4L21 5"/>',
  clock:'<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>', model:'<path d="M12 3v18M6 5l-3 3v8l3 3M18 5l3 3v8l-3 3M8 8h8M8 16h8"/><circle cx="12" cy="12" r="3"/>',
  report:'<path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8zM14 2v6h6M8 13h8M8 17h6"/>',
  code:'<path d="M8 6l-6 6 6 6M16 6l6 6-6 6M14 3l-4 18"/>', settings:'<path d="M12 3l2 3 4-1 1 4 3 3-3 3-1 4-4-1-2 3-2-3-4 1-1-4-3-3 3-3 1-4 4 1z"/><circle cx="12" cy="12" r="3"/>',
  user:'<circle cx="12" cy="8" r="4"/><path d="M4 21v-2a8 8 0 0116 0v2"/>', search:'<circle cx="10" cy="10" r="7"/><path d="M15 15l6 6"/>',
  refresh:'<path d="M20 7v5h-5M4 17v-5h5M6 7a7 7 0 0112-2l2 2M4 17l2 2a7 7 0 0012-2"/>',
  sun:'<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M2 12h2M20 12h2M5 5l1 1M18 18l1 1M5 19l1-1M18 6l1-1"/>',
  density:'<path d="M3 5h18M3 10h18M3 15h18M3 20h18"/>', logout:'<path d="M9 4H4v16h5M14 8l4 4-4 4M7 12h14"/>',
  menu:'<path d="M3 6h18M3 12h18M3 18h18"/>', download:'<path d="M12 3v12M7 10l5 5 5-5M4 16v5h16v-5"/>',
  arrow:'<path d="M4 12h16M14 6l6 6-6 6"/>', lock:'<rect x="5" y="10" width="14" height="11" rx="2"/><path d="M8 10V6a4 4 0 018 0v4M12 14v3"/>',
  empty:'<path d="M4 7l8-4 8 4v10l-8 4-8-4zM4 7l8 5 8-5M12 12v9"/>', plus:'<path d="M12 4v16M4 12h16"/>', check:'<path d="M5 12l4 4L20 5"/>'
};
export function icon(name) {
  const container = el('span');
  container.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[name] || paths.grid}</svg>`;
  return container.firstElementChild;
}
export function button(label, onClick, kind = '', symbol = null) {
  return el('button', {type:'button', class:`button ${kind}`, on:{click:onClick}}, symbol ? icon(symbol) : null, label);
}
export function number(value, digits = 0) { return value === null || value === undefined ? '—' : new Intl.NumberFormat('en', {maximumFractionDigits:digits}).format(Number(value)); }
export function percent(value, fraction = true) { return value === null || value === undefined ? '—' : `${number(Number(value) * (fraction ? 100 : 1), 1)}%`; }
export function human(value) { return String(value ?? 'Unknown').replaceAll('_', ' ').toLowerCase().replace(/\b\w/g, letter => letter.toUpperCase()); }
export function date(value) {
  if (!value) return '—'; const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toISOString().replace('T', ' ').slice(0,19);
}
export function badge(value) {
  const text = String(value ?? 'UNKNOWN').toUpperCase();
  const tone = /HEALTHY|SUCCEEDED|APPROVED|RESOLVED|ACTIVE|READY|UP|COMPLETED|ON_TIME/.test(text) ? 'good'
    : /CRITICAL|OFFLINE|FAILED|ERROR|DISABLED|BREACHED/.test(text) ? 'bad'
    : /DEGRADED|DELAYED|HIGH|WARNING|WARN|RETRY|STALE|MISSING/.test(text) ? 'warning'
    : /OPEN|PENDING|ACKNOWLEDGED|RUNNING|MEDIUM/.test(text) ? 'info' : '';
  return el('span', {class:`badge ${tone}`}, human(value));
}
export function empty(title, message, symbol = 'empty') {
  return el('div', {class:'empty-state'}, el('div', {class:'empty-symbol'}, icon(symbol)), el('h3', {}, title), el('p', {}, message));
}
export function loading() { return el('div', {class:'loading', role:'status'}, el('span', {class:'spinner'}), 'Loading operational observations…'); }
export function errorView(error, retry) {
  return el('section', {class:'panel error-panel'}, empty(error.status === 403 ? 'This view requires additional access' : 'Unable to load this view', error.message, 'alert'),
    el('div', {class:'panel-body'}, error.correlation ? el('span', {class:'error-reference'}, `Reference ${error.correlation}`) : null, button('Try again', retry, '', 'refresh')));
}
export function banner(message, kind = 'info', trailing = null) {
  return el('div', {class:`banner ${kind}`}, icon(kind === 'warning' ? 'alert' : 'pulse'), message, trailing ? el('span', {class:'trailing'}, trailing) : null);
}
export function heading(title, description, ...actions) {
  return el('div', {class:'page-heading'}, el('div', {}, el('h1', {}, title), el('p', {}, description)), el('div', {class:'heading-actions'}, actions));
}
export function panel(title, subtitle, content, action = null, footer = null) {
  return el('section', {class:'panel'}, el('div', {class:'panel-header'}, el('div', {}, el('h2', {}, title), subtitle ? el('p', {}, subtitle) : null), action),
    el('div', {class:'panel-body'}, content), footer ? el('div', {class:'panel-footer'}, footer) : null);
}
export function metric(label, value, note, symbol = 'pulse', tone = '') {
  return el('div', {class:`metric-card ${tone}`}, el('div', {class:'metric-top'}, label, el('span', {class:'metric-icon'}, icon(symbol))),
    el('div', {class:'metric-value number'}, value), el('div', {class:'metric-note'}, note));
}
export function table(columns, items, caption = 'Operational observations') {
  const node = el('table', {}, el('caption', {}, caption), el('thead', {}, el('tr', {}, columns.map(column => el('th', {scope:'col'}, column.label)))));
  const body = el('tbody');
  for (const item of items) body.append(el('tr', {}, columns.map(column => el('td', {class:column.class || ''}, column.render ? column.render(item) : (item[column.key] ?? '—')))));
  node.append(body); return el('div', {class:'table-scroll'}, node);
}
export function linkCell(text, handler) { return el('button', {class:'table-link', type:'button', on:{click:handler}}, text); }
export function pairs(values) {
  return el('dl', {class:'detail-summary'}, values.map(([label,value]) => el('div', {}, el('dt', {}, label), el('dd', {}, value ?? '—'))));
}
export function jsonView(value) { return el('pre', {class:'json-view'}, JSON.stringify(value, null, 2)); }
export function toast(message, error = false) {
  const node = el('div', {class:`toast ${error ? 'error' : ''}`}, message); document.querySelector('#toast-container').append(node);
  setTimeout(() => node.remove(), 6500);
}
export function download(value, filename, mime = 'application/json') {
  const url = URL.createObjectURL(new Blob([JSON.stringify(value, null, 2)], {type:mime}));
  const anchor = el('a', {href:url, download:filename}); anchor.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
}
export function showDetails(title, content, eyebrow = 'INVESTIGATION') {
  document.querySelector('#detail-title').textContent = title; document.querySelector('#detail-eyebrow').textContent = eyebrow;
  document.querySelector('#detail-content').replaceChildren(content); const dialog = document.querySelector('#detail-dialog'); if (!dialog.open) dialog.showModal();
}
export function actionForm(title, fields, submit, label = 'Save changes') {
  const dialog = document.querySelector('#action-dialog'); document.querySelector('#action-title').textContent = title;
  const form = el('form'); const controls = {};
  for (const field of fields) {
    const id = `action-${field.name}`; let input;
    if (field.type === 'select') input = el('select', {id, name:field.name}, field.options.map(option => el('option', {value:typeof option === 'string' ? option : option.value}, typeof option === 'string' ? human(option) : option.label)));
    else input = el(field.type === 'textarea' ? 'textarea' : 'input', {id, name:field.name, type:field.type || 'text', required:field.required !== false, maxlength:field.maxlength || 2000, min:field.min, max:field.max, autocomplete:field.type === 'password' ? 'new-password' : 'off'});
    if (field.value !== undefined) input.value = field.value;
    controls[field.name] = input; form.append(el('div', {class:'field'}, el('label', {for:id}, field.label), input, field.help ? el('small', {class:'muted'}, field.help) : null));
  }
  const error = el('div', {class:'form-error', role:'alert', hidden:true}); const save = el('button', {type:'submit', class:'button primary'}, label);
  form.append(error, el('div', {class:'form-actions'}, button('Cancel', () => dialog.close()), save));
  form.addEventListener('submit', async event => {
    event.preventDefault(); save.disabled = true; error.hidden = true;
    const values = Object.fromEntries(Object.entries(controls).map(([name,input]) => [name, input.type === 'number' ? Number(input.value) : input.value]));
    try { await submit(values); dialog.close(); form.reset(); } catch (failure) { error.hidden = false; error.textContent = failure.message; } finally { save.disabled = false; }
  });
  document.querySelector('#action-content').replaceChildren(form); if (!dialog.open) dialog.showModal(); return form;
}
export function timeline(items, titleField, timeField, detailField = null) {
  return items.length ? el('div', {class:'timeline'}, items.map(item => el('div', {class:'timeline-item'}, el('strong', {}, human(item[titleField])),
    el('small', {}, date(item[timeField]), ' UTC'), detailField && item[detailField] != null ? el('p', {}, typeof item[detailField] === 'object' ? JSON.stringify(item[detailField]) : item[detailField]) : null))) : empty('No recorded events', 'New observations will appear here after ingestion.');
}
