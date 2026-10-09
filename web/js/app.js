/** GridSentinel shell and permission-aware routing. No authentication storage. */
import {api,can,session,rememberSession} from './api/client.js?v=0.1.0';
import {el,icon,empty,loading,errorView,toast} from './components/ui.js?v=0.1.0';
import * as operations from './pages/operations.js?v=0.1.0';
import * as workspace from './pages/workspace.js?v=0.1.0';

const routes=[
  {id:'overview',label:'Overview',icon:'grid',permission:'analytics.read',render:operations.overview},
  {id:'meters',label:'Meter Intelligence',icon:'meter',permission:'meters.read',render:operations.meters},
  {id:'communication',label:'Communication Analytics',icon:'pulse',permission:'analytics.read',render:operations.communication},
  {id:'network',label:'Network / DCU Intelligence',icon:'network',permission:'analytics.read',render:operations.network},
  {id:'geographic',label:'Geographic Intelligence',icon:'map',permission:'analytics.read',render:operations.geographic},
  {id:'incidents',label:'Incidents & RCA',icon:'alert',permission:'incidents.read',render:operations.incidents},
  {id:'quality',label:'Data Quality',icon:'quality',permission:'analytics.read',render:operations.dataQuality},
  {id:'sla',label:'SLA Analytics',icon:'clock',permission:'analytics.read',render:operations.sla},
  {id:'models',label:'ML Analytics',icon:'model',permission:'models.read',render:workspace.models},
  {id:'reports',label:'Reports',icon:'report',permission:'reports.read',render:workspace.reports},
  {id:'developer',label:'Developer Console',icon:'code',permissions:['system.logs.read','jobs.read','system.health.read','connectors.read'],render:workspace.developer},
  {id:'administration',label:'Administration',icon:'settings',permissions:['users.read','roles.read','audit.read','config.read','connectors.read'],render:workspace.administration},
  {id:'profile',label:'My Profile',icon:'user',render:workspace.profile}
];
let generation=0;
const allowed=route=>route.permission?can(route.permission):route.permissions?route.permissions.some(can):true;
const login=document.querySelector('#login'),shell=document.querySelector('#workspace'),page=document.querySelector('#page');

function preference(name,fallback){try{return localStorage.getItem(name)||fallback;}catch{return fallback;}}
function savePreference(name,value){try{localStorage.setItem(name,value);}catch{}}
document.documentElement.dataset.theme=preference('gs-theme','dark');
document.body.classList.toggle('compact',preference('gs-density','comfortable')==='compact');
for(const[id,name]of [['sign-out','logout'],['menu-toggle','menu'],['theme-toggle','sun'],['density-toggle','density'],['refresh-page','refresh']])document.querySelector('#'+id).replaceChildren(icon(name));
document.querySelector('#theme-toggle').addEventListener('click',()=>{const theme=document.documentElement.dataset.theme==='dark'?'light':'dark';document.documentElement.dataset.theme=theme;savePreference('gs-theme',theme);});
document.querySelector('#density-toggle').addEventListener('click',()=>{document.body.classList.toggle('compact');savePreference('gs-density',document.body.classList.contains('compact')?'compact':'comfortable');});
document.querySelector('#menu-toggle').addEventListener('click',()=>{const open=document.querySelector('#sidebar').classList.toggle('open');document.querySelector('#menu-toggle').setAttribute('aria-expanded',open);});
document.querySelector('#refresh-page').addEventListener('click',render);
document.querySelector('#sign-out').addEventListener('click',async()=>{try{await api('/auth/logout',{method:'POST'});showLogin();}catch(error){toast(error.message,true);}});
document.querySelector('#toggle-password').addEventListener('click',event=>{const input=document.querySelector('#password'),shown=input.type==='password';input.type=shown?'text':'password';event.currentTarget.textContent=shown?'Hide':'Show';event.currentTarget.setAttribute('aria-label',shown?'Hide password':'Show password');});
for(const[name,close]of [['detail','detail-close'],['action','action-close']]){
  const dialog=document.querySelector('#'+name+'-dialog');document.querySelector('#'+close).addEventListener('click',()=>dialog.close());
  dialog.addEventListener('close',()=>document.querySelector('#'+name+'-content').replaceChildren());
}
function showLogin(){generation++;session.user=null;session.csrf=null;shell.hidden=true;login.hidden=false;page.replaceChildren();document.querySelector('#password').value='';for(const dialog of document.querySelectorAll('dialog'))if(dialog.open)dialog.close();}
function showWorkspace(){login.hidden=true;shell.hidden=false;document.querySelector('#login-form').reset();document.querySelector('#tenant-label').textContent=session.user.tenant_id;document.querySelector('#scope-label').textContent=`${session.user.tenant_id} workspace`;document.querySelector('#user-name').textContent=session.user.username;document.querySelector('#user-role').textContent=session.user.roles.map(role=>role.replaceAll('_',' ')).join(', ');const initials=session.user.username.slice(0,2).toUpperCase();document.querySelector('#user-avatar').textContent=initials;document.querySelector('#topbar-avatar').textContent=initials;
  const nav=document.querySelector('#navigation');nav.replaceChildren(el('div',{class:'nav-heading'},'INTELLIGENCE'));
  routes.forEach((route,index)=>{if(index===9)nav.append(el('div',{class:'nav-heading workspace'},'WORKSPACE'));const link=el('a',{href:'#'+route.id,class:'nav-link','data-route':route.id},icon(route.icon),route.label,!allowed(route)?el('span',{class:'locked','aria-label':'Additional permission required'},'○'):null);link.addEventListener('click',()=>document.querySelector('#sidebar').classList.remove('open'));nav.append(link);});
  if(!location.hash||location.hash==='#'){const first=routes.find(allowed);location.hash=first?.id||'profile';}else render();
}
async function render(){if(!session.user)return;const id=location.hash.slice(1).split('?')[0]||'overview',route=routes.find(item=>item.id===id);const version=++generation;
  document.querySelectorAll('.nav-link').forEach(node=>{const active=node.dataset.route===id;node.classList.toggle('active',active);if(active)node.setAttribute('aria-current','page');else node.removeAttribute('aria-current');});
  document.querySelector('#breadcrumb-current').textContent=route?.label||'View not found';document.title=`${route?.label||'Workspace'} · GridSentinel`;page.replaceChildren(loading());
  if(!route){page.replaceChildren(empty('View not found','Choose an operational workspace from the navigation.'));return;}
  if(!allowed(route)){page.replaceChildren(empty('Additional access required',`Your current role does not include ${route.label.toLowerCase()}. Contact your platform administrator if this workspace is required.`,'lock'));return;}
  try{const content=await route.render();if(version===generation&&session.user)page.replaceChildren(content);}catch(error){if(version===generation&&session.user)page.replaceChildren(errorView(error,render));}
}
document.querySelector('#login-form').addEventListener('submit',async event=>{event.preventDefault();const form=event.currentTarget,submit=form.querySelector('[type=submit]'),error=document.querySelector('#login-error');submit.disabled=true;error.hidden=true;
  try{const data=await api('/auth/login',{method:'POST',body:Object.fromEntries(new FormData(form))});rememberSession(data);const current=await api('/auth/me');rememberSession(current);showWorkspace();}
  catch(failure){error.hidden=false;error.textContent=failure.message;document.querySelector('#password').value='';}
  finally{submit.disabled=false;}
});
window.addEventListener('hashchange',render);
document.addEventListener('session-expired',showLogin);
document.addEventListener('data-changed',render);
try{rememberSession(await api('/auth/me'));showWorkspace();}catch(error){if(error.status!==401){const node=document.querySelector('#login-error');node.hidden=false;node.textContent=error.message;}}
