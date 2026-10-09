/** Fleet analytics and investigation workflows backed exclusively by the API. */
import {api, can, session, query} from '../api/client.js?v=0.1.0';
import {collectionView} from '../core/collection.js?v=0.1.0';
import {el, icon, button, heading, banner, metric, panel, table, empty, number, percent, human, date, badge, linkCell, pairs, showDetails, loading, errorView, actionForm, toast, timeline, jsonView} from '../components/ui.js?v=0.1.0';
import {trend,bars} from '../charts/svg.js?v=0.1.0';

const meterColumns=[
  {label:'Meter ID',render:r=>linkCell(r.meter_id,()=>meterDetail(r.meter_id))},
  {label:'Status',render:r=>badge(r.current_status)},
  {label:'Model / technology',render:r=>el('div',{},el('span',{class:'cell-primary'},r.model || 'Unknown'),el('span',{class:'cell-secondary'},r.communication_type || 'Unknown'))},
  {label:'Region',key:'region_id'},{label:'Gateway',key:'gateway_id'},
  {label:'Last communication (UTC)',render:r=>el('span',{class:'mono'},date(r.last_seen_at))},
  {label:'Failures',render:r=>number(r.consecutive_failures)},{label:'Missing intervals',render:r=>number(r.missing_interval_count)}
];
const incidentColumns=[
  {label:'Incident',render:r=>el('div',{},linkCell(r.incident_id.slice(0,13),()=>incidentDetail(r.incident_id)),el('span',{class:'cell-secondary'},human(r.incident_type)))},
  {label:'Severity',render:r=>badge(r.severity)},{label:'State',render:r=>badge(r.status)},
  {label:'Suspected cause',render:r=>human(r.suspected_root_cause)},
  {label:'Affected meters',render:r=>number(r.affected_meter_count)},
  {label:'Confidence',render:r=>percent(r.root_cause_confidence)},
  {label:'Gateway',key:'gateway_id'},{label:'Detected (UTC)',render:r=>date(r.detected_at)}
];
function scopeBanner(data) {
  if(data.data_mode==='synthetic')return banner('Synthetic telemetry · These observations are generated test data, with no claim of live fleet performance.','warning',data.updated_at?`Latest ${date(data.updated_at)} UTC`:null);
  if(data.data_mode==='empty')return banner('No fleet observations yet. Connect a source or load an authorized dataset to begin.','info');
  return banner('Observed fleet data', 'info',data.updated_at?`Latest ${date(data.updated_at)} UTC`:'No communication timestamp recorded');
}
function recordFreshness(data) {
  document.querySelector('#freshness').textContent=data.updated_at?`Telemetry observed ${date(data.updated_at)} UTC`:'No communication freshness available';
}
export async function overview() {
  const [data,communication,regions,incidents]=await Promise.all([api('/analytics/overview'),api('/analytics/communication'),api('/analytics/regions'),can('incidents.read')?api('/incidents?limit=5&status=OPEN'):Promise.resolve({items:[]})]);
  recordFreshness(data);const m=data.metrics;
  const summary=el('div',{class:'metrics-grid'},
    metric('Registered meters',number(m.registered_meters),`${number(m.active_meters)} active and eligible`,'meter'),
    metric('Healthy meters',number(m.healthy_meters),'Derived from observed meter state','pulse','accent'),
    metric('Open incidents',number(m.open_incidents),`${number(m.high_risk_incidents)} high or critical severity`,'alert','warning'),
    metric('Mean ingestion delay',m.ingestion_delay_seconds===null?'—':`${number(m.ingestion_delay_seconds,1)}s`,'Observed event time → ingestion time','clock'));
  const breakdown=el('div',{class:'fleet-breakdown'},[['Offline',m.offline_meters],['Degraded / delayed',m.degraded_meters],['Completeness',m.reporting_completeness===null?'Not configured':percent(m.reporting_completeness)],['SLA risk',m.sla_risk===null?'Not configured':m.sla_risk]].map(([label,value])=>el('div',{},el('div',{class:'number'},typeof value==='number'?number(value):value),el('span',{},label))));
  const comm=panel('Communication activity','Observed hourly counts · Last 24 hours',el('div',{},trend(communication.series),breakdown),el('a',{href:'#communication',class:'button small'},'Explore',icon('arrow')));
  const status=panel('Fleet health','Meters with an observed health state',bars(data.status_distribution,'status','count'),el('a',{href:'#meters',class:'button small'},'View meters'));
  const regional=panel('Regional fleet','Registered meters by region',bars(regions.items,'display_name','meter_count','var(--blue)',item=>{location.hash=`meters?region_id=${encodeURIComponent(item.region_id || '')}`;}),el('a',{href:'#geographic',class:'button small'},'View regions'));
  const causes=panel('Incident root causes','Unresolved incidents · Evidence based hypotheses',bars(data.root_causes,'cause','count','var(--amber)'),el('a',{href:'#incidents',class:'button small'},'Investigate'));
  const priorities=el('section',{class:'panel'},el('div',{class:'panel-header'},el('div',{},el('h2',{},'Open incident queue'),el('p',{},'Five recorded open cases · Review severity before acting')),el('a',{href:'#incidents',class:'button small'},'Open workbench',icon('arrow'))),
    el('div',{class:'panel-body'},incidents.items.length?table(incidentColumns.slice(0,6),incidents.items):empty('No open incidents recorded','The queue will populate when correlated failures meet the configured evidence thresholds.','check')));
  return el('div',{},heading('Operations overview','A clear view of fleet health, shared failures, and recovery.'),scopeBanner(data),summary,el('div',{class:'page-grid'},comm,status),el('div',{class:'page-grid equal'},regional,causes),priorities);
}
export async function meters() {
  const initial=Object.fromEntries(new URLSearchParams(location.hash.split('?')[1]||''));
  const direct=button('Find meter',()=>actionForm('Open meter investigation',[{name:'meter_id',label:'Meter identifier',maxlength:128}],async values=>{await api(`/meters/${encodeURIComponent(values.meter_id)}`);await meterDetail(values.meter_id);},'Open meter'),'','search');
  return el('div',{},heading('Meter intelligence','Investigate communication state, historical topology, and recovery evidence.'),await collectionView('/meters',meterColumns,[{name:'q',label:'Meter search',placeholder:'Meter identifier or model'},{name:'status',label:'Health state',options:['HEALTHY','DEGRADED','DELAYED','OFFLINE','UNKNOWN','DECOMMISSIONED']},{name:'region_id',label:'Region',placeholder:'Region identifier'}],{initial,action:direct,emptyTitle:'No meters match this scope',emptyMessage:'Update the filters or ingest a meter registry to begin.'}));
}
export async function meterDetail(id) {
  showDetails(id,loading(),'METER INTELLIGENCE');
  try {
    const data=await api(`/meters/${encodeURIComponent(id)}`),health=data.health || {};
    const events=panel('Communication timeline','Latest recorded events · Up to 100 observations',timeline(data.events||[],'event_type','event_time','error_code'));
    const topology=panel('Topology history','Effective dates preserve historical relationships',(data.topology||[]).length?table([{label:'Gateway',key:'gateway_id'},{label:'DCU',key:'dcu_id'},{label:'Feeder',key:'feeder_id'},{label:'Transformer',key:'transformer_id'},{label:'From (UTC)',render:r=>date(r.valid_from)},{label:'To (UTC)',render:r=>r.valid_to?date(r.valid_to):'Current'}],data.topology):empty('No topology recorded','Gateway, feeder and transformer relationships require authoritative asset records.'));
    const predictions=panel('Recovery predictions','Predictions carry their model version and observation time',(data.predictions||[]).length?table([{label:'Prediction',render:r=>human(r.prediction_type)},{label:'Probability',render:r=>percent(r.prediction_value)},{label:'Model',key:'model_id'},{label:'Predicted (UTC)',render:r=>date(r.predicted_at)}],data.predictions):empty('No stored recovery prediction','An approved active model and eligible feature snapshot are required.'));
    showDetails(id,el('div',{},pairs([['Health state',badge(health.current_status)],['Last communication',date(health.last_seen_at)+' UTC'],['Model / vendor',`${data.model||'Unknown'} / ${data.manufacturer_id||'Unknown'}`],['Firmware',data.firmware_version],['Technology',data.communication_type],['Region',data.region_id],['Gateway',health.gateway_id],['Consecutive failures',number(health.consecutive_failures)],['Missing intervals',number(health.missing_interval_count)],['Delivery state',human(health.delivery_status)],['Communication score',number(health.communication_score,2)],['Expected cadence',data.expected_communication_minutes?`${data.expected_communication_minutes} minutes`:'Unknown']]),events,topology,predictions),'METER INTELLIGENCE');
  }catch(error){showDetails(id,errorView(error,()=>meterDetail(id)));}
}
export async function incidents() {
  return el('div',{},heading('Incidents & root cause analysis','Trace shared failures to evidence, coordinate ownership, and close the loop.'),banner('Root-cause confidence is a hypothesis. Confirm the cause using operator evidence before resolution.'),await collectionView('/incidents',incidentColumns,[{name:'q',label:'Search',placeholder:'Incident, gateway or cause'},{name:'status',label:'Workflow state',options:['OPEN','ACKNOWLEDGED','RESOLVED']},{name:'severity',label:'Severity',options:['CRITICAL','HIGH','MEDIUM','LOW']}],{emptyTitle:'Your incident queue is clear',emptyMessage:'No incidents match your current access scope and filters.'}));
}
const causes=['GATEWAY_FAILURE','DCU_FAILURE','NETWORK_PROVIDER_ISSUE','LOW_SIGNAL_QUALITY','POWER_INTERRUPTION','HES_PROCESSING_DELAY','HES_MDMS_INTEGRATION_DELAY','MDMS_VALIDATION_FAILURE','METER_FIRMWARE_ISSUE','METER_SPECIFIC_FAILURE','TOPOLOGY_MAPPING_ERROR','UNKNOWN'];
export async function incidentDetail(id) {
  showDetails(id,loading(),'INCIDENT WORKBENCH');
  try {
    const data=await api(`/incidents/${encodeURIComponent(id)}`);
    async function action(kind,values){await api(`/incidents/${encodeURIComponent(id)}/${kind}`,{method:'POST',body:values});toast(`Incident ${kind} recorded.`);await incidentDetail(id);document.dispatchEvent(new CustomEvent('data-changed'));}
    const controls=el('div',{class:'detail-actions'});
    if(can('incidents.acknowledge')&&data.status!=='RESOLVED')controls.append(button('Acknowledge',()=>actionForm('Acknowledge incident',[{name:'note',label:'Operator note',type:'textarea',required:false}],values=>action('acknowledge',values)),'','check'));
    if(can('incidents.assign'))controls.append(button('Assign',()=>actionForm('Assign incident',[{name:'assigned_to',label:'Active tenant user ID',type:'number',min:1,value:data.assigned_to||session.user.id},{name:'note',label:'Assignment note',type:'textarea',required:false}],values=>action('assign',values))));
    if(can('incidents.feedback'))controls.append(button('Confirm root cause',()=>actionForm('Confirm root cause',[{name:'confirmed_cause',label:'Confirmed cause',type:'select',options:causes,value:data.suspected_root_cause},{name:'note',label:'Supporting operator evidence',type:'textarea'}],values=>action('feedback',values))));
    if(can('incidents.resolve')&&data.status!=='RESOLVED')controls.append(button('Resolve incident',()=>actionForm('Resolve incident',[{name:'resolution',label:'Resolution explanation',type:'textarea',maxlength:2000}],values=>action('resolve',values),'Resolve incident'),'primary'));
    const evidence=panel('Root-cause evidence','Recorded supporting and contradictory observations',data.evidence.length?el('div',{},data.evidence.map(item=>el('details',{class:'chart-data'},el('summary',{},`${human(item.evidence_type)} · ${item.source_system} · ${date(item.event_time)} UTC`),jsonView(item.evidence_json)))):empty('No evidence attached','A root-cause assessment requires recorded source observations.'));
    const history=panel('Incident timeline','Workflow changes and evidence history',timeline(data.history,'change_type','created_at','after_state'));
    const notes=panel('Operator actions','Notes, assignment and resolution records',data.actions.length?table([{label:'Action',render:r=>human(r.action_type)},{label:'Actor',key:'executed_by'},{label:'Outcome',render:r=>badge(r.action_status)},{label:'Recorded (UTC)',render:r=>date(r.created_at)},{label:'Notes',class:'cell-wrap',render:r=>JSON.stringify(r.result_json)}],data.actions):empty('No operator actions yet','Acknowledgment, assignment, cause confirmation and resolution are audited here.'));
    showDetails(id,el('div',{},pairs([['Severity',badge(data.severity)],['Workflow state',badge(data.status)],['Affected meters',number(data.affected_meter_count)],['Suspected cause',human(data.suspected_root_cause)],['Confidence',percent(data.root_cause_confidence)],['Assigned user',data.assigned_to||'Unassigned'],['Gateway',data.gateway_id],['Region',data.region_id],['Detected',date(data.detected_at)+' UTC']]),controls,evidence,history,notes),'INCIDENT WORKBENCH');
  }catch(error){showDetails(id,errorView(error,()=>incidentDetail(id)));}
}
export async function communication() {
  const data=await api('/analytics/communication'),series=data.series||[];
  const sums=series.reduce((total,row)=>({success:total.success+Number(row.success_count),failure:total.failure+Number(row.failure_count),late:total.late+Number(row.late_count)}),{success:0,failure:0,late:0});
  return el('div',{},heading('Communication analytics','Observed delivery patterns across the last 24 hours.'),el('div',{class:'metrics-grid'},metric('Observed hourly buckets',number(series.length),'Hours with recorded observations','clock'),metric('On-time reads',number(sums.success),'Counts within the selected window','pulse','accent'),metric('Failures',number(sums.failure),'Recorded communication failures','alert','warning'),metric('Late reads',number(sums.late),'Delayed physical receipt retained','quality')),panel('Communication activity','Hourly counts · Missing hours remain unobserved',trend(series)),banner('The trend reflects recorded hourly aggregates. Unobserved time buckets are not treated as successful communication.'));
}
export async function network() {
  const data=await api('/analytics/gateways');
  const columns=[{label:'Gateway',render:r=>linkCell(r.gateway_id,()=>showDetails(r.gateway_id,el('div',{},pairs([['Active meters',number(r.active_meter_count)],['Offline meters',number(r.offline_meter_count)],['Failure ratio',percent(r.failure_rate)],['Network availability',percent(r.network_availability)],['Window start',date(r.window_start)+' UTC'],['Window end',date(r.window_end)+' UTC']]),'GATEWAY INTELLIGENCE')))},
    {label:'Active meters',render:r=>number(r.active_meter_count)},{label:'Offline meters',render:r=>number(r.offline_meter_count)},{label:'Failure rate',render:r=>percent(r.failure_rate)},{label:'Availability',render:r=>percent(r.network_availability)},{label:'Window (UTC)',render:r=>date(r.window_start)}];
  return el('div',{},heading('Network / DCU intelligence','Identify shared gateway impact from observed device and meter evidence.'),banner('Unknown availability is left unmeasured. A meter failure cluster alone does not establish a network outage.'),panel('Gateway observations','Last 24 hours · Recorded correlation windows',data.items.length?table(columns,data.items):empty('No gateway snapshots recorded','Shared-failure correlation creates gateway observations as eligible events arrive.','network')));
}
export async function geographic() {
  const data=await api('/analytics/regions');
  return el('div',{},heading('Geographic intelligence','Explore authoritative regional fleet associations.'),el('div',{class:'page-grid equal'},panel('Fleet distribution','Registered meters by region',bars(data.items,'display_name','meter_count','var(--blue)',r=>location.hash=`meters?region_id=${encodeURIComponent(r.region_id||'')}`)),panel('Offline distribution','Observed offline states by region',bars(data.items,'display_name','offline_count','var(--amber)',r=>location.hash=`meters?status=OFFLINE&region_id=${encodeURIComponent(r.region_id||'')}`))),panel('Region registry','Choose a region to investigate its meters',data.items.length?table([{label:'Region',render:r=>linkCell(r.display_name||'Unmapped',()=>location.hash=`meters?region_id=${encodeURIComponent(r.region_id||'')}`)},{label:'Region ID',key:'region_id'},{label:'Registered',render:r=>number(r.meter_count)},{label:'Offline',render:r=>number(r.offline_count)}],data.items):empty('No region associations','Authorized GIS and asset records supply the regional hierarchy.','map')),banner('Coordinates and base maps are shown only when an authorized geographic source is available. This view uses recorded region associations.'));
}
export async function dataQuality() {
  const data=await api('/analytics/data-quality');
  return el('div',{},heading('Data quality','Keep physical receipt, estimation, delay, and missing data distinct.'),banner('Estimated readings are not equivalent to physically received readings.','warning'),el('div',{class:'page-grid equal'},panel('Interval delivery states','Reconciled intervals · Last 24 hours',bars(data.items,'delivery_status','count')),panel('Delivery observations','Recorded interval outcomes',data.items.length?table([{label:'Delivery state',render:r=>badge(r.delivery_status)},{label:'Intervals',render:r=>number(r.count)}],data.items):empty('No interval observations','HES and MDMS receipts must be reconciled against an approved expected interval schedule.','quality'))));
}
export async function sla() {
  const data=await api('/analytics/sla');
  return el('div',{},heading('SLA analytics','Evaluate delivery obligations against an approved contractual definition.'),data.available===false||data.status==='NOT_CONFIGURED'?banner(data.reason||'No approved SLA contract is configured. No compliance percentage is inferred.','warning'):null,panel('Contract evaluation','Last 24 hours · Approved definitions only',Object.keys(data).length?jsonView(data):empty('No SLA evaluation','Configure and approve a tenant SLA definition before evaluation.','clock')));
}
