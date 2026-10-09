/** Native SVG charts describe observed data and provide accessible source tables. */
import {el, table, empty, number, human} from '../components/ui.js?v=0.1.0';
const NS = 'http://www.w3.org/2000/svg';
function svgNode(tag, attributes, ...children) {
  const node = document.createElementNS(NS, tag);
  for (const [key,value] of Object.entries(attributes)) node.setAttribute(key, value);
  for (const child of children) node.append(typeof child === 'string' ? document.createTextNode(child) : child);
  return node;
}
export function trend(series) {
  if (!series?.length) return empty('No hourly observations yet', 'Communication trends require normalized events in the selected time window.', 'pulse');
  const width = 660, height = 230, left = 40, top = 15, bottom = 40;
  const numeric = series.map(row => ({...row, success_count:Number(row.success_count), failure_count:Number(row.failure_count), late_count:Number(row.late_count)}));
  const maximum = Math.max(1, ...numeric.map(row => row.success_count + row.late_count + row.failure_count));
  const chart = svgNode('svg', {viewBox:`0 0 ${width} ${height}`, class:'chart', role:'img', 'aria-label':'Observed hourly successful, failed, and late communication counts'});
  chart.append(svgNode('title', {}, 'Observed communication by hour'));
  const y = value => top + (height-top-bottom) * (1 - value/maximum);
  const x = index => left + index * (width-left-15) / Math.max(1,numeric.length-1);
  for (let step = 0; step < 4; step++) {
    const value = maximum * step / 3, pos = y(value);
    chart.append(svgNode('line', {x1:left,y1:pos,x2:width-15,y2:pos,class:'chart-grid'}), svgNode('text', {x:left-10,y:pos+3,'text-anchor':'end',class:'chart-label'}, number(value)));
  }
  for (const [field,color] of [['success_count','var(--accent)'],['failure_count','var(--red)'],['late_count','var(--blue)']]) {
    const line = numeric.map((row,index) => `${x(index)},${y(row[field])}`).join(' ');
    if (numeric.length > 1) chart.append(svgNode('polyline', {points:line,fill:'none',stroke:color,'stroke-width':2,'stroke-linejoin':'round'}));
    numeric.forEach((row,index) => { const point=svgNode('circle',{cx:x(index),cy:y(row[field]),r:2.5,fill:color}); point.append(svgNode('title',{},`${row.hour_start}: ${human(field)} ${row[field]}`));chart.append(point); });
  }
  for (const index of [...new Set([0, Math.floor((numeric.length-1)/2), numeric.length-1])]) {
    const time = new Date(numeric[index].hour_start);
    chart.append(svgNode('text', {x:x(index),y:height-15,'text-anchor':index===0?'start':index===numeric.length-1?'end':'middle',class:'chart-label'}, Number.isNaN(time.getTime()) ? '' : time.toISOString().slice(11,16)+' UTC'));
  }
  const legend = el('div',{class:'chart-legend'}, [['On-time reads','teal'],['Failures','red'],['Late reads','blue']].map(([label,color])=>el('span',{},el('i',{class:`legend-key ${color}`}),label)));
  const source = el('details',{class:'chart-data'},el('summary',{},'View observed hourly data'),table([{label:'Hour (UTC)',key:'hour_start'},{label:'On-time reads',key:'success_count'},{label:'Failures',key:'failure_count'},{label:'Late reads',key:'late_count'}],numeric));
  return el('div',{},legend,chart,source);
}
export function bars(items, labelKey, valueKey, color = 'var(--accent)', click = null) {
  if (!items?.length) return empty('No observations available', 'This breakdown will populate as matching data is ingested.');
  const maximum = Math.max(1,...items.map(item=>Number(item[valueKey] || 0)));
  return el('div',{class:'bar-list'},items.map(item=>{
    const label = click ? el('button',{type:'button',class:'table-link',on:{click:()=>click(item)}},human(item[labelKey])) : el('strong',{},human(item[labelKey]));
    const bar = svgNode('svg',{viewBox:'0 0 300 7',class:'bar-track','aria-hidden':'true',preserveAspectRatio:'none'},svgNode('rect',{width:300,height:7,rx:3,fill:'var(--surface-alt)'}),svgNode('rect',{width:Math.max(0,Number(item[valueKey]||0)/maximum*300),height:7,rx:3,fill:color}));
    return el('div',{},el('div',{class:'bar-top'},label,el('span',{class:'number'},number(item[valueKey]))),bar);
  }));
}
