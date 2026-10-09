/** Bounded API collection view with server-side filtering and keyset pagination. */
import {api, query} from '../api/client.js?v=0.1.0';
import {el, button, table, empty, loading, errorView} from '../components/ui.js?v=0.1.0';

export async function collectionView(path, columns, fields = [], options = {}) {
  const root = el('section',{class:'panel'}), results = el('div'), footer = el('div');
  const filters = {...options.initial}, cursors = ['']; let next = null, generation = 0;
  const controls = {};
  const form = el('form',{class:'toolbar'});
  for (const field of fields) {
    const id = `filter-${field.name}`;
    const input = field.options ? el('select',{id,name:field.name},el('option',{value:''},field.all || 'All'),field.options.map(value=>el('option',{value},String(value).replaceAll('_',' '))))
      : el('input',{id,name:field.name,placeholder:field.placeholder || '',maxlength:128,type:'search'});
    if (filters[field.name]) input.value = filters[field.name]; controls[field.name] = input;
    form.append(el('div',{},el('label',{for:id},field.label),input));
  }
  if (fields.length) form.append(el('button',{type:'submit',class:'button'},'Apply filters'),button('Reset',()=>{
    Object.keys(filters).forEach(key=>delete filters[key]);Object.values(controls).forEach(input=>input.value='');cursors.splice(0,cursors.length,'');load();
  },'text'));
  form.append(el('span',{class:'toolbar-spacer'}),options.action || null);
  if (fields.length || options.action) root.append(form);
  root.append(results,footer);
  async function load() {
    const version=++generation;results.replaceChildren(loading());footer.replaceChildren();
    try {
      const data=await api(path+query({...filters,limit:options.limit || 25,cursor:cursors.at(-1)}));
      if (version !== generation) return;
      const items=Array.isArray(data)?data:data.items || [];next=data.next_cursor;
      results.replaceChildren(items.length?table(columns,items,options.caption):empty(options.emptyTitle || 'No matching records',options.emptyMessage || 'Adjust the filters, or check back after new observations are recorded.'));
      const previous=button('← Previous',()=>{cursors.pop();load();},'small');previous.disabled=cursors.length===1;
      const more=button('Next →',()=>{cursors.push(next);load();},'small');more.disabled=!next;
      footer.replaceChildren(el('div',{class:'pagination'},`${items.length} records · Page ${cursors.length}`,el('div',{},previous,more)));
      options.loaded?.(data,filters);
    } catch(error) { if(version===generation)results.replaceChildren(errorView(error,load)); }
  }
  form.addEventListener('submit',event=>{event.preventDefault();for(const[name,input]of Object.entries(controls))filters[name]=input.value;cursors.splice(0,cursors.length,'');options.filterChanged?.(filters);load();});
  root.reload=load;root.filters=filters;await load();return root;
}
