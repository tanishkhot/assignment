// Debug helpers
const DEBUG = true;
const dlog = (...args) => { if (DEBUG) console.log('[SourceSense]', ...args); };
console.log('[SourceSense] JS boot');
try { dlog('initialized'); } catch(_){ }

let currentPage = 1;
let currentAuthType = "basic";
const metadataOptions = { include: new Map(), exclude: new Map() };
let lastWorkflowId = sessionStorage.getItem('lastWorkflowId') || null;
let resultsTimer = null;
let resultsPoller = null;
let resultsViewMode = (localStorage.getItem('resultsViewMode') || 'json');

function goToPage(n){
  dlog('goToPage', { n, auth: !!sessionStorage.getItem('authenticationComplete'), lastWorkflowId: sessionStorage.getItem('lastWorkflowId') });
  if (n < 1 || n > 4) { dlog('goToPage blocked: out of range'); return; }
  // Allow jumping to Results (4) even if auth flag is missing
  if (n > 1 && n !== 4 && !sessionStorage.getItem('authenticationComplete')) { dlog('goToPage blocked: auth required'); return; }
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-buttons').forEach(nv => nv.style.display = 'none');
  const page = document.getElementById(`page${n}`);
  if (!page) { dlog('goToPage blocked: page element missing', n); return; }
  page.classList.add('active');
  const nav = document.getElementById(`page${n}-nav`);
  if (nav) { nav.style.display = 'flex'; dlog('nav shown', n); }
  currentPage = n;
  updateSteps();
  if (n === 3) { dlog('populateMetadataDropdowns'); populateMetadataDropdowns(); }
}
function updateSteps(){ document.querySelectorAll('.step').forEach((s,i)=>{ const k=i+1; s.classList.remove('active','completed'); if(k===currentPage) s.classList.add('active'); else if(k<currentPage) s.classList.add('completed');}); }
function nextPage(){ if(currentPage===1){ if(!sessionStorage.getItem('authenticationComplete')){ testConnection().then(ok=>{ if(ok) goToPage(2);}); return;} goToPage(2); return;} if(currentPage===2){ const cn=document.getElementById('connectionName').value.trim(); if(!cn) return; goToPage(3); return;} }
function previousPage(){ if(currentPage>1) goToPage(currentPage-1); }

async function testConnection(){
  const btn=document.querySelector('.test-connection');
  const err=document.getElementById('connectionError');
  try{
    btn.disabled=true; btn.textContent='Testing...'; err.classList.remove('visible');
    const payload={ authType: currentAuthType, host:document.getElementById('host').value, port:Number(document.getElementById('port').value), username:document.getElementById('username').value, password:document.getElementById('password').value, database:document.getElementById('database').value };
    const res=await fetch('/workflows/v1/auth',{method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
    let data={};
    try{ data=await res.json(); }catch(_){}
    if(!res.ok||!data.success){
      // Try to derive a friendlier message from server details
      const details=(data && (data.details||data.error||data.message))||'';
      let msg=data.message||'Connection failed';
      const lower=(details||'').toLowerCase();
      if(lower.includes('password authentication failed')) msg='Wrong password/username';
      else if(lower.includes('role') && lower.includes('does not exist')) msg='User does not exist';
      else if(lower.includes('database') && lower.includes('does not exist')) msg='Database does not exist';
      throw new Error(msg);
    }
    btn.textContent='Connection Successful'; btn.classList.add('success');
    document.getElementById('nextButton').disabled=false;
    sessionStorage.setItem('authenticationComplete','true');
    return true;
  } catch(e){
    err.textContent=e.message||'Failed to connect.'; err.classList.add('visible');
    btn.textContent='Test Connection';
    document.getElementById('nextButton').disabled=true;
    sessionStorage.removeItem('authenticationComplete');
    return false;
  } finally{ btn.disabled=false; }
}

function attachPasswordToggle(){
  const pwd=document.getElementById('password');
  const toggle=document.getElementById('togglePassword');
  if(!pwd||!toggle) return;
  toggle.addEventListener('click',()=>{
    const isText=pwd.type==='text';
    pwd.type=isText?'password':'text';
    toggle.textContent=isText?'Show':'Hide';
  });
}

// Parse a PostgreSQL connection URL and fill fields
function parseConnUrl(){
  const urlInput=document.getElementById('connUrl');
  if(!urlInput) return;
  const val=urlInput.value.trim();
  if(!val) return;
  let u;
  try{ u=new URL(val); }
  catch(e){ try{ u=new URL('postgresql://'+val); }catch(e2){ alert('Invalid URL'); return; } }
  document.getElementById('host').value = u.hostname || '';
  document.getElementById('port').value = u.port || '5432';
  document.getElementById('username').value = decodeURIComponent(u.username||'');
  document.getElementById('password').value = decodeURIComponent(u.password||'');
  const db=(u.pathname||'').replace(/^\//,'');
  if(db) document.getElementById('database').value = decodeURIComponent(db);
  const ssl=u.searchParams.get('sslmode');
  const sslSel=document.getElementById('sslmode');
  const req=document.getElementById('requireSSL');
  if(sslSel && ssl){ sslSel.value=ssl; if(req) req.checked=(ssl==='require'); }
}

// Attach parser
document.addEventListener('DOMContentLoaded', ()=>{
  const btn=document.getElementById('parseUrl');
  if(btn) btn.addEventListener('click', parseConnUrl);
});

function toggleDropdown(id, evt){
  const dd = document.getElementById(id);
  if (!dd) return;
  const content = dd.querySelector('.dropdown-content');
  // Close all others first
  document.querySelectorAll('.metadata-dropdown').forEach(x=>{
    if (x !== dd){
      x.classList.remove('open','dropup');
      const c = x.querySelector('.dropdown-content');
      c && c.classList.remove('show');
    }
  });
  // Toggle this one
  const willShow = !(content.classList.contains('show'));
  if (willShow){
    dd.classList.add('open');
    content.classList.add('show');
    // Placement calculation: if not enough space below, use dropup
    const rect = content.getBoundingClientRect();
    const spaceBelow = window.innerHeight - rect.top;
    const headerRect = dd.querySelector('.dropdown-header').getBoundingClientRect();
    const estimatedHeight = Math.min(300, content.scrollHeight || 300);
    if (spaceBelow < estimatedHeight + 24){
      dd.classList.add('dropup');
    } else {
      dd.classList.remove('dropup');
    }
    // Close on outside click / ESC
    setTimeout(()=>{
      const closeHandler = (e)=>{
        if (!dd.contains(e.target)){
          content.classList.remove('show');
          dd.classList.remove('open','dropup');
          document.removeEventListener('click', closeHandler);
          document.removeEventListener('keydown', escHandler);
        }
      };
      const escHandler = (e)=>{
        if (e.key === 'Escape'){
          content.classList.remove('show');
          dd.classList.remove('open','dropup');
          document.removeEventListener('click', closeHandler);
          document.removeEventListener('keydown', escHandler);
        }
      };
      document.addEventListener('click', closeHandler);
      document.addEventListener('keydown', escHandler);
    }, 0);
  } else {
    content.classList.remove('show');
    dd.classList.remove('open','dropup');
  }
  if (evt && evt.stopPropagation) evt.stopPropagation();
}

function processMetadataResponse(rows){ const db=new Map(); rows.forEach(it=>{ const c=it.catalog_name||it.TABLE_CATALOG; const s=it.schema_name||it.TABLE_SCHEMA; if(!c||!s) return; if(!db.has(c)) db.set(c,new Set()); db.get(c).add(s);}); return db; }

async function fetchMetadata(){ const payload={ type:'all', authType: currentAuthType, host:document.getElementById('host').value, port:Number(document.getElementById('port').value), username:document.getElementById('username').value, password:document.getElementById('password').value, database:document.getElementById('database').value }; const res=await fetch('/workflows/v1/metadata',{method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)}); if(!res.ok) return new Map(); const data=await res.json(); return processMetadataResponse(data.data||[]); }

function buildDBBlock(type, name, schemas){
  const content=document.getElementById(`${type}Metadata`).querySelector('.dropdown-content');
  const dbDiv=document.createElement('div'); dbDiv.className='database-item';
  const checkbox=document.createElement('input'); checkbox.type='checkbox'; checkbox.className='database-checkbox';
  const label=document.createElement('label'); label.textContent=name;
  const count=document.createElement('span'); count.className='selected-count'; count.textContent=`0/${schemas.size}`;
  dbDiv.appendChild(checkbox); dbDiv.appendChild(label); dbDiv.appendChild(count);

  const schemaList=document.createElement('div'); schemaList.className='schema-list';
  const opposite = (type === 'include') ? 'exclude' : 'include';

  // Build schema-level checkboxes with cross-validation
  schemas.forEach(schema=>{
    const sd=document.createElement('div'); sd.className='database-item';
    const scb=document.createElement('input'); scb.type='checkbox';
    const sl=document.createElement('label'); sl.textContent=schema;
    sd.appendChild(scb); sd.appendChild(sl);
    schemaList.appendChild(sd);

    scb.addEventListener('change', e => {
      if (e.target.checked) {
        const oppSet = metadataOptions[opposite].get(name);
        if (oppSet && oppSet.has(schema)) {
          e.target.checked = false;
          alert(`Cannot ${type} a schema already selected in ${opposite}.`);
          return;
        }
      }
      handleSchemaSelection(type, name, schema, e.target.checked);
      updateSelectionCount(type, name, schemas.size);
      // Disable/enable the opposite checkbox to reflect state
      syncOppositeSchemaCheckbox(opposite, name, schema, e.target.checked);
    });
  });

  content.appendChild(dbDiv);
  content.appendChild(schemaList);

  // Database-level checkbox selection with cross-validation
  checkbox.addEventListener('change', e => {
    if (e.target.checked) {
      const allowed = [];
      const oppMap = metadataOptions[opposite];
      schemaList.querySelectorAll('div.database-item').forEach(item => {
        const sl = item.querySelector('label');
        const cb = item.querySelector('input[type="checkbox"]');
        const sname = sl ? sl.textContent : '';
        const conflict = oppMap.has(name) && oppMap.get(name).has(sname);
        if (!conflict) { cb.checked = true; allowed.push(sname); }
        else { cb.checked = false; }
      });
      handleDatabaseSelection(type, name, allowed, true);
      updateSelectionCount(type, name, schemas.size);
      // Reflect in opposite dropdown: disable allowed schemas there
      allowed.forEach(s => syncOppositeSchemaCheckbox(opposite, name, s, true));
    } else {
      schemaList.querySelectorAll('input[type="checkbox"]').forEach(cb=>cb.checked=false);
      handleDatabaseSelection(type, name, Array.from(schemas), false);
      updateSelectionCount(type, name, schemas.size);
      // Re-enable opposite checkboxes
      Array.from(schemas).forEach(s => syncOppositeSchemaCheckbox(opposite, name, s, false));
    }
  });

  dbDiv.addEventListener('click', e => { if (e.target.type !== 'checkbox') schemaList.classList.toggle('show'); });
}

// Locate schema checkbox in the opposite dropdown and toggle disabled/title
function syncOppositeSchemaCheckbox(opposite, dbName, schemaName, disable){
  const dd = document.getElementById(`${opposite}Metadata`);
  if (!dd) return;
  const content = dd.querySelector('.dropdown-content');
  if (!content) return;
  let dbNode = null;
  // Find the DB header node with database-checkbox
  const items = Array.from(content.children || []);
  for (let i=0; i<items.length; i++){
    const node = items[i];
    const dbCb = node.querySelector && node.querySelector('input.database-checkbox');
    const dbLabel = node.querySelector && node.querySelector('label');
    if (dbCb && dbLabel && dbLabel.textContent === dbName) {
      dbNode = node;
      break;
    }
  }
  if (!dbNode) return;
  const schemaList = dbNode.nextElementSibling;
  if (!schemaList) return;
  const schemaItems = Array.from(schemaList.children || []);
  for (const it of schemaItems){
    const label = it.querySelector && it.querySelector('label');
    const cb = it.querySelector && it.querySelector('input[type="checkbox"]');
    if (label && cb && label.textContent === schemaName){
      cb.disabled = !!disable;
      cb.title = disable ? `Selected in ${opposite === 'include' ? 'exclude' : 'include'} section` : '';
      break;
    }
  }
}

async function populateMetadataDropdowns(){
  const includeDD = document.getElementById('includeMetadata');
  const excludeDD = document.getElementById('excludeMetadata');
  if (!includeDD || !excludeDD) return;

  const incContent = includeDD.querySelector('.dropdown-content');
  const excContent = excludeDD.querySelector('.dropdown-content');

  // show loading spinners while fetching
  const spinnerHtml = '<div class="spinner">Loading metadata…</div>';
  incContent.innerHTML = spinnerHtml;
  excContent.innerHTML = spinnerHtml;

  try {
    const dbs = await fetchMetadata();

    // replace spinners with actual checklists
    incContent.innerHTML = '';
    excContent.innerHTML = '';
    dbs.forEach((schemas, db) => {
      buildDBBlock('include', db, schemas);
      buildDBBlock('exclude', db, schemas);
    });

    document.getElementById('page3-nav').style.display = 'flex';
  } catch (e) {
    console.error('Failed to fetch metadata', e);
    incContent.innerHTML = '<div class="spinner">Failed to load metadata</div>';
    excContent.innerHTML = '<div class="spinner">Failed to load metadata</div>';
  }
}

function handleDatabaseSelection(type, db, schemas, isSel){ if(!metadataOptions[type].has(db)) metadataOptions[type].set(db,new Set()); if(isSel){ schemas.forEach(s=>metadataOptions[type].get(db).add(s)); } else { metadataOptions[type].get(db).clear(); } updateDropdownHeader(type); }
function handleSchemaSelection(type, db, schema, isSel){ if(!metadataOptions[type].has(db)) metadataOptions[type].set(db,new Set()); if(isSel) metadataOptions[type].get(db).add(schema); else metadataOptions[type].get(db).delete(schema); updateDropdownHeader(type); }
function updateDropdownHeader(type){ const dd=document.getElementById(`${type}Metadata`); const header=dd.querySelector('.dropdown-header span'); const sel=[]; metadataOptions[type].forEach((schemas,db)=>{ if(schemas.size>0) sel.push(`${db} (${schemas.size} schemas)`); }); if(sel.length===0) header.textContent='Select databases and schemas'; else if(sel.length===1) header.textContent=sel[0]; else { header.textContent=`${sel[0]} +${sel.length-1} more`; header.title=sel.join('\n'); } }

function formatFilters(){ const f=(map)=>{ const out={}; map.forEach((schemas,db)=>{ if(schemas.size>0){ out[`^${db}$`]=Array.from(schemas).map(s=>`^${s}$`); }}); return JSON.stringify(out); }; return { include: f(metadataOptions.include), exclude: f(metadataOptions.exclude) }; }

async function runPreflightChecks(){ const btn=document.getElementById('runPreflightChecks'); btn.disabled=true; btn.textContent='Checking...'; const rc=document.querySelector('.preflight-content'); rc.innerHTML=''; try{ const filters=formatFilters(); const payload={ credentials:{ authType: currentAuthType, host:document.getElementById('host').value, port:Number(document.getElementById('port').value), username:document.getElementById('username').value, password:document.getElementById('password').value, database:document.getElementById('database').value }, metadata:{ "include-filter": filters.include, "exclude-filter": filters.exclude, "temp-table-regex": document.getElementById('temp-table-regex').value, "exclude_views": false, "exclude_empty_tables": false } }; const res=await fetch('/workflows/v1/check',{method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)}); const data=await res.json(); const ok=(s)=>`<div class="check-item"><span>Check</span><div class="check-status ${s.success?'success':'error'}">${s.success?'✔️':'❌'}<span>${s.success? (s.successMessage||'OK') : (s.failureMessage||'Failed')}</span></div></div>`; rc.innerHTML = ok(data.data.databaseSchemaCheck||{})+ ok(data.data.tablesCheck||{})+ ok(data.data.versionCheck||{}); } catch(e){ rc.innerHTML='<div class="check-item"><span>Preflight</span><div class="check-status error">❌<span>Failed</span></div></div>'; } finally{ btn.disabled=false; btn.textContent='Check'; }}

function setupPreflight(){ const b=document.getElementById('runPreflightChecks'); if(b){ b.addEventListener('click', runPreflightChecks);} }

function handleRunWorkflow(){
  dlog('handleRunWorkflow attach');
  const runBtn = document.getElementById('runWorkflowButton');
  const modal = document.getElementById('successModal');
  const inlineBtn = document.getElementById('goToResultsInline');
  if(!runBtn){ dlog('runWorkflowButton not found'); return; }
  runBtn.addEventListener('click', async ()=>{
    dlog('Run clicked');
    try{
      runBtn.disabled=true; runBtn.textContent='Starting...';
      const filters=formatFilters();
      const tenant=(window.env&&window.env.TENANT_ID)||'default';
      const app=(window.env&&window.env.APP_NAME)||'postgres';
      const epoch=Math.floor(Date.now()/1000);
      const payload={ credentials:{ authType: currentAuthType, host:document.getElementById('host').value, port:Number(document.getElementById('port').value), username:document.getElementById('username').value, password:document.getElementById('password').value, database:document.getElementById('database').value }, connection:{ connection_name: document.getElementById('connectionName').value, connection_qualified_name: `${tenant}/${app}/${epoch}` }, metadata:{ "include-filter": filters.include, "exclude-filter": filters.exclude, "temp-table-regex": document.getElementById('temp-table-regex').value, "exclude_views": false, "exclude_empty_tables": false }, tenant_id: tenant };
      const res=await fetch('/workflows/v1/start',{method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
      dlog('start response status', res.status);
      let data={}; try{ data=await res.json(); }catch(_){}
      if(!res.ok) { dlog('start failed body', data); throw new Error((data && (data.error||data.message)) || 'Failed to start workflow'); }
      const wfId = data.workflow_id || data.id || data.workflowId || (data.data && (data.data.workflow_id||data.data.id)) || `${tenant}-${epoch}`;
      lastWorkflowId = wfId; sessionStorage.setItem('lastWorkflowId', wfId); dlog('workflow id set', wfId);
      runBtn.textContent='Started Successfully';
      modal.classList.add('show');
      const viewBtn = document.getElementById('viewResultsBtn');
      if(viewBtn){ viewBtn.onclick = (e)=>{ e.preventDefault(); dlog('modal View Results clicked'); modal.classList.remove('show'); startResultsFlow(); }; } else { dlog('viewResultsBtn not found'); }
      if(inlineBtn){ inlineBtn.disabled = false; inlineBtn.onclick = (e)=>{ e.preventDefault(); dlog('inline Go to Results clicked'); startResultsFlow(); }; } else { dlog('inline Go to Results not found'); }
    } catch(e){ console.error(e); dlog('Run start error', e); runBtn.textContent='Failed to Start'; }
    finally{ runBtn.disabled=false; runBtn.textContent='Run'; }
  });
}

async function startResultsFlow(){
  // Ensure guards pass even if auth flag was cleared
  dlog('startResultsFlow begin', { lastWorkflowId });
  try { sessionStorage.setItem('authenticationComplete','true'); } catch(_) {}
  goToPage(4);
  const loader=document.getElementById('resultsLoader');
  const pre=document.getElementById('resultsContent');
  const err=document.getElementById('resultsError');
  const actions=document.getElementById('resultsActions');
  const openRaw=document.getElementById('openRawFile');
  const openDash=document.getElementById('openDashboard');
  const reload=document.getElementById('reloadResults');
  // Try to discover a workflow id if we don't have one yet
  if(!lastWorkflowId){
    try{
      const resp = await fetch('/workflows/v1/latest-output', { cache: 'no-store' });
      dlog('latest-output status', resp.status);
      if (resp.ok){ const js = await resp.json(); if (js && js.workflow_id){ lastWorkflowId = js.workflow_id; sessionStorage.setItem('lastWorkflowId', lastWorkflowId); dlog('discovered workflow id', lastWorkflowId); } }
    } catch(_){}
  }
  if(openRaw && lastWorkflowId){ openRaw.href = `/workflows/v1/result/${lastWorkflowId}`; dlog('openRaw set', openRaw.href); }
  // Set dashboard link
  try{
    const host = (window.env && window.env.TEMPORAL_UI_HOST) || location.hostname;
    const port = (window.env && window.env.TEMPORAL_UI_PORT) || '8233';
    const tenant = (window.env && window.env.TENANT_ID) || 'default';
    const dashUrl = `http://${host}:${port}/namespaces/${tenant}/workflows`;
    if (openDash) { openDash.href = dashUrl; dlog('dashboard href', dashUrl); }
  } catch(_){ }
  // Show workflow id on UI if available
  try{
    const meta = document.getElementById('resultsMeta');
    const wfid = document.getElementById('workflowIdText');
    if (meta && wfid){
      if (lastWorkflowId){ wfid.textContent = lastWorkflowId; meta.style.display = 'block'; }
      else { meta.style.display = 'none'; }
    }
  } catch(_){ }
  if(reload){ reload.onclick = ()=>{ loadResultsAfterDelay(0); }; }
  // reset state
  err.style.display='none'; actions.style.display='none'; pre.style.display='none'; loader.style.display='flex';
  loadResultsAfterDelay(20);
}

function loadResultsAfterDelay(seconds){
  const loader=document.getElementById('resultsLoader');
  const pre=document.getElementById('resultsContent');
  const err=document.getElementById('resultsError');
  const actions=document.getElementById('resultsActions');
  const countdownEl=document.getElementById('resultsCountdown');
  // Reset UI to loading state on every retry
  try{
    if (loader) loader.style.display='flex';
    if (pre) pre.style.display='none';
    if (err){ err.classList.remove('visible'); err.style.display='none'; }
    if (actions) actions.style.display='none';
  } catch(_){}
  if(resultsTimer){ clearInterval(resultsTimer); resultsTimer=null; }
  if(resultsPoller){ clearInterval(resultsPoller); resultsPoller=null; }
  let remaining = Number(seconds)||0;
  if(remaining>0){
    countdownEl.textContent = remaining;
    resultsTimer = setInterval(()=>{
      remaining-=1;
      countdownEl.textContent = Math.max(0, remaining);
      if(remaining<=0){ clearInterval(resultsTimer); resultsTimer=null; fetchResults(); }
    }, 1000);
  } else { fetchResults(); }
  async function fetchResults(){
    try{
      if (!lastWorkflowId){
        // Try discover again
        try{
          const resp = await fetch('/workflows/v1/latest-output', { cache: 'no-store' });
          dlog('latest-output (retry) status', resp.status);
          if (resp.ok){ const js = await resp.json(); if (js && js.workflow_id){ lastWorkflowId = js.workflow_id; sessionStorage.setItem('lastWorkflowId', lastWorkflowId); dlog('discovered (retry) workflow id', lastWorkflowId);
              try{ const meta=document.getElementById('resultsMeta'); const wfid=document.getElementById('workflowIdText'); if(meta&&wfid){ wfid.textContent=lastWorkflowId; meta.style.display='block'; } }catch(_){ }
          try{ const openRaw=document.getElementById('openRawFile'); if(openRaw){ const isJson = (resultsViewMode === 'json'); openRaw.href = isJson ? `/workflows/v1/result-json/${lastWorkflowId}` : `/workflows/v1/result/${lastWorkflowId}`; openRaw.textContent = isJson ? 'Open JSON' : 'Open Text'; } }catch(_){ }
            } }
        } catch(_){ }
      }
      if (!lastWorkflowId) throw new Error('Workflow id unavailable');
      const isJson = (resultsViewMode === 'json');
      const path = isJson ? `/output/${lastWorkflowId}/output.json` : `/output/${lastWorkflowId}/output.txt`;
      let res = await fetch(path, { cache: 'no-store' });
      dlog('fetch results', { mode: resultsViewMode, path, status: res.status });
  if(!res.ok){
        // Fallback to server endpoint if /output isn't mounted
        const alt = isJson ? `/workflows/v1/result-json/${lastWorkflowId}` : `/workflows/v1/result/${lastWorkflowId}`;
        try {
          const altRes = await fetch(alt, { cache: 'no-store' });
          dlog('fetch alt results', { mode: resultsViewMode, alt, status: altRes.status });
          if (altRes.ok) { res = altRes; }
        } catch(_) { /* ignore */ }
      }
      if(!res.ok){
        // one more attempt: if 404, maybe a newer run finished
        if (res.status === 404){
          try{
            const latest = await fetch('/workflows/v1/latest-output', { cache: 'no-store' });
            dlog('latest-output (post-404) status', latest.status);
            if (latest.ok){ const js = await latest.json(); if (js && js.workflow_id && js.workflow_id !== lastWorkflowId){ lastWorkflowId = js.workflow_id; sessionStorage.setItem('lastWorkflowId', lastWorkflowId); dlog('switching to newer workflow id', lastWorkflowId);
                try{ const meta=document.getElementById('resultsMeta'); const wfid=document.getElementById('workflowIdText'); if(meta&&wfid){ wfid.textContent=lastWorkflowId; meta.style.display='block'; } }catch(_){ }
                try{ const openRaw=document.getElementById('openRawFile'); if(openRaw){ openRaw.href = `/workflows/v1/result/${lastWorkflowId}`; } }catch(_){ }
                return fetchResults(); }
            }
          } catch(_){ }
        }
        throw new Error(`Could not fetch results (${res.status})`);
      }
      let rendered = '';
      if (isJson){
        try { rendered = JSON.stringify(await res.json(), null, 2); }
        catch(_) { rendered = await res.text(); }
      } else {
        rendered = await res.text();
      }
      pre.textContent = rendered || '(empty file)'; pre.style.display='block'; loader.style.display='none'; actions.style.display='flex'; err.style.display='none';
      try{ const openRaw=document.getElementById('openRawFile'); if(openRaw){ openRaw.href = isJson ? `/workflows/v1/result-json/${lastWorkflowId}` : `/workflows/v1/result/${lastWorkflowId}`; openRaw.textContent = isJson ? 'Open JSON' : 'Open Text'; } }catch(_){ }
      try{ /* removed excel link */ }catch(_){ }
      // Try to fetch and render summary (show placeholder if missing)
      try {
        const sumRes = await fetch(`/workflows/v1/summary/${lastWorkflowId}`, { cache: 'no-store' });
        dlog('fetch summary', { status: sumRes.status });
        if (sumRes.ok){
          const summary = await sumRes.json();
          renderSummary(summary);
          try {
            const openSummary = document.getElementById('openSummary');
            if (openSummary) openSummary.href = `/output/${lastWorkflowId}/summary.json`;
            /* removed excel link */
          } catch(_){ }
        } else {
          renderSummary({ types: {} });
        }
      } catch(_){ renderSummary({ types: {} }); }
      dlog('results loaded');
      if (resultsPoller){ clearInterval(resultsPoller); resultsPoller = null; }
    } catch(e){
      loader.style.display='none'; pre.style.display='none'; actions.style.display='flex';
      err.textContent = 'Results not ready yet. You can try again in a few seconds.'; err.classList.add('visible'); err.style.display='block';
      dlog('results fetch error', e);
      if (!resultsPoller){ resultsPoller = setInterval(()=>{ fetchResults(); }, 5000); }
    }
  }
}

document.addEventListener('DOMContentLoaded', ()=>{
  dlog('DOMContentLoaded');
  // Make step 4 explicitly launch the results flow so the loader is shown even if guards would block a plain nav
  document.querySelectorAll('.step').forEach(step=>{
    step.addEventListener('click',()=>{
      const n=parseInt(step.dataset.step);
      if (n === 4){
        const modal = document.getElementById('successModal');
        if (modal) { modal.classList.remove('show'); dlog('Sidebar Results clicked'); }
        startResultsFlow();
        return;
      }
      if(n<=currentPage) { dlog('Sidebar step clicked', n); goToPage(n); }
    });
  });
  sessionStorage.removeItem('authenticationComplete');
  attachPasswordToggle();
  setupPreflight();
  handleRunWorkflow();
  // Inline results button state
  const inlineBtn = document.getElementById('goToResultsInline');
  if (inlineBtn){
    dlog('inline Go to Results present');
    inlineBtn.onclick = async (e)=>{
      e.preventDefault();
      const modal = document.getElementById('successModal');
      if (modal) { modal.classList.remove('show'); dlog('Inline Results clicked'); }
      await startResultsFlow();
    };
  } else { dlog('inline Go to Results NOT found'); }
  if (lastWorkflowId){ const s4=document.querySelector('.step[data-step="4"]'); s4 && s4.classList.add('completed'); }
  // Make dropdown headers keyboard accessible
  document.querySelectorAll('.metadata-dropdown .dropdown-header').forEach(h=>{
    h.setAttribute('tabindex','0');
    h.addEventListener('keydown', (e)=>{
      if (e.key === 'Enter' || e.key === ' '){
        const parent = h.closest('.metadata-dropdown');
        if (parent) toggleDropdown(parent.id, e);
        e.preventDefault();
      }
    });
  });
  // Global error hooks
  window.addEventListener('error', (ev)=>{ dlog('window error', ev.message || ev.error); });
  window.addEventListener('unhandledrejection', (ev)=>{ dlog('unhandledrejection', ev.reason); });
  // Results view toggle
  const jsonBtn = document.getElementById('viewJsonBtn');
  const textBtn = document.getElementById('viewTextBtn');
  function updateToggleUI(){
    if (!jsonBtn || !textBtn) return;
    const isJson = (resultsViewMode === 'json');
    jsonBtn.setAttribute('aria-selected', isJson ? 'true' : 'false');
    textBtn.setAttribute('aria-selected', isJson ? 'false' : 'true');
    jsonBtn.className = isJson ? 'btn' : 'btn btn-secondary';
    textBtn.className = isJson ? 'btn btn-secondary' : 'btn';
    try{
      const openRaw=document.getElementById('openRawFile');
      if(openRaw){ openRaw.textContent = isJson ? 'Open JSON' : 'Open Text'; }
    }catch(_){ }
  }
  updateToggleUI();
  if (jsonBtn) jsonBtn.addEventListener('click', ()=>{ resultsViewMode='json'; localStorage.setItem('resultsViewMode','json'); updateToggleUI(); loadResultsAfterDelay(0); });
  if (textBtn) textBtn.addEventListener('click', ()=>{ resultsViewMode='text'; localStorage.setItem('resultsViewMode','text'); updateToggleUI(); loadResultsAfterDelay(0); });
});

// Helper: update database-level selection counter in dropdowns
function updateSelectionCount(type, dbName, totalSchemas){
  try {
    const selected = (metadataOptions[type].get(dbName) || new Set()).size;
    const dd = document.getElementById(`${type}Metadata`);
    if (!dd) return;
    const content = dd.querySelector('.dropdown-content');
    if (!content) return;
    const items = Array.from(content.children || []);
    for (let i = 0; i < items.length; i++){
      const node = items[i];
      const dbCb = node.querySelector && node.querySelector('input.database-checkbox');
      const dbLabel = node.querySelector && node.querySelector('label');
      if (dbCb && dbLabel && dbLabel.textContent === dbName){
        const countEl = node.querySelector('.selected-count');
        if (countEl) countEl.textContent = `${selected}/${totalSchemas}`;
        break;
      }
    }
  } catch (_) { /* no-op */ }
}

// Render summary JSON into the results panel
function renderSummary(summary){
  try {
    const wrap = document.getElementById('resultsSummary');
    const body = document.getElementById('resultsSummaryBody');
    if (!wrap || !body) return;
    const types = (summary && summary.types) || {};
    const lines = [];
    const order = [
      'database','schema','table','column',
      'index','quality_metric',
      'view_dependency','relationship'
    ];
    order.forEach(k=>{
      const s = types[k];
      if (s) lines.push(`${k}: ${s.total_record_count || 0} rows (${s.chunk_count || 0} chunks)`);
    });
    if (lines.length === 0){ body.textContent = 'No summary available.'; }
    else { body.innerHTML = `<pre class="results-pre" style="margin:0; max-height:240px;">${lines.join('\n')}</pre>`; }
    wrap.style.display = 'block';
  } catch(_){ }
}
