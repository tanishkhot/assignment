let currentPage = 1;
let currentAuthType = "basic";
const metadataOptions = { include: new Map(), exclude: new Map() };

function goToPage(n){ if (n<1||n>3) return; if(n>1 && !sessionStorage.getItem('authenticationComplete')) return; document.querySelectorAll('.page').forEach(p=>p.classList.remove('active')); document.querySelectorAll('.nav-buttons').forEach(nv=>nv.style.display='none'); document.getElementById(`page${n}`).classList.add('active'); document.getElementById(`page${n}-nav`) && (document.getElementById(`page${n}-nav`).style.display='flex'); currentPage=n; updateSteps(); if(n===3){ populateMetadataDropdowns(); }}
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
      if(lower.includes('password authentication failed')) msg='Wrong password';
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

function toggleDropdown(id){ const dd=document.getElementById(id); const content=dd.querySelector('.dropdown-content'); document.querySelectorAll('.dropdown-content').forEach(c=>{ if(c!==content) c.classList.remove('show');}); content.classList.toggle('show'); event.stopPropagation(); }

function processMetadataResponse(rows){ const db=new Map(); rows.forEach(it=>{ const c=it.catalog_name||it.TABLE_CATALOG; const s=it.schema_name||it.TABLE_SCHEMA; if(!c||!s) return; if(!db.has(c)) db.set(c,new Set()); db.get(c).add(s);}); return db; }

async function fetchMetadata(){ const payload={ type:'all', authType: currentAuthType, host:document.getElementById('host').value, port:Number(document.getElementById('port').value), username:document.getElementById('username').value, password:document.getElementById('password').value, database:document.getElementById('database').value }; const res=await fetch('/workflows/v1/metadata',{method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)}); if(!res.ok) return new Map(); const data=await res.json(); return processMetadataResponse(data.data||[]); }

function buildDBBlock(type, name, schemas){ const content=document.getElementById(`${type}Metadata`).querySelector('.dropdown-content'); const dbDiv=document.createElement('div'); dbDiv.className='database-item'; const checkbox=document.createElement('input'); checkbox.type='checkbox'; checkbox.className='database-checkbox'; const label=document.createElement('label'); label.textContent=name; const count=document.createElement('span'); count.className='selected-count'; count.textContent=`0/${schemas.size}`; dbDiv.appendChild(checkbox); dbDiv.appendChild(label); dbDiv.appendChild(count); const schemaList=document.createElement('div'); schemaList.className='schema-list'; schemas.forEach(schema=>{ const sd=document.createElement('div'); sd.className='database-item'; const scb=document.createElement('input'); scb.type='checkbox'; const sl=document.createElement('label'); sl.textContent=schema; sd.appendChild(scb); sd.appendChild(sl); schemaList.appendChild(sd); scb.addEventListener('change',e=>{ handleSchemaSelection(type,name,schema,e.target.checked); updateSelectionCount(type,name,schemas.size);});}); content.appendChild(dbDiv); content.appendChild(schemaList); checkbox.addEventListener('change',e=>{ handleDatabaseSelection(type,name,Array.from(schemas),e.target.checked); schemaList.querySelectorAll('input[type="checkbox"]').forEach(cb=>cb.checked=e.target.checked); updateSelectionCount(type,name,schemas.size);}); dbDiv.addEventListener('click',e=>{ if(e.target.type!=='checkbox') schemaList.classList.toggle('show');}); }

async function populateMetadataDropdowns(){ const includeDD=document.getElementById('includeMetadata'); const excludeDD=document.getElementById('excludeMetadata'); includeDD.querySelector('.dropdown-content').innerHTML=''; excludeDD.querySelector('.dropdown-content').innerHTML=''; const dbs=await fetchMetadata(); dbs.forEach((schemas,db)=>{ buildDBBlock('include',db,schemas); buildDBBlock('exclude',db,schemas);}); document.getElementById('page3-nav').style.display='flex'; }

function handleDatabaseSelection(type, db, schemas, isSel){ if(!metadataOptions[type].has(db)) metadataOptions[type].set(db,new Set()); if(isSel){ schemas.forEach(s=>metadataOptions[type].get(db).add(s)); } else { metadataOptions[type].get(db).clear(); } updateDropdownHeader(type); }
function handleSchemaSelection(type, db, schema, isSel){ if(!metadataOptions[type].has(db)) metadataOptions[type].set(db,new Set()); if(isSel) metadataOptions[type].get(db).add(schema); else metadataOptions[type].get(db).delete(schema); updateDropdownHeader(type); }
function updateDropdownHeader(type){ const dd=document.getElementById(`${type}Metadata`); const header=dd.querySelector('.dropdown-header span'); const sel=[]; metadataOptions[type].forEach((schemas,db)=>{ if(schemas.size>0) sel.push(`${db} (${schemas.size} schemas)`); }); if(sel.length===0) header.textContent='Select databases and schemas'; else if(sel.length===1) header.textContent=sel[0]; else { header.textContent=`${sel[0]} +${sel.length-1} more`; header.title=sel.join('\n'); } }

function formatFilters(){ const f=(map)=>{ const out={}; map.forEach((schemas,db)=>{ if(schemas.size>0){ out[`^${db}$`]=Array.from(schemas).map(s=>`^${s}$`); }}); return JSON.stringify(out); }; return { include: f(metadataOptions.include), exclude: f(metadataOptions.exclude) }; }

async function runPreflightChecks(){ const btn=document.getElementById('runPreflightChecks'); btn.disabled=true; btn.textContent='Checking...'; const rc=document.querySelector('.preflight-content'); rc.innerHTML=''; try{ const filters=formatFilters(); const payload={ credentials:{ authType: currentAuthType, host:document.getElementById('host').value, port:Number(document.getElementById('port').value), username:document.getElementById('username').value, password:document.getElementById('password').value, database:document.getElementById('database').value }, metadata:{ "include-filter": filters.include, "exclude-filter": filters.exclude, "temp-table-regex": document.getElementById('temp-table-regex').value, "exclude_views": false, "exclude_empty_tables": false } }; const res=await fetch('/workflows/v1/check',{method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)}); const data=await res.json(); const ok=(s)=>`<div class="check-item"><span>Check</span><div class="check-status ${s.success?'success':'error'}">${s.success?'✔️':'❌'}<span>${s.success? (s.successMessage||'OK') : (s.failureMessage||'Failed')}</span></div></div>`; rc.innerHTML = ok(data.data.databaseSchemaCheck||{})+ ok(data.data.tablesCheck||{})+ ok(data.data.versionCheck||{}); } catch(e){ rc.innerHTML='<div class="check-item"><span>Preflight</span><div class="check-status error">❌<span>Failed</span></div></div>'; } finally{ btn.disabled=false; btn.textContent='Check'; }}

function setupPreflight(){ const b=document.getElementById('runPreflightChecks'); if(b){ b.addEventListener('click', runPreflightChecks);} }

function handleRunWorkflow(){ const runBtn=document.getElementById('runWorkflowButton'); const modal=document.getElementById('successModal'); if(!runBtn) return; runBtn.addEventListener('click', async ()=>{ try{ runBtn.disabled=true; runBtn.textContent='Starting...'; const filters=formatFilters(); const tenant=(window.env&&window.env.TENANT_ID)||'default'; const app=(window.env&&window.env.APP_NAME)||'postgres'; const epoch=Math.floor(Date.now()/1000); const payload={ credentials:{ authType: currentAuthType, host:document.getElementById('host').value, port:Number(document.getElementById('port').value), username:document.getElementById('username').value, password:document.getElementById('password').value, database:document.getElementById('database').value }, connection:{ connection_name: document.getElementById('connectionName').value, connection_qualified_name: `${tenant}/${app}/${epoch}` }, metadata:{ "include-filter": filters.include, "exclude-filter": filters.exclude, "temp-table-regex": document.getElementById('temp-table-regex').value, "exclude_views": false, "exclude_empty_tables": false }, tenant_id: tenant }; const res=await fetch('/workflows/v1/start',{method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)}); if(!res.ok) throw new Error('Failed to start workflow'); runBtn.textContent='Started Successfully'; modal.classList.add('show'); } catch(e){ console.error(e); runBtn.textContent='Failed to Start'; } finally{ setTimeout(()=>{ runBtn.disabled=false; runBtn.textContent='Run'; document.getElementById('successModal').classList.remove('show'); }, 2500);} }); }

document.addEventListener('DOMContentLoaded', ()=>{
  document.querySelectorAll('.step').forEach(step=>{ step.addEventListener('click',()=>{ const n=parseInt(step.dataset.step); if(n<=currentPage) goToPage(n);});});
  sessionStorage.removeItem('authenticationComplete');
  attachPasswordToggle();
  setupPreflight();
  handleRunWorkflow();
});
