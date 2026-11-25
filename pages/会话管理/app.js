const $ = id => document.getElementById(id);
// Same defaults as SETTINGS in main.py; used until the saved settings arrive.
const DEFAULT_SETTINGS = {auto_images:false,avatars:false,send_key:'ctrl_enter',reply_mode:'confirm',message_actions:'always',image_save:'manual',image_quota:1024};
const state = {chat:null,trace:null,lastTrace:null,messages:[],logs:[],image:'',busy:false,generation:0,request:null,reference:null,reviewIds:[],expanded:new Set(),failedImages:new Set(),failedAvatars:new Set(),copies:new Map(),copyLoads:new Map(),copyAsked:new Set(),deadUrls:new Set(),imageTools:new Map(),saveSeen:new Set(),saveQueue:[],settings:{...DEFAULT_SETTINGS},settingsLoaded:false};
const kinds = {group:'群聊',private:'好友',temporary:'临时'};
const statuses = {recalled:'已撤回（接口确认）',received:'已接收',sending:'发送中',accepted:'接口已受理',failed:'发送失败',unknown:'结果未知，请核对'};
const time = value => new Date(value*1000).toLocaleString('zh-CN',{hour12:false});
let bridge;
function notify(text){$('notice').textContent=text;$('notice').style.display='block';clearTimeout(notify.timer);notify.timer=setTimeout(()=>$('notice').style.display='none',6000);}
async function api(method,path,data={}){const result=await bridge[method](path,data);if(result?.status==='error')throw new Error(result.message);return result?.data??result;}
function node(tag,text,cls){const el=document.createElement(tag);if(text!=null)el.textContent=text;if(cls)el.className=cls;return el;}
// Today's times show only the clock; the full time stays in the tooltip.
function shortTime(value){const date=new Date(value*1000);const today=date.toDateString()===new Date().toDateString();return date.toLocaleString('zh-CN',today?{hour:'2-digit',minute:'2-digit',hour12:false}:{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hour12:false});}
// QQ's public avatar addresses; only plain QQ / group numbers are used.
function avatarUrl(id,group){id=String(id||'');return /^[1-9]\d{4,11}$/.test(id)?(group?`https://p.qlogo.cn/gh/${id}/${id}/100`:`https://q1.qlogo.cn/g?b=qq&nk=${id}&s=100`):'';}
function avatar(name,id,group){
  const box=node('span',null,'avatar');box.setAttribute('aria-hidden','true');
  box.append(node('span',[...String(name||'').trim()][0]||'?','avatar-initial'));
  const url=avatarUrl(id,group);
  // An address that failed once (blocked network, no avatar) is not requested again.
  if(url&&!state.failedAvatars.has(url)){const img=node('img');img.alt='';img.referrerPolicy='no-referrer';img.loading='lazy';img.onerror=()=>{state.failedAvatars.add(url);img.remove();};img.src=url;box.append(img);}
  return box;
}
function imageUrl(d){const url=d.preview||d.url||d.file||'';return /^https:\/\//i.test(url)||/^data:image\/(jpeg|png|gif|webp);base64,/.test(url)?url:'';}
// Request IDs: randomUUID needs a secure context, getRandomValues does not.
function requestId(){
  if(crypto.randomUUID)return crypto.randomUUID();
  const b=crypto.getRandomValues(new Uint8Array(16));b[6]=b[6]&15|64;b[8]=b[8]&63|128;
  const h=[...b].map(x=>x.toString(16).padStart(2,'0')).join('');
  return `${h.slice(0,8)}-${h.slice(8,12)}-${h.slice(12,16)}-${h.slice(16,20)}-${h.slice(20)}`;
}
async function chats(){
  state.chatRows=await api('apiGet','chats',{q:$('chatSearch').value});
  renderChats();
}
function renderChats(){
  const rows=state.chatRows;if(!rows)return;
  // Polling returns the same list most of the time; redrawing it would reload
  // avatars and lose hover and focus.
  const signature=JSON.stringify([rows,state.chat?.id,state.settings.avatars,new Date().toDateString()]);if(state.chatSignature===signature)return;state.chatSignature=signature;
  $('chats').replaceChildren();
  for(const row of rows){
    const b=node('button',null,'chat'+(state.chat?.id===row.id?' active':''));b.type='button';
    const text=node('div',null,'chat-text');
    const top=node('div',null,'chat-top');
    const sentAt=row.last_time?new Date(row.last_time*1000):null;
    const clock=node('time',sentAt?shortTime(row.last_time):'','chat-time');if(sentAt){clock.dateTime=sentAt.toISOString();clock.title=time(row.last_time);}
    top.append(node('b',row.title||row.target),clock);
    const meta=node('div',null,'chat-meta');
    meta.append(node('span',kinds[row.kind]||row.kind,'chat-kind'),node('span',row.last_sender||'暂无发送人','chat-sender'));
    const preview=node('small',row.preview||'暂无消息','chat-preview');preview.title=row.preview||'';
    text.append(top,meta,preview);
    if(state.settings.avatars)b.append(avatar(row.title||row.target,row.target,row.kind==='group'));
    b.append(text);b.onclick=()=>selectChat(row);$('chats').append(b);
  }
  if(!rows.length)$('chats').append(node('div',$('chatSearch').value?'没有匹配的会话':'暂无会话，等待新消息…','empty'));
}
async function selectChat(chat){
  if(state.busy){notify('正在发送，请等发送结束后再切换会话');return;}
  if(state.chat?.id!==chat.id){$('text').value='';clearImage();state.reference=null;$('composerMode').value='direct';$('replyMode').value=state.settings.reply_mode;state.expanded.clear();state.failedImages.clear();state.copies.clear();state.copyAsked.clear();state.deadUrls.clear();forgetQueuedSaves();state.lastTrace=null;}
  state.chat=chat;state.stick=true;const generation=++state.generation;
  state.messages=[];state.signature=null;state.noOlder=false;
  // The previous conversation must not stay on screen under the new title.
  $('messages').replaceChildren(node('div','正在读取消息…','empty'));
  // "全部" logs do not belong to a conversation, so they stay when switching.
  if(state.trace!==''){state.logs=[];state.logScope=null;state.logSignature=null;state.logFollow=true;showTrace(null);}
  $('title').textContent=chat.title||chat.target;
  $('route').textContent=`${kinds[chat.kind]} · ${chat.target} · 账号 ${chat.account}${chat.group_id?' · 来源群 '+chat.group_id:''}`;
  $('send').disabled=state.busy;$('messageSearch').value='';state.request=null;
  $('guideUser').disabled=chat.kind==='group';
  $('guideUser').textContent=chat.kind==='group'?'从群成员消息添加引导':'为此用户添加引导';
  renderComposer();
  try{await Promise.all([chats(),messages()]);}
  catch(e){if(state.generation===generation){$('messages').replaceChildren(node('div','读取消息失败，稍后会自动重试','empty'));notify('读取消息失败：'+e.message);}return;}
  if(state.generation===generation){const panel=$('messages');panel.scrollTop=panel.scrollHeight;}
}
function activeJobsFor(message){return message.direction==='in'?jobRows.filter(job=>(job.trace===message.trace||job.source_message_id===message.id)&&['running','stopping','awaiting_approval'].includes(job.status)):[];}
function renderSignature(){return JSON.stringify([state.messages,state.trace,state.settings.avatars,state.settings.auto_images,[...state.failedImages],state.messages.map(message=>activeJobsFor(message).map(job=>[job.trace,job.status,job.step,job.step_detail,job.step_started]))]);}
function renderMessages(){
  const signature=renderSignature();if(state.signature===signature)return;state.signature=signature;
  const panel=$('messages');const nearBottom=panel.scrollHeight-panel.scrollTop-panel.clientHeight<90;panel.replaceChildren();state.imageTools.clear();
  for(const message of state.messages){
    const row=node('article',null,'message '+message.direction+(state.settings.avatars?' with-avatar':'')+(message.status==='recalled'?' recalled':''));const content=node('div',null,'message-content');
    const meta=node('div',`${message.sender} · ${shortTime(message.created)}`,'meta');meta.title=time(message.created);content.append(meta);
    const bubble=node('div',null,'bubble'+(state.trace===message.trace||message.related_traces?.includes(state.trace)?' selected':''));
    bubble.tabIndex=0;bubble.setAttribute('role','button');
    bubble.onkeydown=e=>{if(e.target===bubble&&(e.key==='Enter'||e.key===' ')){e.preventDefault();bubble.click();}};
    if(!message.body?.some(seg=>seg.type!=='text'||String(seg.data?.text||'').trim()))bubble.append(node('span','[未采集到消息内容]','missing-content'));
    for(const [index,seg] of (message.body||[]).entries()){const d=seg.data||{};const key=message.id+':'+index;
      if(seg.type==='text')bubble.append(node('span',d.text||''));
      else if(seg.type==='image')bubble.append(imageSegment(d,key,message,index));
      else if(['record','audio','video','file'].includes(seg.type)){
        const label={record:'语音',audio:'语音',video:'视频',file:'文件'}[seg.type];
        const details=node('details',null,'attachment-details');details.onclick=e=>e.stopPropagation();details.open=state.expanded.has(key);details.ontoggle=()=>{if(details.open)state.expanded.add(key);else state.expanded.delete(key);};
        details.append(node('summary',`查看${label}信息${d.name?' · '+d.name:''}`));
        const url=d.url||d.file||'';
        details.append(/^https:\/\//i.test(url)?attachmentLink(url):node('small','暂无可打开的附件地址'));
        bubble.append(details);
      }else if(seg.type==='at')bubble.append(node('span','@'+(d.qq||'')));
      else if(seg.type==='reply')bubble.append(node('span','[引用消息 '+(d.id||'')+'] '));
      else bubble.append(node('span',`[${seg.type}] ${d.name||d.text||''}`));
    }
    bubble.onclick=async()=>{if(String(window.getSelection?.()||''))return;showTrace(message.trace);renderMessages();try{await logs();}catch(e){notify(e.message);}};
    content.append(bubble);
    // Received is the normal state of every incoming message; show the others.
    if(message.status!=='received')content.append(node('small',message.direction==='in'&&message.status==='recalled'?'对方已撤回（内容已保留）':statuses[message.status]||message.status,'status '+message.status));
    if(message.direction==='in'){
      const actions=node('div',null,'message-actions');
      for(const [label,mode] of [['引用回复','direct'],['AI 回复','reply'],['添加引导','new']]){
        const button=node('button',label);button.type='button';button.onclick=()=>chooseReference(message,mode,'message');actions.append(button);
      }
      content.append(actions);
      row.oncontextmenu=event=>{
        // Keep the browser menu for copying from text fields.
        if(state.busy||['INPUT','TEXTAREA'].includes(event.target?.tagName))return;event.preventDefault();state.menuMessage=message;
        const menu=$('messageMenu');menu.hidden=false;
        menu.style.left=Math.max(8,Math.min(event.clientX,window.innerWidth-menu.offsetWidth-8))+'px';
        menu.style.top=Math.max(8,Math.min(event.clientY,window.innerHeight-menu.offsetHeight-8))+'px';
        $('menuReply').focus();
      };
    }
    for(const activeJob of activeJobsFor(message)){
      const progress=node('div',null,'message-progress');
      const progressText=activeJob.status==='awaiting_approval'?'回复已生成 · 等待你确认发送':activeJob.status==='stopping'?'正在中断…':`处理中 · ${jobStep(activeJob)}`;
      const indicator=node('span',progressText,activeJob.status==='awaiting_approval'?'review-label':'processing-label');indicator.setAttribute('role','status');
      if(activeJob.step_started)indicator.title='当前步骤开始于 '+time(activeJob.step_started);
      const stop=node('button',activeJob.status==='stopping'?'正在中断':'中断','stop-message');stop.type='button';stop.disabled=activeJob.status==='stopping';
      stop.onclick=async()=>{
        stop.disabled=true;indicator.textContent='正在中断…';
        try{const result=await api('apiPost','stop',{trace:activeJob.trace});notify(result.stopped?'任务已中断':'已拦截后续发送，任务正在退出');await jobs();if(state.trace===message.trace||state.trace===activeJob.trace)await logs();}
        catch(e){notify(e.message);stop.disabled=false;indicator.textContent=progressText;}
      };
      progress.append(indicator,stop);content.append(progress);
      if(activeJob.status==='awaiting_approval'){
        const review=node('button','查看待发回复','view-review');review.type='button';
        review.onclick=async()=>{$('jobs').value=activeJob.trace;jobSelection();await loadReview();$('reviewPanel').scrollIntoView({block:'nearest',behavior:'smooth'});};
        progress.append(review);
      }
    }
    if(message.direction==='out'&&message.remote_id&&message.status!=='recalled'){
      const recall=node('button','撤回','recall');recall.type='button';let armed=false;
      // The sandbox has no confirm(); a second click within a few seconds confirms.
      recall.onclick=async()=>{
        if(!armed){armed=true;recall.textContent='确认撤回？';setTimeout(()=>{armed=false;if(!recall.disabled)recall.textContent='撤回';},4000);return;}
        recall.disabled=true;try{await api('apiPost','recall',{id:message.id});notify('撤回接口已受理');await messages();}catch(e){notify(e.message);recall.disabled=false;armed=false;recall.textContent='撤回';}
      };content.append(recall);
    }
    if(state.settings.avatars){
      const own=message.direction==='out';const chat=state.chat||{};
      row.append(avatar(message.sender,own?chat.account:message.sender_id||(chat.kind!=='group'?chat.target:''),false));
    }
    row.append(content);panel.append(row);
  }
  if(!state.messages.length)panel.append(node('div','暂无匹配消息','empty'));if(nearBottom)panel.scrollTop=panel.scrollHeight;
}
// Images load only when asked, or at once with 自动显示聊天图片. One that fails
// (usually expired or recalled) is shown from the copy saved on the server when
// there is one; otherwise it falls back to the button instead of reloading on
// every redraw.
function imageSegment(d,key,message,index){
  const url=imageUrl(d);
  // Only received images with a web address can be saved; the server reads the
  // address from its own record of the message.
  const canSave=message?.direction==='in'&&/^https?:\/\//i.test(d.url||d.file||'');
  if(!url&&!canSave)return node('span','[图片：暂无可预览地址]');
  // Polling replaces the message objects, so the current one is looked up.
  const copyState=()=>(state.messages.find(x=>x.id===message.id)||message).media?.[index];
  const box=node('span',null,'image-box');const tools=node('span',null,'image-tools');
  const drawTools=()=>{
    tools.replaceChildren();if(!canSave)return;
    if(copyState()==='saved'){
      const chip=node('span','已保存','saved-chip');chip.title='服务器上已有副本，图片过期或被撤回后仍可查看';
      const download=node('button','下载');download.type='button';download.onclick=e=>{e.stopPropagation();downloadCopy(message.id,index);};
      tools.append(chip,download);return;
    }
    const save=node('button',copyState()==='missing'?'副本缺失，重新保存':copyState()==='failed'?'保存失败，重试':'保存');save.type='button';save.title='保存到服务器，图片过期或被撤回后仍可查看';
    save.onclick=async e=>{
      e.stopPropagation();save.disabled=true;save.textContent='正在保存…';
      // The conversation may have been redrawn meanwhile: the buttons now on screen are updated.
      const done=await saveCopy(message.id,index,true);state.imageTools.get(key)?.();
      // An image the browser could not show appears once its copy exists.
      if(done&&(!url||state.failedImages.has(key))){state.failedImages.delete(key);state.expanded.add(key);state.signature=null;renderMessages();}
    };
    tools.append(save);
  };
  const copyImage=src=>{const img=node('img');img.alt='图片（服务器保存的副本）';img.onload=keepBottom;img.onclick=e=>{e.stopPropagation();openViewer(src,message.id,index);};img.src=src;return img;};
  const loadCopy=()=>{
    if(state.copies.has(key))return copyImage(state.copies.get(key));
    const slot=node('span','正在读取已保存的图片…','image-note');
    fetchCopy(key,message.id,index).then(src=>slot.replaceWith(copyImage(src)))
      .catch(e=>{state.expanded.delete(key);state.failedImages.add(key);slot.textContent='[已保存的图片无法显示：'+e.message+']';});
    return slot;
  };
  // A copy is fetched by itself once per conversation. After the small cache
  // has let it go it comes back on a click, so a long conversation does not
  // fetch its images again on every redraw.
  const copyView=()=>{
    if(state.copies.has(key)||state.copyLoads.has(key)||!state.copyAsked.has(key)){state.copyAsked.add(key);return loadCopy();}
    const again=node('button','查看已保存的图片','load-image');again.type='button';
    again.onclick=e=>{e.stopPropagation();again.replaceWith(loadCopy());};
    return again;
  };
  // The QQ address failed and a copy exists: later redraws do not ask QQ again.
  const fromCopy=()=>copyState()==='saved'&&(!url||state.deadUrls.has(key));
  const showImage=()=>{
    if(!url)return loadCopy();
    const img=node('img');img.alt='图片';img.referrerPolicy='no-referrer';
    img.onerror=()=>{
      if(copyState()==='saved'){state.deadUrls.add(key);img.replaceWith(copyView());return;}
      state.expanded.delete(key);state.failedImages.add(key);img.replaceWith(node('span','[图片已过期或无法预览]'));
    };
    // 自动保存看过的图片: an image counts as seen once the browser has shown it.
    img.onload=()=>{keepBottom();if(canSave&&state.settings.image_save==='viewed'&&(!copyState()||copyState()==='missing'))queueSave(message.id,index);};
    img.onclick=e=>{e.stopPropagation();openViewer(/^https:\/\//i.test(d.url||'')?d.url:url,copyState()==='saved'?message.id:null,index);};
    img.src=url;return img;
  };
  const wanted=state.expanded.has(key)||(state.settings.auto_images&&!state.failedImages.has(key));
  let view;
  if(state.copies.has(key)||(wanted&&fromCopy()))view=copyView();
  else if(wanted&&url)view=showImage();
  else if(!url&&copyState()!=='saved')view=node('span','[图片：浏览器无法直接预览，保存后可查看]','image-note');
  else{
    const load=node('button',state.failedImages.has(key)?'图片加载失败，点击重试':'查看图片（按需加载）','load-image');load.type='button';
    load.onclick=e=>{e.stopPropagation();state.failedImages.delete(key);state.expanded.add(key);load.replaceWith(fromCopy()?loadCopy():showImage());};
    view=load;
  }
  state.imageTools.set(key,drawTools);drawTools();box.append(view,tools);return box;
}
function keepBottom(){if(state.stick!==false){const panel=$('messages');panel.scrollTop=panel.scrollHeight;}}
$('messages').onscroll=()=>{const panel=$('messages');state.stick=panel.scrollHeight-panel.scrollTop-panel.clientHeight<90;};
// One request per copy at a time: a redraw while it is on its way waits for the
// same answer.
function fetchCopy(key,id,index){
  let pending=state.copyLoads.get(key);
  if(!pending){
    pending=api('apiGet','media',{id,index}).then(data=>{
      if(!imageUrl({url:data.url}))throw new Error('格式不支持');
      rememberCopy(key,data.url);return data.url;
    }).finally(()=>state.copyLoads.delete(key));
    state.copyLoads.set(key,pending);
  }
  return pending;
}
// Copies already fetched in this conversation; a few, so memory stays bounded.
function rememberCopy(key,url){state.copies.delete(key);state.copies.set(key,url);while(state.copies.size>24)state.copies.delete(state.copies.keys().next().value);}
// Keep the local rows equal to what the next poll returns, so one saved image
// does not redraw the whole conversation.
function markCopy(id,index,status){
  const message=state.messages.find(x=>x.id===id);if(!message)return;
  const current=renderSignature()===state.signature;
  message.media={...(message.media||{}),[index]:status};
  if(current)state.signature=renderSignature();
}
async function saveCopy(id,index,manual){
  try{
    const result=await api('apiPost','media_save',{id,index,...(manual?{retry:true}:{})});
    markCopy(id,index,result.status==='saved'?'saved':'failed');
    if(manual)notify(result.status==='saved'?'图片已保存到服务器':'保存失败：'+(result.reason||'原因未知'));
    return result.status==='saved';
  }catch(e){if(manual)notify('保存失败：'+e.message);return null;}
}
// One request at a time, and each image once per page session. An image whose
// request never got an answer, or that was still waiting when the conversation
// was left, is forgotten, so showing it again saves it.
function queueSave(id,index){
  const key=id+':'+index;if(state.saveSeen.has(key))return;state.saveSeen.add(key);
  state.saveQueue.push([id,index]);if(!state.saving)pumpSaves();
}
function forgetQueuedSaves(){for(const [id,index] of state.saveQueue)state.saveSeen.delete(id+':'+index);state.saveQueue.length=0;}
async function pumpSaves(){
  state.saving=true;
  try{while(state.saveQueue.length){
    const [id,index]=state.saveQueue.shift();const key=id+':'+index;
    if(await saveCopy(id,index,false)===null)state.saveSeen.delete(key);
    state.imageTools.get(key)?.();
  }}
  finally{state.saving=false;}
}
// The dashboard fetches the file with the administrator's login and saves it.
async function downloadCopy(id,index){
  try{await bridge.download('media_file',{id,index});notify('已开始下载原图');}
  catch(e){notify('下载失败：'+(e?.message||e));}
}
// The page's sandbox has no allow-popups, so a new-window link would do nothing.
function attachmentLink(url){
  const box=node('div',null,'attachment-link');
  const field=node('input');field.readOnly=true;field.value=url;field.setAttribute('aria-label','附件地址');field.onfocus=()=>field.select();
  const copy=node('button','复制链接');copy.type='button';
  copy.onclick=async()=>{
    try{await navigator.clipboard.writeText(url);notify('链接已复制，可在新的浏览器标签页中打开');return;}catch{}
    field.focus();field.select();
    let copied=false;try{copied=document.execCommand('copy');}catch{}
    notify(copied?'链接已复制，可在新的浏览器标签页中打开':'请按 Ctrl+C 复制已选中的链接');
  };
  box.append(field,copy);return box;
}
async function messages(older=false){if(!state.chat)return;const generation=state.generation;const data={chat:state.chat.id,q: $('messageSearch').value};if(older&&state.messages.length)data.before=state.messages[0].id;const rows=await api('apiGet','messages',data);if(generation!==state.generation)return;if(older){const known=new Set(state.messages.map(x=>x.id));const fresh=rows.filter(x=>!known.has(x.id));if(!fresh.length){notify('没有更早的消息了');return;}const panel=$('messages');const height=panel.scrollHeight;const top=panel.scrollTop;state.messages=[...fresh,...state.messages].sort((a,b)=>a.id-b.id);renderMessages();panel.scrollTop=top+panel.scrollHeight-height;return;}else{const byId=new Map(state.messages.map(x=>[x.id,x]));for(const row of rows)byId.set(row.id,row);state.messages=[...byId.values()].sort((a,b)=>a.id-b.id);}renderMessages();}
async function logs(older=false){
  if(state.trace===null)return;
  const trace=state.trace;const data={trace,q:$('logSearch').value,level:$('level').value};
  const scope=JSON.stringify([trace,data.q,data.level]);
  if(older&&state.logs.length&&scope===state.logScope)data.before=state.logs[0].id;
  const rows=await api('apiGet','logs',data);
  if(scope!==JSON.stringify([state.trace,$('logSearch').value,$('level').value]))return;
  const panel=$('logs');const oldTop=panel.scrollTop;const oldHeight=panel.scrollHeight;
  const changed=scope!==state.logScope;
  const follow=older?false:(changed||state.logFollow!==false);
  if(!changed&&(older||!follow)){
    const merged=new Map(state.logs.map(log=>[log.id,log]));for(const log of rows)merged.set(log.id,log);
    state.logs=[...merged.values()].sort((a,b)=>a.id-b.id);
  }else state.logs=rows;
  state.logScope=scope;state.logFollow=follow;
  const signature=JSON.stringify([scope,state.logs]);
  if(signature!==state.logSignature){
    state.logSignature=signature;panel.replaceChildren();
    for(const log of state.logs){const el=node('div',null,'log '+log.level);el.append(node('small',`${time(log.created)} · ${log.level} · ${log.source}`),node('span',log.body));panel.append(el);}
    if(!state.logs.length)panel.append(node('div','暂无匹配日志','empty'));
    panel.scrollTop=follow?panel.scrollHeight:(older&&!changed?oldTop+panel.scrollHeight-oldHeight:oldTop);
  }else if(follow)panel.scrollTop=panel.scrollHeight;
}
$('logs').onscroll=()=>{const panel=$('logs');state.logFollow=panel.scrollHeight-panel.scrollTop-panel.clientHeight<=24;};
$('composer').onsubmit=async event=>{
  event.preventDefault();if(!state.chat||state.busy)return;
  const chat=state.chat;const text=$('text').value;const mode=$('composerMode').value;
  if(!text.trim()&&!(mode==='direct'&&state.image)&&mode!=='reply')return;
  if(mode==='reply'&&!(state.reference?.kind==='message'&&state.reference.message)){notify('请先右键或点击消息旁的“AI 回复”选择要回复的消息');return;}
  if(mode!=='direct'&&chat.kind==='group'&&!state.reference?.message){notify('请先选择群成员的消息');return;}
  if(mode==='append'&&!$('appendTask').value){notify('该用户暂无可以加入的任务，可选择新建任务');return;}
  state.request ||= requestId();
  const reference=state.reference;
  const payload=mode==='direct'?{
    chat:chat.id,text,image:state.image,request_id:state.request,
    ...(reference?.message?{reference_id:reference.message.id,reference_kind:reference.kind}:{})
  }:{
    chat:chat.id,guidance:text,mode,reply_mode:$('replyMode').value,request_id:state.request,
    reference_kind:reference?.kind||'user',
    ...(reference?.message?{message_id:reference.message.id}:{}),
    ...(mode==='append'?{trace:$('appendTask').value}:{})
  };
  state.busy=true;renderComposer();
  try{
    const result=await api('apiPost',mode==='direct'?'send':'guide',payload);
    // A repeated request whose first attempt has no confirmed result is not
    // sent twice by accident: the text stays, and the next click is a new send.
    if(result.duplicate&&mode==='direct'&&result.status!=='accepted'){
      state.request=null;showTrace(result.trace);
      notify('上一次发送的结果未知，这次没有再发。请先看聊天记录：确认对方没收到后，再点一次“发送消息”即可重新发送。');
      await messages();await logs();return;
    }
    notify(result.duplicate?'该请求已提交，请查看对应任务或消息':mode==='direct'?'发送接口已受理':mode==='append'?'引导已加入用户任务，将作用于接下来的 AI 回复':mode==='reply'?(payload.reply_mode==='confirm'?'已让 AI 回复这条消息，生成后等待你确认':'已让 AI 回复这条消息，生成后自动发送'):'引导任务已创建');
    if(state.chat?.id===chat.id){
      $('text').value='';clearImage();state.request=null;showTrace(result.trace);
      // A quote applies to one message. Guidance modes stay selected, so a
      // second guidance text can never go to the user by mistake.
      if(mode==='reply'){state.reference=null;$('composerMode').value='direct';}
      else if(mode==='direct')state.reference=null;
      await jobs();
      if(mode!=='direct'){$('jobs').value=result.trace;jobSelection();await loadReview();}
      await messages();await logs();
    }
  }catch(error){notify(error.message+'；重试会保留本次请求 ID，避免重复提交。');}
  finally{state.busy=false;renderComposer();}
};
$('text').oninput=()=>{state.request=null;};
// Ctrl / ⌘ + Enter always sends; plain Enter sends only when chosen in settings,
// and never while an input method is still composing.
$('text').onkeydown=e=>{
  if(e.key!=='Enter'||e.isComposing||e.keyCode===229)return;
  if(e.ctrlKey||e.metaKey||(state.settings.send_key==='enter'&&!e.shiftKey&&!e.altKey)){e.preventDefault();$('composer').requestSubmit();}
};
function sendKeyHint(){return state.settings.send_key==='enter'?'Enter 发送，Shift + Enter 换行':'Ctrl / ⌘ + Enter 发送';}
function clearImage(){state.image='';$('image').value='';$('attachment').textContent='';$('clearImage').hidden=true;state.request=null;}
$('clearImage').onclick=clearImage;$('image').onchange=async()=>{const file=$('image').files[0];if(!file)return;if(file.size>2*1024*1024){notify('图片不能超过 2 MB');clearImage();return;}const reader=new FileReader();reader.onload=()=>{state.image=String(reader.result).split(',')[1];$('attachment').textContent='待发送图片：'+file.name;$('clearImage').hidden=false;state.request=null;};reader.readAsDataURL(file);};
// trace: null = no message selected, '' = all logs, otherwise one processing chain.
function showTrace(trace){
  state.trace=trace;if(trace)state.lastTrace=trace;
  $('logAll').setAttribute('aria-pressed',String(trace===''));$('logSelected').setAttribute('aria-pressed',String(trace!==''));
  $('trace').textContent=trace===''?'全部日志，包含没有关联到消息的后台日志':trace?'追踪 '+trace:'点击聊天气泡查看关联日志';
  if(trace===null)$('logs').replaceChildren(node('div','暂无选中的消息','empty'));
}
function setLogScope(trace){
  state.logScope=null;state.logFollow=true;showTrace(trace);if(state.chat)renderMessages();
  return trace===null?undefined:logs();
}
for(const [id,fn] of [['older',()=>messages(true)],['olderLogs',()=>logs(true)],['logAll',()=>state.trace===''?undefined:setLogScope('')],['logSelected',()=>state.trace===''?setLogScope(state.lastTrace):undefined]])$(id).onclick=()=>Promise.resolve(fn()).catch(e=>notify(e.message));
// Searches follow typing after a short pause; Enter searches at once.
function liveSearch(id,fn){
  const run=()=>{clearTimeout(liveSearch[id]);Promise.resolve(fn()).catch(e=>notify(e.message));};
  $(id).oninput=()=>{clearTimeout(liveSearch[id]);liveSearch[id]=setTimeout(run,300);};$(id).onchange=run;
}
liveSearch('chatSearch',chats);liveSearch('messageSearch',()=>{state.generation++;state.messages=[];return messages();});liveSearch('logSearch',()=>logs());
$('level').onchange=()=>logs().catch(e=>notify(e.message));
$('export').onclick=()=>{const blob=new Blob([JSON.stringify({chat:state.chat,trace:state.trace,logs:state.logs},null,2)],{type:'application/json'});const url=URL.createObjectURL(blob);const a=node('a');a.href=url;a.download='astrbot-trace-'+(state.trace||'global')+'.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
async function health(){
  const data=await api('apiGet','state');
  const text='已连接'+(data.dropped?` · 丢弃 ${data.dropped} 条记录`:'');
  $('health').className='health '+(data.dropped?'warn':'ok');$('health').textContent=text;
  $('health').title=`保留 ${data.retention_days} 天聊天和日志 · 丢弃记录 ${data.dropped}`;
  if(data.images)$('imageUsage').textContent=`已保存 ${data.images.files} 张，占用 ${(data.images.bytes/1048576).toFixed(1)} MB（上限 ${data.images.quota_mb>=1024?data.images.quota_mb/1024+' GB':data.images.quota_mb+' MB'}）`+(data.images.queued?`，${data.images.queued} 张等待保存`:'');
  if(data.version)$('about').textContent=`管理插件 ${data.version} · 设置保存在插件数据库中，所有管理员共用。`;
}
function offline(error){$('health').className='health error';$('health').textContent='连接异常';$('health').title=error.message;}
// Every round retries, so the page recovers by itself after a reload or a network
// error; the status shows the last result.
async function poll(){
  try{if(!document.hidden){if(!state.settingsLoaded)await loadSettings();await health();await chats();await messages();await logs();await jobs();}}
  catch(e){offline(e);}
  finally{clearTimeout(poll.timer);poll.timer=setTimeout(poll,2500);}
}
async function start(){for(let i=0;i<100&&!window.AstrBotPluginPage;i++)await new Promise(r=>setTimeout(r,100));bridge=window.AstrBotPluginPage;if(!bridge)throw new Error('请从 AstrBot 插件的「会话管理」页面打开');await bridge.ready();await poll();}
start().catch(e=>{offline(e);notify(e.message);});

let jobRows=[];
const jobNames={running:'处理中',stopping:'正在中断',cancelled:'已中断',completed:'处理结束',failed:'失败',draft:'草稿就绪',interrupted:'重启中断',awaiting_approval:'待确认发送'};
const stepNames={model:'等待模型回复',tool:'执行工具',response:'处理模型结果',decorating:'整理待发送消息',sending:'发送回复',sent:'发送完成，等待后续处理',preparing_guidance:'准备用户任务',preparing_draft:'准备引导上下文',guided_model:'按引导生成回复',draft_ready:'草稿已生成',awaiting_approval:'等待你确认发送'};
function jobStep(job){return (stepNames[job.step]||'等待阶段通知')+(job.step_detail?'：'+job.step_detail:'');}
async function jobs(){
  const rows=await api('apiGet','jobs');jobRows=rows;const selected=$('jobs').value;
  $('jobs').replaceChildren(node('option','选择任务'));$('jobs').firstChild.value='';
  for(const row of rows){const step=row.status==='running'?' · '+jobStep(row):'';const option=node('option',`${jobNames[row.status]||row.status}${step} · ${row.title||'会话'} · ${(row.prompt||'').slice(0,30)}`);option.value=row.trace;$('jobs').append(option);}
  $('jobs').value=rows.some(x=>x.trace===selected)?selected:'';
  $('runningCount').textContent=rows.filter(x=>['running','stopping'].includes(x.status)).length;
  const waiting=rows.filter(x=>x.pending_count>0).length;$('waitingCount').hidden=!waiting;$('waitingCount').textContent=`${waiting} 个待确认`;$('waitingCount').title='点击查看待确认的回复';
  jobSelection();renderComposer();if(state.chat)renderMessages();await loadReview();
}
function jobSelection(){
  const job=jobRows.find(x=>x.trace===$('jobs').value);
  $('stop').disabled=!job||!['running','stopping','awaiting_approval'].includes(job.status);$('guide').disabled=!job?.can_append;$('newFromJob').disabled=!job;$('useDraft').hidden=!job?.draft;
  const step=job?`\n${['running','stopping','awaiting_approval'].includes(job.status)?'当前步骤':'最后步骤'}：${jobStep(job)}${job.step_started?'\n步骤开始：'+time(job.step_started):''}`:'';
  const delivery=job?.reply_mode?`\n发送方式：${job.reply_mode==='confirm'?'我确认后发送':'生成后自动发送'}`:'';
  $('jobInfo').textContent=job?`${jobNames[job.status]||job.status} · ${time(job.started)}${step}${delivery}\n原始任务：${job.prompt||''}`:'选择任务可查看引导、待发回复和关联日志';
  if(state.reviewTrace!==job?.trace){state.reviewSignature=null;state.reviewIds=[];$('reviewPanel').hidden=true;$('instructionHistory').hidden=true;}
}
$('jobs').onchange=async()=>{
  jobSelection();
  const job=jobRows.find(x=>x.trace===$('jobs').value);
  const navigation=(state.navigation||0)+1;state.navigation=navigation;
  if(!job){try{await loadReview();}catch(e){notify(e.message);}return;}
  if(state.busy){notify('消息正在发送，请发送结束后再切换任务');return;}
  try{
    const rows=await api('apiGet','chats',{id:job.chat});
    if(state.navigation!==navigation)return;
    const chat=rows.find(x=>x.id===job.chat);
    if(!chat)throw new Error('对应会话已不存在');
    $('chatSearch').value='';
    await selectChat(chat);
    if(state.navigation!==navigation||state.chat?.id!==job.chat)return;
    const related=await api('apiGet','messages',{chat:job.chat,trace:job.trace});
    if(state.navigation!==navigation||state.chat?.id!==job.chat)return;
    const merged=new Map(state.messages.map(x=>[x.id,x]));
    for(const message of related)merged.set(message.id,message);
    state.messages=[...merged.values()].sort((a,b)=>a.id-b.id);
    showTrace(job.trace);
    renderMessages();
    const selected=$('messages').querySelector('.message.in .selected')||$('messages').querySelector('.selected');
    (selected||$('title')).scrollIntoView({block:'center',behavior:'smooth'});
    await logs();
    await loadReview();
  }catch(e){notify(e.message);}
};
$('stop').onclick=async()=>{const trace=$('jobs').value;if(!trace)return;try{const result=await api('apiPost','stop',{trace});notify(result.stopped?'任务已中断，旧任务后续发送已拦截':'已拦截后续发送，任务正在退出');await jobs();}catch(e){notify(e.message);}};
$('useDraft').onclick=async()=>{const job=jobRows.find(x=>x.trace===$('jobs').value);if(!job?.draft)return;try{const rows=await api('apiGet','chats',{id:job.chat});const chat=rows.find(x=>x.id===job.chat);if(!chat)throw new Error('原会话已不可用');await selectChat(chat);state.reference=null;$('composerMode').value='direct';$('text').value=job.draft;state.request=null;renderComposer();$('text').focus();notify('草稿已填入，可编辑后发送');}catch(e){notify(e.message);}};

function chooseReference(message,mode,kind){
  if(!state.chat||state.busy)return;
  state.reference={chat:state.chat.id,message,kind};state.request=null;
  $('composerMode').value=mode;$('messageMenu').hidden=true;renderComposer();$('text').focus();
}

function renderComposer(){
  const mode=$('composerMode').value;const reference=state.reference;
  $('reference').hidden=!reference;
  if(reference){
    const person=reference.message?.sender||state.chat?.title||state.chat?.target||'';
    $('referenceTitle').textContent=(reference.kind!=='message'?'选择的用户 · ':mode==='reply'?'AI 将回复这条消息 · ':'引用的用户消息 · ')+person;
    $('referenceText').textContent=reference.kind==='message'?(reference.message?.body||[]).map(seg=>seg.type==='text'?(seg.data?.text||''):`[${{image:'图片',file:'文件',record:'语音',video:'视频',reply:'引用',at:'@'}[seg.type]||seg.type}]`).join(' '):`${reference.message?.sender_id?'QQ '+reference.message.sender_id:state.chat?.kind==='group'?'用户 QQ 号待核实':'QQ '+(state.chat?.target||'')} · 在当前会话回复`;
  }
  $('replyModeLabel').hidden=mode==='direct';$('appendTaskLabel').hidden=mode!=='append';$('imageLabel').hidden=mode!=='direct';
  $('composerLabel').textContent=mode==='direct'?'发送给用户的消息':mode==='reply'?'补充要求（可不填，仅供 AI 使用）':'我的引导词（仅供 AI 使用）';
  $('text').setAttribute('aria-label',mode==='direct'?'消息内容':mode==='reply'?'补充要求':'我的引导词');
  $('text').placeholder=mode==='direct'?'输入要发送的消息 · '+sendKeyHint():mode==='reply'?'可不填。例如：语气轻松一点，简短回应。':'例如：用当前人设温和地回复，解释清楚刚才的问题。';
  $('guidanceHint').textContent=mode==='direct'?'以下内容将以机器人 QQ 账号原样发送。':mode==='reply'?'AI 会按人设和上下文主动回复上方消息，回复会引用原消息；这里的补充不会发给用户。':'上方是引用对象；这里的文字会作为管理员引导插入 AI 任务。';
  $('sendHint').textContent=mode==='direct'?sendKeyHint():$('replyMode').value==='confirm'?'AI 回复生成后等待你确认':'AI 回复生成后自动发送到当前会话';
  $('send').textContent={direct:'发送消息',reply:'让 AI 回复',new:'新建引导任务',append:'加入用户任务'}[mode];
  const selected=$('appendTask').value;
  const candidates=jobRows.filter(job=>job.chat===state.chat?.id&&job.can_append&&(state.chat?.kind!=='group'||(reference?.message&&(job.source_message_id===reference.message.id||(reference.message.sender_id&&job.sender_id===reference.message.sender_id)))));
  $('appendTask').replaceChildren(node('option',candidates.length?'选择该用户的任务':'暂无可加入任务，请新建任务'));$('appendTask').firstChild.value='';
  for(const job of candidates){const option=node('option',`${jobStep(job)} · ${(job.prompt||'').slice(0,40)}`);option.value=job.trace;$('appendTask').append(option);}
  $('appendTask').value=candidates.some(job=>job.trace===selected)?selected:candidates.length===1?candidates[0].trace:'';
  $('send').disabled=!state.chat||state.busy||(mode==='append'&&!$('appendTask').value);
  for(const id of ['composerMode','replyMode','appendTask','clearReference','text','image','clearImage'])$(id).disabled=state.busy;
}

$('guideUser').onclick=()=>chooseReference(null,'new','user');
$('clearReference').onclick=()=>{if(state.busy)return;state.reference=null;state.request=null;renderComposer();};
$('composerMode').onchange=()=>{
  if($('composerMode').value!=='direct'&&state.image){$('composerMode').value='direct';notify('请先移除待发送图片；引导任务可直接引用用户的图片消息。');}
  if(!['direct','reply'].includes($('composerMode').value)&&!state.reference&&state.chat?.kind!=='group')state.reference={chat:state.chat?.id,message:null,kind:'user'};
  if($('composerMode').value==='reply'&&state.reference?.message)state.reference={...state.reference,kind:'message'};
  state.request=null;renderComposer();
};
for(const id of ['replyMode','appendTask'])$(id).onchange=()=>{state.request=null;renderComposer();};
for(const [id,mode,kind] of [['menuReply','direct','message'],['menuAiReply','reply','message'],['menuGuideMessage','new','message'],['menuGuideUser','new','user']])$(id).onclick=()=>chooseReference(state.menuMessage,mode,kind);
document.addEventListener('pointerdown',event=>{if(!$('messageMenu').contains(event.target))$('messageMenu').hidden=true;});
document.addEventListener('keydown',event=>{if(event.key==='Escape'){$('messageMenu').hidden=true;$('viewer').hidden=true;}});

for(const [id,mode] of [['guide','append'],['newFromJob','new']])$(id).onclick=()=>taskGuidance(mode).catch(e=>notify(e.message));
async function taskGuidance(mode){
  const job=jobRows.find(x=>x.trace===$('jobs').value);if(!job)return;
  if(state.busy){notify('正在发送，请稍候再操作');return;}
  if(state.chat?.id!==job.chat)await $('jobs').onchange();
  if(state.chat?.id!==job.chat)return;
  let message=state.messages.find(x=>x.id===job.source_message_id);
  if(!message&&job.source_message_id){
    const related=await api('apiGet','messages',{chat:job.chat,trace:job.trace});message=related.find(x=>x.id===job.source_message_id);
  }
  chooseReference(message||null,mode,message?'message':'user');
  if(mode==='append')$('appendTask').value=job.trace;
  if(job.reply_mode)$('replyMode').value=job.reply_mode;renderComposer();
}
// The waiting count opens the first task that needs a decision.
$('waitingCount').onclick=event=>{
  event.preventDefault();const job=jobRows.find(x=>x.pending_count>0);if(!job)return;
  $('taskPanel').open=true;$('jobs').value=job.trace;Promise.resolve($('jobs').onchange()).catch(e=>notify(e.message));
};

async function loadReview(){
  const trace=$('jobs').value;
  if(!trace){state.reviewTrace=null;state.reviewIds=[];$('reviewPanel').hidden=true;$('instructionHistory').hidden=true;return;}
  const data=await api('apiGet','review',{trace});if($('jobs').value!==trace)return;
  const signature=JSON.stringify([trace,data]);if(signature===state.reviewSignature)return;
  state.reviewSignature=signature;state.reviewTrace=trace;state.reviewIds=data.items.filter(row=>row.status==='pending').map(row=>row.id);
  const instructions=$('instructionHistory');instructions.replaceChildren();instructions.hidden=!data.instructions.length;
  if(data.instructions.length){
    const details=node('details');details.append(node('summary',`本任务的管理员引导（${data.instructions.length}）`));
    for(const item of data.instructions){
      if(item.reference_kind==='message'&&item.quoted_text)details.append(node('div',`引用 ${item.sender||'用户'}：${item.quoted_text}`,'instruction-reference'));
      details.append(node('div',item.guidance,'instruction-text'));
    }
    instructions.append(details);
  }
  const job=jobRows.find(row=>row.trace===trace);let route=[];try{route=JSON.parse(job?.chat||'[]');}catch{}
  $('reviewTarget').textContent=`发送到：${job?.title||route[3]||''} · ${kinds[route[2]]||''} ${route[3]||''} · 账号 ${route[1]||''}${route[4]?' · 来源群 '+route[4]:''}`;
  const panel=$('reviewBody');panel.replaceChildren();$('reviewPanel').hidden=!data.items.length;
  for(const item of data.items){
    const entry=node('div',null,'review-entry');entry.append(node('small',`${time(item.created)} · ${{pending:'待确认',approved:'已确认，等待发送',sending:'正在发送',accepted:'接口已受理',failed:'发送失败',unknown:'发送结果未知，请查看日志',discarded:'已放弃'}[item.status]||item.status}`));
    for(const seg of item.body){
      const d=seg.data||{};
      if(seg.type==='text')entry.append(node('div',d.text||''));
      else if(seg.type==='image'){
        const url=imageUrl(d);const button=node('button','查看待发图片');button.type='button';
        button.disabled=!url;
        button.onclick=()=>{const img=node('img');img.src=url;img.alt='待发图片';img.referrerPolicy='no-referrer';button.replaceWith(img);};entry.append(button);
        if(url&&state.settings.auto_images)button.onclick();
      }else if(seg.type==='reply')entry.append(node('small',`引用消息 ${d.id||''}`));
      else if(seg.type==='at')entry.append(node('span',`@${d.qq||''} `));
      else entry.append(node('div',`[${seg.type}] ${d.name||d.text||''}`));
    }
    panel.append(entry);
  }
  $('approveReply').hidden=!state.reviewIds.length;$('discardReply').hidden=!state.reviewIds.length;
}

for(const [id,action] of [['approveReply','approve'],['discardReply','discard']])$(id).onclick=async()=>{
  const trace=state.reviewTrace;const ids=[...state.reviewIds];if(!trace||!ids.length||state.busy)return;
  state.busy=true;renderComposer();$('approveReply').disabled=true;$('discardReply').disabled=true;
  try{
    const result=await api('apiPost',action,{trace,ids});
    notify(action==='discard'?'已放弃待发回复并停止任务':result.duplicate?'这批回复已经确认，请查看发送状态':'已确认，正在发送以上回复');
    await jobs();await messages();await logs();
  }catch(error){notify(error.message);}
  finally{state.busy=false;$('approveReply').disabled=false;$('discardReply').disabled=false;renderComposer();}
};
// Settings are saved in console.db through the plugin API: the sandboxed page
// has no browser storage of its own.
async function loadSettings(){try{applySettings(await api('apiGet','settings'));}catch{}}
function applySettings(values){
  const previous=state.settings.reply_mode;
  state.settings={...DEFAULT_SETTINGS,...values};state.settingsLoaded=true;
  if(state.settings.reply_mode!==previous)$('replyMode').value=state.settings.reply_mode;
  syncSettings();
}
function syncSettings(){
  const s=state.settings;
  $('settingAutoImages').checked=s.auto_images;$('settingAvatars').checked=s.avatars;
  $('settingSendKey').value=s.send_key;$('settingReplyMode').value=s.reply_mode;$('settingMessageActions').value=s.message_actions;
  $('settingImageSave').value=s.image_save;$('settingImageQuota').value=String(s.image_quota);
  $('app').setAttribute('data-actions',s.message_actions);
  state.reviewSignature=null;renderChats();if(state.chat)renderMessages();renderComposer();
}
async function saveSetting(key,value){
  const previous={...state.settings};
  $('settingsStatus').className='';$('settingsStatus').textContent='正在保存…';
  try{applySettings(await api('apiPost','settings',{[key]:value}));$('settingsStatus').textContent='已保存';}
  catch(e){state.settings=previous;syncSettings();$('settingsStatus').className='error';$('settingsStatus').textContent='保存失败：'+e.message;}
}
for(const [id,key,read] of [['settingAutoImages','auto_images',el=>el.checked],['settingAvatars','avatars',el=>el.checked],['settingSendKey','send_key',el=>el.value],['settingReplyMode','reply_mode',el=>el.value],['settingMessageActions','message_actions',el=>el.value],['settingImageSave','image_save',el=>el.value],['settingImageQuota','image_quota',el=>Number(el.value)]])$(id).onchange=()=>saveSetting(key,read($(id)));
$('openSettings').onclick=()=>{syncSettings();$('settingsStatus').textContent='';$('settings').showModal();};
$('closeSettings').onclick=()=>$('settings').close();
// A click on the dimmed area outside the panel closes the dialog.
$('settings').onclick=event=>{if(event.target===$('settings'))$('settings').close();};
// id is set when the server has a copy, so the original can be downloaded.
function openViewer(url,id=null,index=0){$('viewerImage').src=url;state.viewerCopy=id==null?null:[id,index];$('viewerDownload').hidden=id==null;$('viewer').hidden=false;}
$('viewerDownload').onclick=event=>{event.stopPropagation();if(state.viewerCopy)downloadCopy(...state.viewerCopy);};
$('viewer').onclick=()=>{$('viewer').hidden=true;};$('viewerClose').onclick=event=>{event.stopPropagation();$('viewer').hidden=true;};




