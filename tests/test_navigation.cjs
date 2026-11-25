const fs=require('node:fs'),vm=require('node:vm'),assert=require('node:assert/strict');
class Element {
 constructor(){this.value='';this.children=[];this.style={};this.scrollHeight=0;this.scrollTop=0;this.clientHeight=300;}
 setAttribute(name,value){this[name]=value;} append(...items){this.children.push(...items);} replaceChildren(...items){this.children=items;}
 get firstChild(){return this.children[0];} scrollIntoView(){this.scrolled=true;} querySelector(){return null;} focus(){} contains(){return false;}
}
const created=[];const elements=new Map();const get=id=>{if(!elements.has(id))elements.set(id,new Element());return elements.get(id);};
get('composerMode').value='direct';get('replyMode').value='confirm';
const chat={id:'["qq","999","temporary","200","100"]',title:'测试用户',target:'200',account:'999',kind:'temporary',group_id:'100',last_sender:'测试发送人',last_time:1700000010,preview:'[图片]'};
const source={id:1,chat:chat.id,trace:'ai-job',direction:'in',sender:'测试用户',created:1,status:'received',body:[{type:'text',data:{text:'原始问题'}},{type:'image',data:{url:'https://example.invalid/manual-only.png'}}]};
const job={trace:'ai-job',chat:chat.id,title:'测试用户',prompt:'原始问题',status:'running',started:1};
let logRows=[];const calls=[];const bridge={ready:async()=>({}),apiGet:async(path,data)=>{calls.push([path,data]);if(path==='state')return {retention_days:14,dropped:0};if(path==='chats')return data.id?[chat]:(data.q?[]:[chat]);if(path==='jobs')return [job];if(path==='messages')return data.trace?[source]:[];if(path==='logs')return logRows;if(path==='review')return {items:[],instructions:[]};throw Error(path);},apiPost:async(path,data)=>{calls.push([path,data]);if(path==='stop'){job.status='cancelled';return {stopped:true};}throw Error(path);}};
const sandbox={document:{getElementById:get,createElement:tag=>{const el=new Element();el.tagName=tag;el.replaceWith=()=>{};created.push(el);return el;},hidden:false,addEventListener(){}},window:{AstrBotPluginPage:bridge},setTimeout:()=>0,clearTimeout(){},console,Map,Set,Date,Promise,crypto:{randomUUID:()=> 'id'}};
const context=vm.createContext(sandbox);vm.runInContext(fs.readFileSync(process.argv[2],'utf8'),context);
(async()=>{await new Promise(setImmediate);await vm.runInContext('jobs()',context);get('chatSearch').value='不匹配的筛选';get('jobs').value='ai-job';await get('jobs').onchange();assert.equal(get('title').textContent,'测试用户');assert.match(get('route').textContent,/来源群 100/);assert.equal(get('chatSearch').value,'');assert.equal(get('trace').textContent,'追踪 ai-job');assert.equal(vm.runInContext('state.messages[0].id',context),1);assert.equal(vm.runInContext('state.chat.id',context),chat.id);assert(calls.some(([path,data])=>path==='chats'&&data.id===chat.id));assert(get('title').scrolled);assert(created.some(el=>el.tagName==='time'&&el.dateTime));assert(created.some(el=>el.textContent==='测试发送人'));assert.equal(created.filter(el=>el.tagName==='img').length,0);const load=created.find(el=>el.textContent==='查看图片（按需加载）');assert(load);load.onclick({stopPropagation(){}});assert.equal(created.filter(el=>el.tagName==='img').length,1);const currentElements=()=>{const flatten=el=>[el,...el.children.flatMap(flatten)];return flatten(get('messages'));};
assert(currentElements().some(el=>el.textContent==='处理中 · 等待阶段通知'));
job.step='tool';job.step_detail='search_web';job.step_started=10;
await vm.runInContext('jobs()',context);
assert(currentElements().some(el=>el.textContent==='处理中 · 执行工具：search_web'));
assert.match(get('jobInfo').textContent,/当前步骤：执行工具：search_web/);
assert.match(get('jobInfo').textContent,/步骤开始：/);
assert(get('jobs').children.some(el=>el.textContent.includes('执行工具：search_web')));
job.step='model';job.step_detail='';job.step_started=20;
await vm.runInContext('jobs()',context);
assert(currentElements().some(el=>el.textContent==='处理中 · 等待模型回复'));
assert(!currentElements().some(el=>el.textContent==='处理中 · 执行工具：search_web'));
const inlineStop=currentElements().find(el=>el.className==='stop-message');assert(inlineStop);await inlineStop.onclick();assert(calls.some(([path,data])=>path==='stop'&&data.trace==='ai-job'));
const flatten=el=>[el,...el.children.flatMap(flatten)];assert(!flatten(get('messages')).some(el=>el.className==='message-progress'));
const logPanel=get('logs');logPanel.scrollHeight=1000;logPanel.clientHeight=300;Object.defineProperty(logPanel,'scrollTop',{get(){return this._top||0;},set(value){this._top=Math.max(0,Math.min(value,this.scrollHeight-this.clientHeight));}});
logRows=[{id:1,created:1,level:'INFO',source:'test',body:'first'}];vm.runInContext('state.logScope=null;state.logSignature=null',context);await vm.runInContext('logs()',context);assert.equal(logPanel.scrollTop,700);
logPanel.scrollTop=100;logPanel.onscroll();logRows.push({id:2,created:2,level:'INFO',source:'test',body:'second'});await vm.runInContext('logs()',context);assert.equal(logPanel.scrollTop,100);assert.equal(vm.runInContext('state.logFollow',context),false);
logPanel.scrollTop=700;logPanel.onscroll();logRows.push({id:3,created:3,level:'INFO',source:'test',body:'third'});await vm.runInContext('logs()',context);assert.equal(logPanel.scrollTop,700);assert.equal(vm.runInContext('state.logFollow',context),true);
assert.match(get('jobInfo').textContent,/最后步骤：等待模型回复/);
const messagesPanel=get('messages');messagesPanel.scrollHeight=2000;messagesPanel.scrollTop=100;
await vm.runInContext('selectChat(state.chat)',context);assert.equal(messagesPanel.scrollTop,2000);
job.trace='background-job';job.source_message_id=source.id;job.status='running';source.related_traces=['background-job'];
await vm.runInContext('jobs()',context);get('jobs').value='background-job';await get('jobs').onchange();
assert(currentElements().some(el=>el.className==='bubble selected'));
const linkedStop=currentElements().find(el=>el.className==='stop-message');assert(linkedStop);await linkedStop.onclick();
assert(calls.some(([path,data])=>path==='stop'&&data.trace==='background-job'));
assert(!currentElements().some(el=>el.className==='message-progress'));
vm.runInContext('state.messages=[{...state.messages[0],body:[]}];renderMessages()',context);
assert(currentElements().some(el=>el.textContent==='[未采集到消息内容]'));
console.log('Background task navigation, original-message highlighting, correct interruption target and empty-content fallback passed');
console.log('Live processing phases, tool names, fallback, cancellation and chat opening scroll passed');
console.log('Inline interruption and pause/resume log following passed');console.log('Task navigation, exact temporary-chat routing, search reset and original-message loading passed');})().catch(error=>{console.error(error);process.exitCode=1;});



