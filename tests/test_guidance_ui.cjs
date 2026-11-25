const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');

class Element {
  constructor(tag = 'div') {
    this.tagName = tag; this.value = ''; this.children = []; this.style = {};
    this.hidden = false; this.scrollHeight = 0; this.scrollTop = 0; this.clientHeight = 300;
  }
  setAttribute(name, value) { this[name] = value; }
  append(...items) { this.children.push(...items); }
  replaceChildren(...items) { this.children = items; }
  get firstChild() { return this.children[0]; }
  scrollIntoView() { this.scrolled = true; }
  querySelector() { return null; }
  focus() { this.focused = true; }
  contains(target) { return this === target || this.children.some(child => child.contains(target)); }
}
const elements = new Map();
const get = id => { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); };
get('composerMode').value = 'direct'; get('replyMode').value = 'confirm';
const chat = {id:'["qq","999","temporary","200","100"]',title:'测试用户',target:'200',account:'999',kind:'temporary',group_id:'100'};
const other = {id:'["qq","999","private","201",""]',title:'另一位用户',target:'201',account:'999',kind:'private',group_id:''};
const source = {id:1,chat:chat.id,trace:'live',direction:'in',sender:'测试用户',sender_id:'200',created:1,status:'received',body:[{type:'text',data:{text:'用户原话'}}]};
const live = {trace:'live',chat:chat.id,title:'测试用户',prompt:'用户原话',status:'running',step:'model',started:1,source_message_id:1,sender_id:'200',can_append:true,pending_count:0};
const jobs = [live]; const calls = []; const reviews = new Map(); let counter = 0; let rejectNext = false;
const bridge = {
  ready: async () => ({}),
  apiGet: async (path, data) => {
    if (path === 'state') return {retention_days:14,dropped:0};
    if (path === 'chats') return [chat,other].filter(row => !data.id || data.id === row.id);
    if (path === 'jobs') return jobs;
    if (path === 'messages') return data.chat === chat.id ? [source] : [];
    if (path === 'logs') return [];
    if (path === 'review') return reviews.get(data.trace) || {items:[],instructions:[]};
    throw Error(path);
  },
  apiPost: async (path, data) => {
    calls.push([path, JSON.parse(JSON.stringify(data))]);
    if (rejectNext) { rejectNext = false; throw Error('Temporary network error'); }
    if (path === 'guide') {
      if (data.mode === 'append') return {trace:data.trace};
      const trace = 'guided-' + (++counter);
      jobs.push({...live, trace, status:'awaiting_approval', reply_mode:data.reply_mode, can_append:false,pending_count:1});
      reviews.set(trace, {items:[{id:counter,trace,status:'pending',created:2,body:[{type:'text',data:{text:'AI 生成的回复'}}]}],instructions:[{guidance:data.guidance,quoted_text:'用户原话',sender:'测试用户',reference_kind:data.reference_kind}]});
      return {trace};
    }
    if (path === 'send') return {trace:'manual',status:'accepted'};
    if (path === 'approve' || path === 'discard') {
      for (const item of reviews.get(data.trace).items) item.status = path === 'approve' ? 'accepted' : 'discarded';
      const job = jobs.find(row => row.trace === data.trace); job.status = 'completed'; job.pending_count = 0;
      return {trace:data.trace,status:'approved'};
    }
    if (path === 'stop') return {stopped:true};
    throw Error(path);
  }
};
const context = vm.createContext({
  document:{getElementById:get,createElement:tag=>new Element(tag),hidden:false,addEventListener(){}},
  window:{AstrBotPluginPage:bridge,innerWidth:1280,innerHeight:900},
  setTimeout:()=>0,clearTimeout(){},console,Map,Set,Date,Promise,
  crypto:{randomUUID:()=>require('node:crypto').randomUUID()}
});
vm.runInContext(fs.readFileSync(process.argv[2], 'utf8'), context);
const submit = () => get('composer').onsubmit({preventDefault(){}});
const flatten = element => [element, ...element.children.flatMap(flatten)];

(async () => {
  await new Promise(setImmediate);
  await vm.runInContext('jobs()', context);
  get('jobs').value = 'live'; await get('jobs').onchange();
  assert.match(get('route').textContent, /来源群 100/);
  flatten(get('messages')).find(el => el.textContent === '添加引导').onclick();
  assert.equal(get('composerMode').value, 'new');
  assert.match(get('referenceTitle').textContent, /引用的用户消息/);
  assert.equal(get('referenceText').textContent, '用户原话');
  assert.match(get('composerLabel').textContent, /仅供 AI/);
  get('text').value = '管理员引导'; await submit();
  const created = calls.find(([path]) => path === 'guide')[1];
  assert.equal(created.guidance, '管理员引导'); assert.equal(created.message_id, 1);
  assert.equal(created.reference_kind, 'message'); assert.equal(created.reply_mode, 'confirm');
  assert(!('text' in created)); assert(!calls.some(([path]) => path === 'send'));
  assert.equal(get('reviewPanel').hidden, false);
  assert(flatten(get('reviewBody')).some(el => el.textContent === 'AI 生成的回复'));
  assert(!flatten(get('reviewBody')).some(el => el.textContent === '管理员引导'));
  assert.match(get('reviewTarget').textContent, /来源群 100/);
  await get('approveReply').onclick(); await get('approveReply').onclick();
  assert.equal(calls.filter(([path]) => path === 'approve').length, 1);
  assert.equal(get('approveReply').hidden, true);

  const article = flatten(get('messages')).find(el => el.tagName === 'article');
  article.oncontextmenu({preventDefault(){},clientX:20,clientY:30});
  assert.equal(get('messageMenu').hidden, false);
  get('menuGuideUser').onclick();
  assert.match(get('referenceTitle').textContent, /选择的用户/);
  get('composerMode').value = 'append'; get('composerMode').onchange();
  get('text').value = '补充引导'; get('replyMode').value = 'auto';
  rejectNext = true; await submit();
  const retryId = calls.at(-1)[1].request_id;
  await submit();
  assert.equal(calls.at(-1)[1].request_id, retryId);
  assert.equal(calls.at(-1)[1].trace, 'live');
  assert.equal(calls.at(-1)[1].mode, 'append');
  assert.equal(calls.at(-1)[1].reference_kind, 'user');
  assert.equal(calls.at(-1)[1].reply_mode, 'auto');
  assert(!calls.some(([path]) => path === 'send'));

  article.oncontextmenu({preventDefault(){},clientX:20,clientY:30}); get('menuReply').onclick();
  get('text').value = '手动原样发送'; await submit();
  const sent = calls.find(([path]) => path === 'send')[1];
  assert.equal(sent.text, '手动原样发送'); assert.equal(sent.reference_id, 1);
  assert(!('guidance' in sent)); assert.equal(sent.chat, chat.id);

  get('guideUser').onclick(); get('text').value = '不应带到其他用户的引导';
  get('jobs').value = '';
  await vm.runInContext(`selectChat(${JSON.stringify(other)})`, context);
  assert.equal(get('composerMode').value, 'direct');
  assert.equal(get('reference').hidden, true); assert.equal(get('text').value, '');
  get('composerMode').value = 'append'; get('composerMode').onchange();
  assert.equal(get('appendTask').value, ''); assert.equal(get('send').disabled, true);
  console.log('Reference identity, guidance-only payloads, append/new modes, review, retries, direct replies and conversation isolation passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
