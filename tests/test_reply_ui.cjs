const fs = require('node:fs');
const path = require('node:path');
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
const chat = {id:'["qq","999","group","100",""]',title:'测试群',target:'100',account:'999',kind:'group',group_id:''};
const source = {id:7,chat:chat.id,trace:'ingress',direction:'in',sender:'群友',sender_id:'300',remote_id:'456',created:1,status:'received',body:[{type:'text',data:{text:'有人知道这个吗'}}]};
const own = {id:8,chat:chat.id,trace:'manual',direction:'out',sender:'机器人',sender_id:'',remote_id:'457',created:2,status:'sent',body:[{type:'text',data:{text:'机器人消息'}}]};
const jobs = []; const calls = []; let counter = 0; let rejectNext = false;
const bridge = {
  ready: async () => ({}),
  apiGet: async (route, data) => {
    if (route === 'state') return {retention_days:14,dropped:0};
    if (route === 'chats') return [chat].filter(row => !data.id || data.id === row.id);
    if (route === 'jobs') return jobs;
    if (route === 'messages') return data.chat === chat.id ? [source, own] : [];
    if (route === 'logs') return [];
    if (route === 'review') return {items:[],instructions:[]};
    throw Error(route);
  },
  apiPost: async (route, data) => {
    calls.push([route, JSON.parse(JSON.stringify(data))]);
    if (rejectNext) { rejectNext = false; throw Error('Temporary network error'); }
    if (route === 'guide') {
      const trace = 'reply-' + (++counter);
      jobs.push({trace,chat:chat.id,title:'测试群',prompt:'有人知道这个吗',status:'running',step:'model',started:3,source_message_id:7,sender_id:'300',reply_mode:data.reply_mode,can_append:true,pending_count:0});
      return {trace, mode:data.mode, reply_mode:data.reply_mode};
    }
    throw Error(route);
  }
};
const context = vm.createContext({
  document:{getElementById:get,createElement:tag=>new Element(tag),hidden:false,addEventListener(){}},
  window:{AstrBotPluginPage:bridge,innerWidth:1280,innerHeight:900},
  setTimeout:()=>0,clearTimeout(){},console,Map,Set,Date,Promise,
  crypto:{randomUUID:()=>require('node:crypto').randomUUID()}
});
vm.runInContext(fs.readFileSync(process.argv[2], 'utf8'), context);
const html = fs.readFileSync(path.join(path.dirname(process.argv[2]), 'index.html'), 'utf8');
const submit = () => get('composer').onsubmit({preventDefault(){}});
const flatten = element => [element, ...element.children.flatMap(flatten)];
const guides = () => calls.filter(([route]) => route === 'guide').map(([, data]) => data);
const aiButtons = () => flatten(get('messages')).filter(el => el.textContent === 'AI 回复');

(async () => {
  await new Promise(setImmediate);
  await vm.runInContext(`selectChat(${JSON.stringify(chat)})`, context);
  assert.match(html, /<button id="menuAiReply" role="menuitem">让 AI 回复这条消息<\/button>/);
  assert.match(html, /<option value="reply">让 AI 回复所选消息<\/option>/);
  // Only received messages can be answered, from the inline action or the menu.
  assert.equal(aiButtons().length, 1);
  const article = flatten(get('messages')).find(el => el.tagName === 'article' && el.oncontextmenu);
  article.oncontextmenu({preventDefault(){},clientX:20,clientY:30});
  get('menuAiReply').onclick();
  assert.equal(get('messageMenu').hidden, true);
  assert.equal(get('composerMode').value, 'reply');
  assert.equal(get('reference').hidden, false);
  assert.match(get('referenceTitle').textContent, /AI 将回复这条消息 · 群友/);
  assert.equal(get('referenceText').textContent, '有人知道这个吗');
  assert.match(get('composerLabel').textContent, /可不填/);
  assert.match(get('guidanceHint').textContent, /引用原消息/);
  assert.equal(get('send').textContent, '让 AI 回复');
  assert.equal(get('replyModeLabel').hidden, false);
  assert.equal(get('imageLabel').hidden, true);

  // No guidance is needed; one click starts exactly one task.
  await submit();
  assert.equal(guides().length, 1);
  const first = guides()[0];
  assert.match(first.request_id, /^[0-9a-f-]{36}$/);
  assert.deepEqual(first, {chat:chat.id,guidance:'',mode:'reply',reply_mode:'confirm',request_id:first.request_id,reference_kind:'message',message_id:7});
  assert(!calls.some(([route]) => route === 'send'));
  assert.match(get('notice').textContent, /已让 AI 回复这条消息，生成后等待你确认/);
  assert.equal(get('jobs').value, 'reply-1');
  // The selection is cleared, so an empty re-click cannot start a second paid task.
  assert.equal(get('composerMode').value, 'direct');
  assert.equal(get('reference').hidden, true);
  await submit();
  assert.equal(calls.length, 1);

  // A failed request keeps its ID for the retry and stays on the same message.
  aiButtons()[0].onclick();
  assert.equal(get('composerMode').value, 'reply');
  get('replyMode').value = 'auto'; get('replyMode').onchange();
  assert.match(get('sendHint').textContent, /自动发送/);
  get('text').value = '语气轻松一点'; get('text').oninput();
  rejectNext = true; await submit();
  assert.match(get('notice').textContent, /重试会保留本次请求 ID/);
  assert.equal(get('composerMode').value, 'reply');
  assert.equal(get('reference').hidden, false);
  const retryId = guides()[1].request_id;
  assert.notEqual(retryId, first.request_id);
  await submit();
  assert.deepEqual(guides()[2], {chat:chat.id,guidance:'语气轻松一点',mode:'reply',reply_mode:'auto',request_id:retryId,reference_kind:'message',message_id:7});
  assert.match(get('notice').textContent, /生成后自动发送/);
  assert.equal(get('composerMode').value, 'direct');
  assert.equal(get('text').value, '');

  // Switching to reply mode needs a selected message.
  get('composerMode').value = 'reply'; get('composerMode').onchange();
  assert.equal(get('reference').hidden, true);
  await submit();
  assert.equal(guides().length, 3);
  assert.match(get('notice').textContent, /请先右键或点击消息旁的“AI 回复”/);

  // A user selection made from a message becomes a reply to that message.
  article.oncontextmenu({preventDefault(){},clientX:20,clientY:30});
  get('menuGuideUser').onclick();
  assert.equal(get('composerMode').value, 'new');
  assert.match(get('referenceTitle').textContent, /选择的用户/);
  get('composerMode').value = 'reply'; get('composerMode').onchange();
  assert.match(get('referenceTitle').textContent, /AI 将回复这条消息/);
  await submit();
  assert.equal(guides().length, 4);
  assert.equal(guides()[3].reference_kind, 'message'); assert.equal(guides()[3].message_id, 7);
  assert.equal(guides()[3].mode, 'reply');

  // A pending image belongs to a direct send and blocks AI reply mode.
  vm.runInContext("state.image='aGVsbG8='", context);
  get('composerMode').value = 'reply'; get('composerMode').onchange();
  assert.equal(get('composerMode').value, 'direct');
  assert.match(get('notice').textContent, /请先移除待发送图片/);
  assert.equal(guides().length, 4);
  console.log('AI reply entry points, empty guidance, one task per click, retries, conversion and guards passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
