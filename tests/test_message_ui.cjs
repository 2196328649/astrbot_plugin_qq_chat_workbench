const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');

class Element {
  constructor(tag = 'div') {
    this.tagName = tag; this.value = ''; this.children = []; this.style = {}; this.parent = null;
    this.hidden = false; this.scrollHeight = 0; this.scrollTop = 0; this.clientHeight = 300;
  }
  setAttribute(name, value) { this[name] = value; }
  append(...items) { for (const item of items) { if (item instanceof Element) item.parent = this; this.children.push(item); } }
  replaceChildren(...items) { this.children = []; this.append(...items); }
  replaceWith(other) { const list = this.parent.children; other.parent = this.parent; list[list.indexOf(this)] = other; this.parent = null; }
  get firstChild() { return this.children[0]; }
  scrollIntoView() {} querySelector() { return null; } focus() {}
  contains(target) { return this === target || this.children.some(child => child.contains?.(target)); }
}
const elements = new Map();
const get = id => { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); };
get('composerMode').value = 'direct'; get('replyMode').value = 'confirm';
const chat = {id:'["qq","999","private","200",""]',title:'测试用户',target:'200',account:'999',kind:'private',group_id:''};
const other = {id:'["qq","999","private","201",""]',title:'另一位用户',target:'201',account:'999',kind:'private',group_id:''};
const photo = {id:1,chat:chat.id,trace:'photo',direction:'in',sender:'测试用户',sender_id:'200',created:1,status:'received',
  body:[{type:'image',data:{url:'https://example.invalid/a.jpg'}},{type:'file',data:{name:'a.txt',url:'https://example.invalid/a.txt'}}]};
const own = {trace:'photo',chat:chat.id,title:'测试用户',prompt:'看图',status:'running',step:'model',started:1,source_message_id:1,sender_id:'200',can_append:true,pending_count:0};
const jobs = [own]; const sent = []; const outcomes = [];
const bridge = {
  ready: async () => ({}),
  apiGet: async (path, data) => {
    if (path === 'state') return {retention_days:14,dropped:0};
    if (path === 'chats') return [chat, other];
    if (path === 'jobs') return jobs;
    if (path === 'messages') return data.chat === chat.id ? [photo] : [];
    if (path === 'logs') return [];
    if (path === 'review') return {items:[],instructions:[]};
    throw Error(path);
  },
  apiPost: async (path, data) => {
    assert.equal(path, 'send'); sent.push(JSON.parse(JSON.stringify(data)));
    const outcome = outcomes.shift() || {trace:'manual',status:'accepted'};
    // The dashboard host rejects a failed plugin request with its message only.
    if (outcome instanceof Error) throw outcome;
    return outcome;
  }
};
const context = vm.createContext({
  document:{getElementById:get,createElement:tag=>new Element(tag),hidden:false,addEventListener(){}},
  window:{AstrBotPluginPage:bridge,innerWidth:1280,innerHeight:900},
  setTimeout:()=>0,clearTimeout(){},console,Map,Set,Date,Promise,
  crypto:{randomUUID:()=>require('node:crypto').randomUUID()}
});
vm.runInContext(fs.readFileSync(process.argv[2], 'utf8'), context);
const flatten = element => [element, ...(element.children || []).flatMap(flatten)];
const all = predicate => flatten(get('messages')).filter(el => el && predicate(el));
const submit = () => get('composer').onsubmit({preventDefault(){}});

(async () => {
  await new Promise(setImmediate);
  await vm.runInContext('jobs()', context);
  await vm.runInContext(`selectChat(${JSON.stringify(chat)})`, context);
  all(el => el.className === 'load-image')[0].onclick({stopPropagation(){}});
  const details = all(el => el.className === 'attachment-details')[0];
  details.open = true; details.ontoggle?.();
  const panel = get('messages').children;

  // Another conversation's task progress must not rebuild this conversation.
  jobs.push({...own, trace:'elsewhere', chat:other.id, source_message_id:9});
  await vm.runInContext('jobs()', context);
  jobs.at(-1).step = 'tool'; await vm.runInContext('jobs()', context);
  assert.equal(get('messages').children, panel);

  // This conversation's own progress redraws, keeping what was opened.
  own.step = 'tool'; await vm.runInContext('jobs()', context);
  assert.notEqual(get('messages').children, panel);
  assert.equal(all(el => el.className === 'load-image').length, 0);
  assert.equal(all(el => el.tagName === 'img' && el.src === 'https://example.invalid/a.jpg').length, 1);
  assert.equal(all(el => el.className === 'attachment-details')[0].open, true);
  assert.match(all(el => el.className === 'processing-label')[0].textContent, /执行工具/);
  const failed = all(el => el.tagName === 'img')[0]; failed.onerror();
  own.step = 'response'; await vm.runInContext('jobs()', context);
  assert.equal(all(el => el.className === 'load-image').length, 1);
  all(el => el.className === 'load-image')[0].onclick({stopPropagation(){}});

  // Opened items belong to this conversation only.
  await vm.runInContext(`selectChat(${JSON.stringify(other)})`, context);
  await vm.runInContext(`selectChat(${JSON.stringify(chat)})`, context);
  assert.equal(all(el => el.className === 'load-image').length, 1);
  assert.equal(all(el => el.className === 'attachment-details')[0].open, false);

  // SnowLuma reports a send that may already have reached QQ as failed too,
  // so any failure keeps the text and its request ID: a retry cannot send twice.
  get('composerMode').value = 'direct'; get('text').value = '结果未知';
  outcomes.push(new Error('发送失败或结果未知，请先检查聊天记录，勿重复发送'));
  await submit();
  assert.equal(get('text').value, '结果未知');
  assert.match(get('notice').textContent, /勿重复发送；重试会保留本次请求 ID/);
  outcomes.push(new Error('发送失败或结果未知，请先检查聊天记录，勿重复发送'));
  await submit();
  assert.equal(sent[1].request_id, sent[0].request_id);
  assert.equal(get('text').value, '结果未知');
  // Editing the text after checking the chat makes it a new request.
  get('text').value = '改过的消息'; get('text').oninput();
  await submit();
  assert.notEqual(sent[2].request_id, sent[0].request_id);
  assert.equal(sent[2].text, '改过的消息'); assert.equal(get('text').value, '');
  console.log('Per-conversation redraw, kept images and attachments, and failed-send retries passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
