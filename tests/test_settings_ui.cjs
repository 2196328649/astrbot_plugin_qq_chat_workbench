const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const assert = require('node:assert/strict');

class Element {
  constructor(tag = 'div') {
    this.tagName = tag; this.value = ''; this.children = []; this.style = {}; this.parent = null;
    this.hidden = false; this.scrollHeight = 0; this.scrollTop = 0; this.clientHeight = 300;
  }
  setAttribute(name, value) { this[name] = String(value); }
  append(...items) { for (const item of items) { if (item instanceof Element) item.parent = this; this.children.push(item); } }
  replaceChildren(...items) { this.children = []; this.append(...items); }
  replaceWith(other) { const list = this.parent.children; other.parent = this.parent; list[list.indexOf(this)] = other; this.parent = null; }
  remove() { const list = this.parent.children; list.splice(list.indexOf(this), 1); this.parent = null; }
  get firstChild() { return this.children[0]; }
  scrollIntoView() {} querySelector() { return null; } focus() {} select() {}
  showModal() { this.open = true; } close() { this.open = false; }
  requestSubmit() { this.submitted = this.onsubmit({preventDefault(){}}); return this.submitted; }
  contains(target) { return this === target || this.children.some(child => child.contains?.(target)); }
}
const elements = new Map();
const get = id => { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); };
get('composerMode').value = 'direct'; get('replyMode').value = 'confirm';
const chat = {id:'["qq","999999","private","123456",""]',title:'小明',target:'123456',account:'999999',kind:'private',group_id:'',last_sender:'小明',last_time:1700000000,preview:'[图片]'};
const group = {id:'["qq","999999","group","7654321",""]',title:'测试群',target:'7654321',account:'999999',kind:'group',group_id:'',last_sender:'群友',last_time:1700000000,preview:'你好'};
const photo = {id:1,chat:chat.id,trace:'photo',direction:'in',sender:'小明',sender_id:'123456',created:1700000000,status:'received',
  body:[{type:'image',data:{url:'https://example.invalid/a.jpg'}},{type:'file',data:{name:'a.txt',url:'https://example.invalid/a.txt'}}]};
const own = {id:2,chat:chat.id,trace:'manual',direction:'out',sender:'机器人',sender_id:'',remote_id:'9',created:1700000001,status:'accepted',body:[{type:'text',data:{text:'你好'}}]};
const ALLOWED = {auto_images:[false,true],avatars:[false,true],send_key:['ctrl_enter','enter'],reply_mode:['confirm','auto'],message_actions:['always','hover'],image_save:['manual','viewed','all'],image_quota:[1024,512,2048,4096]};
const saved = {auto_images:true,avatars:true,send_key:'enter',reply_mode:'auto',message_actions:'hover',image_save:'viewed',image_quota:2048};
let settingsGetFails = 1; let failNextSave = false; let stateFails = false; let dropped = 0;
const gets = []; const posts = []; const sent = [];
const bridge = {
  ready: async () => ({}),
  apiGet: async (route, data) => {
    gets.push([route, data]);
    if (route === 'settings') { if (settingsGetFails) { settingsGetFails--; throw Error('Temporary network error'); } return {...saved}; }
    if (route === 'state') { if (stateFails) throw Error('Network down'); return {version:'0.2.9',retention_days:14,dropped,images:{mode:saved.image_save,quota_mb:saved.image_quota,files:3,bytes:1572864,queued:0,skipped:0}}; }
    if (route === 'chats') return [chat, group].filter(row => !data.id || data.id === row.id);
    if (route === 'jobs') return [];
    if (route === 'messages') return data.chat === chat.id ? [photo, own] : [];
    if (route === 'logs') return [];
    if (route === 'review') return {items:[],instructions:[]};
    throw Error(route);
  },
  apiPost: async (route, data) => {
    if (route === 'settings') {
      posts.push(JSON.parse(JSON.stringify(data)));
      if (failNextSave) { failNextSave = false; throw Error('管理插件正在重载，请稍后重试'); }
      for (const [key, value] of Object.entries(data)) assert(ALLOWED[key].includes(value), key);
      Object.assign(saved, data); return {...saved};
    }
    if (route === 'send') { sent.push(JSON.parse(JSON.stringify(data))); return {trace:'manual-' + sent.length, status:'accepted'}; }
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
const pageDir = path.dirname(process.argv[2]);
const html = fs.readFileSync(path.join(pageDir, 'index.html'), 'utf8');
const css = fs.readFileSync(path.join(pageDir, 'style.css'), 'utf8');
const run = code => vm.runInContext(code, context);
const flatten = element => [element, ...(element.children || []).flatMap(flatten)];
const inside = (id, predicate) => flatten(get(id)).filter(el => el && predicate(el));
const chatImages = () => inside('messages', el => el.tagName === 'img' && el.src === 'https://example.invalid/a.jpg');
const key = props => {
  const event = {key:'Enter', isComposing:false, keyCode:13, ...props, prevented:false, preventDefault() { event.prevented = true; }};
  get('composer').submitted = undefined; get('text').onkeydown(event); return event;
};

(async () => {
  await new Promise(setImmediate);
  // The page: no header row, no refresh button; settings and log scope exist.
  assert(!/<header/.test(html)); assert(!html.includes('会话、任务引导与回复确认'));
  assert(!html.includes('id="refresh"')); assert(!html.includes('id="system"'));
  for (const id of ['openSettings','settings','settingAutoImages','settingAvatars','settingSendKey','settingReplyMode','settingMessageActions','settingImageSave','settingImageQuota','imageUsage','viewerDownload','viewerClose','logAll','logSelected','health','viewer'])
    assert(html.includes(`id="${id}"`), id);
  // Dark colours follow AstrBot's theme, not the operating system.
  assert(!css.includes('prefers-color-scheme')); assert(css.includes(':where([data-theme=dark])'));
  // Narrow screens: images fit the bubble, action buttons wrap whole, addresses shrink.
  assert(/@media\(max-width:640px\)\{[^@]*\.bubble img\{max-width:100%\}/.test(css));
  assert(/\.message-actions\{[^}]*flex-wrap:wrap/.test(css));
  assert(/\.attachment-link input\{[^}]*min-width:0/.test(css));

  // A failed settings request keeps the defaults; the next round loads them.
  assert.equal(run('state.settingsLoaded'), false);
  assert.equal(inside('chats', el => el.className === 'avatar').length, 0);
  assert.equal(get('health').textContent, '已连接'); assert.equal(get('health').className, 'health ok');
  await run('poll()');
  assert.equal(run('state.settingsLoaded'), true);
  assert.equal(gets.filter(([route]) => route === 'settings').length, 2);
  await run('poll()');
  assert.equal(gets.filter(([route]) => route === 'settings').length, 2);
  assert.equal(get('settingAutoImages').checked, true); assert.equal(get('settingAvatars').checked, true);
  assert.equal(get('settingSendKey').value, 'enter'); assert.equal(get('settingReplyMode').value, 'auto');
  assert.equal(get('settingMessageActions').value, 'hover'); assert.equal(get('app')['data-actions'], 'hover');
  assert.equal(get('replyMode').value, 'auto');
  assert.match(get('about').textContent, /0\.2\.9/);
  // Image saving: three modes and a size limit, with the current usage beside them.
  assert.equal(get('settingImageSave').value, 'viewed'); assert.equal(get('settingImageQuota').value, '2048');
  assert.equal(get('imageUsage').textContent, '已保存 3 张，占用 1.5 MB（上限 2 GB）');
  for (const text of ['手动保存', '自动保存看过的图片', '自动保存全部图片（防撤回）']) assert(html.includes(text), text);

  // QQ avatars in the conversation list, with the first character underneath.
  const avatars = inside('chats', el => el.className === 'avatar');
  assert.equal(avatars.length, 2);
  assert.deepEqual(avatars.map(box => box.children.find(el => el.className === 'avatar-initial').textContent), ['小', '测']);
  assert.deepEqual(avatars.map(box => box.children.find(el => el.tagName === 'img').src),
    ['https://q1.qlogo.cn/g?b=qq&nk=123456&s=100', 'https://p.qlogo.cn/gh/7654321/7654321/100']);
  assert.equal(run('avatarUrl("12345abc")'), ''); assert.equal(run('avatarUrl("0123456")'), '');

  // A newly opened conversation uses the default AI reply mode.
  get('replyMode').value = 'confirm';
  await run(`selectChat(${JSON.stringify(chat)})`);
  assert.equal(get('replyMode').value, 'auto');
  const rows = inside('messages', el => el.tagName === 'article');
  assert.deepEqual(rows.map(row => row.className), ['message in with-avatar', 'message out with-avatar']);
  assert.deepEqual(inside('messages', el => el.tagName === 'img' && el.src.startsWith('https://q1.qlogo.cn')).map(img => img.src),
    ['https://q1.qlogo.cn/g?b=qq&nk=123456&s=100', 'https://q1.qlogo.cn/g?b=qq&nk=999999&s=100']);
  const meta = inside('messages', el => el.className === 'meta')[0];
  assert.equal(meta.textContent, '小明 · ' + run('shortTime(1700000000)')); assert.equal(meta.title, run('time(1700000000)'));
  // "已接收" is the normal state of an incoming message and is not shown.
  assert.deepEqual(inside('messages', el => /^status /.test(el.className)).map(el => el.textContent), ['接口已受理']);
  // Attachment addresses are copied: the sandbox would block a new window.
  const link = inside('messages', el => el.className === 'attachment-link')[0];
  assert.equal(link.children[0].value, 'https://example.invalid/a.txt'); assert.equal(link.children[0].readOnly, true);
  assert.equal(inside('messages', el => el.tagName === 'a').length, 0);

  // Images load at once; an expired one falls back to the button, not a reload loop.
  assert.equal(chatImages().length, 1); assert.equal(inside('messages', el => el.className === 'load-image').length, 0);
  chatImages()[0].onclick({stopPropagation(){}});
  assert.equal(get('viewer').hidden, false); assert.equal(get('viewerImage').src, 'https://example.invalid/a.jpg');
  get('viewer').onclick(); assert.equal(get('viewer').hidden, true);
  chatImages()[0].onerror();
  assert.equal(inside('messages', el => el.textContent === '[图片已过期或无法预览]').length, 1);
  run('renderMessages()');
  assert.equal(chatImages().length, 0);
  assert.equal(inside('messages', el => el.className === 'load-image')[0].textContent, '图片加载失败，点击重试');
  inside('messages', el => el.className === 'load-image')[0].onclick({stopPropagation(){}});
  assert.equal(chatImages().length, 1); assert.equal(run('state.failedImages.size'), 0);
  // A broken avatar leaves the first character.
  const box = inside('messages', el => el.className === 'avatar')[0];
  box.children.find(el => el.tagName === 'img').onerror();
  assert.deepEqual(box.children.map(el => el.className), ['avatar-initial']);
  // The failed address is not requested again when the list is redrawn.
  run('state.chatSignature=null;renderChats()');
  assert.deepEqual(inside('chats', el => el.className === 'avatar').map(item => item.children.some(el => el.tagName === 'img')), [false, true]);

  // Enter sends; Shift + Enter and input-method composition do not.
  get('text').value = '你好呀';
  assert.equal(key({shiftKey:true}).prevented, false); assert.equal(key({isComposing:true}).prevented, false);
  assert.equal(key({keyCode:229}).prevented, false); assert.equal(get('composer').submitted, undefined);
  assert.equal(key({}).prevented, true); await get('composer').submitted;
  assert.equal(sent.length, 1); assert.equal(sent[0].text, '你好呀');
  assert.equal(get('sendHint').textContent, 'Enter 发送，Shift + Enter 换行');
  assert.match(get('text').placeholder, /Enter 发送/);

  // Each change saves only its own key; a failed save restores the control.
  get('openSettings').onclick(); assert.equal(get('settings').open, true);
  get('settingSendKey').value = 'ctrl_enter'; await get('settingSendKey').onchange();
  assert.deepEqual(posts.at(-1), {send_key:'ctrl_enter'}); assert.equal(get('settingsStatus').textContent, '已保存');
  assert.equal(get('sendHint').textContent, 'Ctrl / ⌘ + Enter 发送');
  get('text').value = '第二条';
  assert.equal(key({}).prevented, false); assert.equal(get('composer').submitted, undefined);
  assert.equal(key({ctrlKey:true}).prevented, true); await get('composer').submitted;
  assert.equal(sent.length, 2); assert.equal(sent[1].text, '第二条');
  failNextSave = true; get('settingAvatars').checked = false; await get('settingAvatars').onchange();
  assert.deepEqual(posts.at(-1), {avatars:false});
  assert.equal(get('settingAvatars').checked, true); assert.equal(run('state.settings.avatars'), true);
  assert.equal(get('settingsStatus').className, 'error'); assert.match(get('settingsStatus').textContent, /保存失败：管理插件正在重载/);
  assert.equal(inside('chats', el => el.className === 'avatar').length, 2);
  get('settingAvatars').checked = false; await get('settingAvatars').onchange();
  assert.equal(inside('chats', el => el.className === 'avatar').length, 0);
  assert.equal(inside('messages', el => el.className === 'avatar').length, 0);
  assert.deepEqual(inside('messages', el => el.tagName === 'article').map(row => row.className), ['message in', 'message out']);
  get('settingAutoImages').checked = false; await get('settingAutoImages').onchange();
  await run(`selectChat(${JSON.stringify(group)})`); await run(`selectChat(${JSON.stringify(chat)})`);
  assert.equal(chatImages().length, 0); assert.equal(inside('messages', el => el.className === 'load-image').length, 1);
  get('settingMessageActions').value = 'always'; await get('settingMessageActions').onchange();
  assert.equal(get('app')['data-actions'], 'always');
  get('settingImageSave').value = 'all'; await get('settingImageSave').onchange();
  assert.deepEqual(posts.at(-1), {image_save:'all'}); assert.equal(run('state.settings.image_save'), 'all');
  // The limit is saved as a number, not as the text of the list entry.
  get('settingImageQuota').value = '512'; await get('settingImageQuota').onchange();
  assert.deepEqual(posts.at(-1), {image_quota:512}); assert.equal(get('settingImageQuota').value, '512');
  get('settingReplyMode').value = 'confirm'; await get('settingReplyMode').onchange();
  assert.equal(get('replyMode').value, 'confirm');
  assert(posts.every(body => Object.keys(body).length === 1));
  get('settings').onclick({target:get('settingSendKey')}); assert.equal(get('settings').open, true);
  get('settings').onclick({target:get('settings')}); assert.equal(get('settings').open, false);

  // Log scope: one message's chain or all logs; "全部" stays across conversations.
  const bubble = inside('messages', el => el.className === 'bubble')[0];
  await bubble.onclick();
  assert.equal(run('state.trace'), 'photo'); assert.equal(get('trace').textContent, '追踪 photo');
  assert.equal(get('logSelected')['aria-pressed'], 'true'); assert.equal(get('logAll')['aria-pressed'], 'false');
  await get('logAll').onclick();
  assert.equal(run('state.trace'), ''); assert.equal(get('logAll')['aria-pressed'], 'true');
  assert.match(get('trace').textContent, /全部日志/); assert.equal(gets.at(-1)[0], 'logs'); assert.equal(gets.at(-1)[1].trace, '');
  await get('logSelected').onclick();
  assert.equal(run('state.trace'), 'photo'); assert.equal(get('trace').textContent, '追踪 photo');
  await get('logAll').onclick();
  await run(`selectChat(${JSON.stringify(group)})`);
  assert.equal(run('state.trace'), ''); assert.equal(get('logAll')['aria-pressed'], 'true');
  await get('logSelected').onclick();
  assert.equal(run('state.trace'), null); assert.equal(get('trace').textContent, '点击聊天气泡查看关联日志');
  assert.equal(get('logs').children[0].textContent, '暂无选中的消息');

  // The status recovers by itself after a connection error.
  stateFails = true; await run('poll()');
  assert.equal(get('health').className, 'health error'); assert.equal(get('health').textContent, '连接异常');
  assert.equal(get('health').title, 'Network down');
  stateFails = false; dropped = 3; await run('poll()');
  assert.equal(get('health').className, 'health warn'); assert.equal(get('health').textContent, '已连接 · 丢弃 3 条记录');
  console.log('Settings load, retry, save and restore; avatars, automatic images, send keys, log scope and connection status passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
