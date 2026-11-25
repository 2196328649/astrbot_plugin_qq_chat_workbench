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
  showModal() { this.open = true; } close() { this.open = false; }
  contains(target) { return this === target || this.children.some(child => child.contains?.(target)); }
}
const elements = new Map();
const get = id => { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); };
get('composerMode').value = 'direct'; get('replyMode').value = 'confirm';
const chat = {id:'["qq","999","private","200",""]',title:'测试用户',target:'200',account:'999',kind:'private',group_id:''};
const other = {id:'["qq","999","private","201",""]',title:'另一位用户',target:'201',account:'999',kind:'private',group_id:''};
const QQ = 'https://multimedia.nt.qq.com.cn/download?fileid=';
const COPY = 'data:image/png;base64,iVBORw0KGgo=';
const incoming = (id, body, extra = {}) => ({id,chat:chat.id,trace:'t' + id,direction:'in',sender:'测试用户',sender_id:'200',created:id,status:'received',body,media:{},...extra});
// The image is the second segment, so its index is 1.
const rows = [
  incoming(1, [{type:'text',data:{text:'看这个'}},{type:'image',data:{url:QQ + 'a'}}]),
  incoming(2, [{type:'image',data:{url:QQ + 'b'}}]),
  incoming(3, [{type:'image',data:{url:QQ + 'c'}}]),
  // An address the page itself cannot load (not HTTPS); the server still can.
  incoming(4, [{type:'image',data:{url:'http://gchat.qpic.cn/d'}}]),
  incoming(5, [{type:'text',data:{text:'撤回前的话'}}], {status:'recalled'}),
  {id:6,chat:chat.id,trace:'t6',direction:'out',sender:'机器人',sender_id:'',remote_id:'77',created:6,status:'accepted',body:[{type:'text',data:{text:'你好'}}],media:{}},
  {id:7,chat:chat.id,trace:'t7',direction:'out',sender:'机器人',sender_id:'',remote_id:'78',created:7,status:'recalled',body:[{type:'text',data:{text:'发错了'}},{type:'image',data:{url:QQ + 'own'}}],media:{}},
  ...[8, 9, 10, 11].map(id => incoming(id, [{type:'image',data:{url:QQ + id}}]))
];
const waiting = {trace:'held',chat:other.id,title:'另一位用户',prompt:'画一只猫',status:'awaiting_approval',step:'response',started:1,source_message_id:null,sender_id:'201',can_append:false,pending_count:1,reply_mode:'confirm'};
const idle = {...waiting,trace:'plain',status:'running',pending_count:0};
const gets = []; const posts = []; const downloads = []; const outcomes = [];
let failMessages = false; let failDownload = false; let failSave = false;
// While set, the request stays unanswered until the promise resolves.
let holdSave = null; let holdMedia = null;
const bridge = {
  ready: async () => ({}),
  apiGet: async (route, data) => {
    gets.push([route, JSON.parse(JSON.stringify(data))]);
    if (route === 'settings') return {auto_images:false,avatars:false,send_key:'ctrl_enter',reply_mode:'confirm',message_actions:'always',image_save:'manual',image_quota:1024};
    if (route === 'state') return {version:'0.2.9',retention_days:14,dropped:0,images:{mode:'manual',quota_mb:512,files:2,bytes:3145728,queued:1,skipped:0}};
    if (route === 'chats') return [chat, other];
    if (route === 'jobs') return [idle, waiting];
    if (route === 'messages') {
      if (failMessages) throw Error('网络中断');
      if (data.chat !== chat.id || data.before) return [];
      return JSON.parse(JSON.stringify(rows));
    }
    if (route === 'logs') return [];
    if (route === 'review') return {items:[],instructions:[]};
    if (route === 'media') {
      if (holdMedia) await holdMedia;
      if (rows.find(row => row.id === data.id)?.media[data.index] !== 'saved') throw Error('服务器上没有这张图片的副本');
      return {url:COPY,mime:'image/png',bytes:8};
    }
    throw Error(route);
  },
  apiPost: async (route, data) => {
    posts.push([route, JSON.parse(JSON.stringify(data))]);
    if (route === 'media_save') {
      if (holdSave) await holdSave;
      if (failSave) throw Error('网络中断');
      // Message 2's address has expired: the server reports why, the row keeps it.
      const row = rows.find(item => item.id === data.id); const failed = data.id === 2;
      row.media = {...row.media, [data.index]: failed ? 'failed' : 'saved'};
      return failed ? {status:'failed',reason:'下载失败，图片可能已过期或网络不通'} : {status:'saved',mime:'image/png',bytes:8};
    }
    if (route === 'recall') { rows.find(item => item.id === data.id).status = 'recalled'; return {ok:true}; }
    if (route === 'send') return outcomes.shift() || {trace:'manual',status:'accepted'};
    throw Error(route);
  },
  download: async (route, data, filename) => {
    if (failDownload) throw Error('文件不存在');
    downloads.push([route, JSON.parse(JSON.stringify(data)), filename]);
  }
};
const context = vm.createContext({
  document:{getElementById:get,createElement:tag=>new Element(tag),hidden:false,addEventListener(){}},
  window:{AstrBotPluginPage:bridge,innerWidth:1280,innerHeight:900},
  setTimeout:()=>0,clearTimeout(){},console,Map,Set,Date,Promise,
  // No randomUUID: a dashboard opened over plain HTTP only has getRandomValues.
  crypto:{getRandomValues:bytes=>require('node:crypto').randomFillSync(bytes)}
});
vm.runInContext(fs.readFileSync(process.argv[2], 'utf8'), context);
const run = code => vm.runInContext(code, context);
const click = {stopPropagation(){}, preventDefault(){}};
const flatten = element => [element, ...(element.children || []).flatMap(flatten)];
const all = predicate => flatten(get('messages')).filter(el => el && predicate(el));
const article = id => all(el => el.tagName === 'article')[rows.findIndex(row => row.id === id)];
const within = (id, predicate) => flatten(article(id)).filter(el => el && predicate(el));
const tools = id => within(id, el => el.className === 'image-tools')[0].children.map(el => el.textContent);
const tool = (id, text) => within(id, el => el.className === 'image-tools')[0].children.find(el => el.textContent === text);
const images = id => within(id, el => el.tagName === 'img');
const count = route => posts.filter(([name]) => name === route).length;
const settle = async () => { for (let i = 0; i < 6; i++) await new Promise(setImmediate); };
const submit = () => get('composer').onsubmit({preventDefault(){}});

(async () => {
  await settle();
  await run(`selectChat(${JSON.stringify(chat)})`);
  assert.equal(run('state.settings.image_save'), 'manual');

  // 手动保存: every received web image has a 保存 button, sent images have none.
  assert.deepEqual([1, 2, 3, 4].map(tools), [['保存'], ['保存'], ['保存'], ['保存']]);
  assert.equal(within(7, el => el.className === 'image-tools')[0].children.length, 0);
  within(1, el => el.className === 'load-image')[0].onclick(click);
  assert.equal(images(1)[0].src, QQ + 'a');
  // Looking at an image saves nothing in this mode.
  images(1)[0].onload(); await settle();
  assert.equal(count('media_save'), 0);

  // Saving asks the server by message and position only; the page sends no address.
  const panel = get('messages').children;
  await tool(1, '保存').onclick(click);
  assert.deepEqual(posts.at(-1), ['media_save', {id:1,index:1,retry:true}]);
  assert.equal(get('notice').textContent, '图片已保存到服务器');
  assert.deepEqual(tools(1), ['已保存', '下载']);
  // One saved image does not rebuild the conversation, nor does the next poll.
  await run('messages()');
  assert.equal(get('messages').children, panel);
  assert.equal(images(1)[0].src, QQ + 'a');
  tool(1, '下载').onclick(click); await settle();
  assert.deepEqual(downloads.at(-1), ['media_file', {id:1,index:1}, undefined]);
  assert.equal(get('notice').textContent, '已开始下载原图');
  failDownload = true; tool(1, '下载').onclick(click); await settle(); failDownload = false;
  assert.equal(get('notice').textContent, '下载失败：文件不存在');

  // A failed save says why and can be tried again.
  await tool(2, '保存').onclick(click);
  assert.equal(get('notice').textContent, '保存失败：下载失败，图片可能已过期或网络不通');
  assert.deepEqual(tools(2), ['保存失败，重试']);
  await run('messages()');
  assert.deepEqual(tools(2), ['保存失败，重试']);
  await tool(2, '保存失败，重试').onclick(click);
  assert.deepEqual(posts.at(-1), ['media_save', {id:2,index:0,retry:true}]);

  // The QQ address has expired (or the image was recalled): the saved copy is shown.
  images(1)[0].onerror();
  assert.equal(within(1, el => el.className === 'image-note')[0].textContent, '正在读取已保存的图片…');
  await settle();
  assert.deepEqual(gets.filter(([route]) => route === 'media').map(([, data]) => data), [{id:1,index:1}]);
  assert.equal(images(1)[0].src, COPY); assert.equal(images(1)[0].alt, '图片（服务器保存的副本）');
  // Enlarged, it offers the original file; a redraw does not fetch it again.
  images(1)[0].onclick(click);
  assert.equal(get('viewer').hidden, false); assert.equal(get('viewerImage').src, COPY);
  assert.equal(get('viewerDownload').hidden, false);
  get('viewerDownload').onclick(click); await settle();
  assert.deepEqual(downloads.at(-1), ['media_file', {id:1,index:1}, undefined]);
  get('viewerClose').onclick(click); assert.equal(get('viewer').hidden, true);
  run('state.signature=null;renderMessages()');
  assert.equal(images(1)[0].src, COPY);
  assert.equal(gets.filter(([route]) => route === 'media').length, 1);
  // Without a copy, an expired image only says so, and has no download in the viewer.
  within(3, el => el.className === 'load-image')[0].onclick(click);
  images(3)[0].onclick(click); assert.equal(get('viewerDownload').hidden, true); get('viewer').onclick();
  images(3)[0].onerror();
  assert.equal(within(3, el => el.textContent === '[图片已过期或无法预览]').length, 1);
  assert.equal(gets.filter(([route]) => route === 'media').length, 1);

  // An image the browser cannot load itself is viewable once saved.
  assert.equal(within(4, el => el.className === 'image-note')[0].textContent, '[图片：浏览器无法直接预览，保存后可查看]');
  assert.equal(images(4).length, 0);
  await tool(4, '保存').onclick(click); await settle();
  assert.deepEqual(gets.at(-1), ['media', {id:4,index:0}]);
  assert.equal(images(4)[0].src, COPY); assert.deepEqual(tools(4), ['已保存', '下载']);

  // 自动保存看过的图片: saved once it has been displayed, each image once.
  run("state.settings.image_save='viewed'");
  const before = count('media_save'); get('notice').textContent = '';
  within(3, el => el.className === 'load-image')[0].onclick(click);
  images(3)[0].onload(); images(3)[0].onload(); await settle();
  assert.equal(count('media_save'), before + 1);
  assert.deepEqual(posts.at(-1), ['media_save', {id:3,index:0}]);
  assert.deepEqual(tools(3), ['已保存', '下载']);
  // An automatic save is silent, and a failed image is not retried by looking at it.
  assert.equal(get('notice').textContent, '');
  within(2, el => el.className === 'load-image')[0].onclick(click);
  images(2)[0].onload(); await settle();
  assert.equal(count('media_save'), before + 1);

  // A message the other side recalled stays readable and says so.
  assert.match(article(5).className, /^message in.* recalled$/);
  assert.equal(within(5, el => /^status /.test(el.className))[0].textContent, '对方已撤回（内容已保留）');
  assert.equal(within(5, el => el.className === 'bubble')[0].children.some(el => el.textContent === '撤回前的话'), true);
  assert.equal(within(7, el => /^status /.test(el.className))[0].textContent, '已撤回（接口确认）');
  assert.equal(within(7, el => el.className === 'recall').length, 0);

  // Recalling a sent message needs a second click.
  const recall = within(6, el => el.className === 'recall')[0];
  await recall.onclick(); assert.equal(recall.textContent, '确认撤回？'); assert.equal(count('recall'), 0);
  await recall.onclick();
  assert.deepEqual(posts.filter(([route]) => route === 'recall').map(([, data]) => data), [{id:6}]);
  assert.equal(within(6, el => el.className === 'recall').length, 0);

  // No older messages: said once, the list stays as it is.
  const kept = get('messages').children;
  await get('older').onclick(); await settle();
  assert.equal(get('notice').textContent, '没有更早的消息了'); assert.equal(get('messages').children, kept);

  // A repeated request with an unknown result is not sent again silently: the
  // text stays, and the next click is a new request.
  get('text').value = '在吗';
  outcomes.push({trace:'first',duplicate:true,status:'unknown'});
  await submit();
  assert.match(get('notice').textContent, /上一次发送的结果未知，这次没有再发/);
  assert.equal(get('text').value, '在吗'); assert.equal(run('state.request'), null);
  await submit();
  const sends = posts.filter(([route]) => route === 'send').map(([, data]) => data.request_id);
  assert.equal(sends.length, 2); assert.notEqual(sends[0], sends[1]);
  assert.match(sends[0], /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.equal(get('text').value, ''); assert.equal(get('notice').textContent, '发送接口已受理');
  // One that did arrive is only reported.
  get('text').value = '收到了吗'; outcomes.push({trace:'first',duplicate:true,status:'accepted'});
  await submit();
  assert.equal(get('notice').textContent, '该请求已提交，请查看对应任务或消息'); assert.equal(get('text').value, '');

  // Saved-image usage in the settings dialog.
  await run('health()');
  assert.equal(get('imageUsage').textContent, '已保存 2 张，占用 3.0 MB（上限 512 MB），1 张等待保存');

  // The waiting count opens the first task that needs a decision.
  await run('jobs()');
  assert.equal(get('waitingCount').textContent, '1 个待确认');
  get('waitingCount').onclick(click); await settle();
  assert.equal(get('taskPanel').open, true); assert.equal(get('jobs').value, 'held');

  // Leave and come back: nothing is expanded or cached any more.
  await run(`selectChat(${JSON.stringify(other)})`);
  await run(`selectChat(${JSON.stringify(chat)})`);
  run("state.settings.image_save='manual'");
  const mediaGets = () => gets.filter(([route]) => route === 'media').length;
  const saves = () => posts.filter(([route]) => route === 'media_save').map(([, data]) => data.id);
  const redraw = () => run('state.signature=null;renderMessages()');
  const expand = id => within(id, el => el.className === 'load-image')[0].onclick(click);
  let release;

  // The conversation is redrawn while a save is on its way: the button now on
  // screen still ends as 已保存.
  holdSave = new Promise(resolve => { release = resolve; });
  const saving = tool(8, '保存').onclick(click);
  await settle(); redraw();
  assert.deepEqual(tools(8), ['保存']);
  release(); holdSave = null; await saving; await settle();
  assert.deepEqual(tools(8), ['已保存', '下载']);

  // A copy on its way is asked for once, and a redraw does not try the dead QQ
  // address again.
  expand(1);
  const asked = mediaGets();
  holdMedia = new Promise(resolve => { release = resolve; });
  images(1)[0].onerror(); redraw();
  assert.equal(images(1).length, 0);
  assert.equal(within(1, el => el.className === 'image-note')[0].textContent, '正在读取已保存的图片…');
  assert.equal(mediaGets(), asked + 1);
  release(); holdMedia = null; await settle();
  assert.equal(images(1)[0].src, COPY); assert.equal(mediaGets(), asked + 1);
  // Once the small cache has let a copy go, a redraw offers it instead of
  // fetching it again; a click brings it back.
  run("for(let i=0;i<24;i++)rememberCopy('x'+i,'data:image/png;base64,AA==')");
  redraw();
  assert.equal(images(1).length, 0); assert.equal(mediaGets(), asked + 1);
  within(1, el => el.textContent === '查看已保存的图片')[0].onclick(click); await settle();
  assert.equal(images(1)[0].src, COPY); assert.equal(mediaGets(), asked + 2);

  // 自动保存看过的图片: an image still waiting when the conversation is left is
  // saved the next time it is shown.
  run("state.settings.image_save='viewed'");
  const start = saves().length; get('notice').textContent = '';
  holdSave = new Promise(resolve => { release = resolve; });
  for (const id of [9, 10]) { expand(id); images(id)[0].onload(); }
  await settle();
  assert.deepEqual(saves().slice(start), [9]);
  await run(`selectChat(${JSON.stringify(other)})`);
  release(); holdSave = null; await settle();
  assert.deepEqual(saves().slice(start), [9]);
  await run(`selectChat(${JSON.stringify(chat)})`);
  assert.deepEqual(tools(9), ['已保存', '下载']);
  expand(10); images(10)[0].onload(); await settle();
  assert.deepEqual(saves().slice(start), [9, 10]);
  assert.deepEqual(tools(10), ['已保存', '下载']);
  // So is one whose request got no answer; nothing is said about either.
  failSave = true; expand(11); images(11)[0].onload(); await settle(); failSave = false;
  assert.deepEqual(tools(11), ['保存']);
  redraw(); images(11)[0].onload(); await settle();
  assert.deepEqual(saves().slice(start), [9, 10, 11, 11]);
  assert.deepEqual(tools(11), ['已保存', '下载']);
  assert.equal(get('notice').textContent, '');
  run("state.settings.image_save='manual'");

  // A conversation that cannot be read does not keep the previous one on screen.
  rows.find(row => row.id === 1).media = {1:'missing'};
  await run('messages()');
  assert.deepEqual(tools(1), ['副本缺失，重新保存']);
  await tool(1, '副本缺失，重新保存').onclick(click);
  assert.deepEqual(posts.at(-1), ['media_save', {id:1,index:1,retry:true}]);
  assert.deepEqual(tools(1), ['已保存', '下载']);
  await run(`selectChat(${JSON.stringify(chat)})`);
  failMessages = true;
  await run(`selectChat(${JSON.stringify(other)})`);
  assert.deepEqual(get('messages').children.map(el => el.textContent), ['读取消息失败，稍后会自动重试']);
  assert.equal(get('notice').textContent, '读取消息失败：网络中断');
  failMessages = false; await run('messages()');
  assert.equal(all(el => el.tagName === 'article').length, 0);
  console.log('Image save, saved copies, download, viewed-mode saving, recall labels, repeated sends, waiting tasks, redraws during a save and copy reloading passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
