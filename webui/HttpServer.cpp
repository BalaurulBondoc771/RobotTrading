#include "../engine/platform.h"
#include "HttpServer.h"
#include "EngineProxy.h"
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <algorithm>

// ============================================================
// Embedded HTML page (raw string literal — C++11)
// ============================================================
static const char* HTML_PAGE = R"HTML(<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BADEA HFT v10</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d0d0d;color:#c8c8c8;font:12px/1.5 'Consolas','Courier New',monospace;overflow:hidden}
header{background:#111;border-bottom:1px solid #222;padding:8px 16px;display:flex;align-items:center;gap:12px;height:36px}
header h1{font-size:13px;color:#4caf50;letter-spacing:2px;white-space:nowrap}
.badge{font-size:10px;padding:2px 7px;border-radius:8px;white-space:nowrap}
.on {background:#1a3a1a;color:#4caf50;border:1px solid #4caf50}
.off{background:#1e1e1e;color:#555;border:1px solid #333}
.run{background:#3a2a10;color:#ff9800;border:1px solid #ff9800}
.layout{display:flex;height:calc(100vh - 36px)}
.sidebar{width:270px;min-width:270px;background:#111;border-right:1px solid #1e1e1e;overflow-y:auto;display:flex;flex-direction:column;gap:1px}
.main{flex:1;display:flex;flex-direction:column;overflow:hidden;min-width:0}
.sec{background:#141414;padding:10px 12px}
.sec-title{color:#555;font-size:10px;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;border-bottom:1px solid #1e1e1e;padding-bottom:4px}
.fld{display:flex;align-items:center;margin-bottom:5px;gap:5px}
.fld label{color:#666;width:68px;flex-shrink:0;font-size:11px}
.fld input,.fld select{background:#1a1a1a;border:1px solid #333;color:#c8c8c8;padding:3px 6px;border-radius:2px;font:12px 'Consolas','Courier New',monospace;flex:1;min-width:0}
.fld input:focus,.fld select:focus{outline:none;border-color:#4caf50}
.btn{background:#1a2a1a;border:1px solid #4caf50;color:#4caf50;padding:3px 10px;border-radius:2px;cursor:pointer;font:11px 'Consolas',monospace;white-space:nowrap}
.btn:hover{background:#253a25}
.btn.d{background:#2a1a1a;border-color:#f44336;color:#f44336}
.btn.d:hover{background:#3a2020}
.btn.panic{background:#3a0a0a;border-color:#ff5722;color:#ff5722;font-size:13px;font-weight:bold;padding:5px 14px}
.btn.panic:hover{background:#5a1010}
.btn-row{display:flex;gap:5px;flex-wrap:wrap;margin-top:4px}
.prices{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;padding:8px 14px;background:#111;border-bottom:1px solid #1e1e1e;flex-shrink:0}
.pc .pl{color:#555;font-size:9px;text-transform:uppercase;letter-spacing:.5px}
.pc .pv{font-size:18px;font-weight:bold;color:#4caf50}
.pc .pv.neg{color:#f44336}
.chain-area{flex:1;overflow-y:auto;min-height:0}
table{width:100%;border-collapse:collapse;font-size:11px}
th{color:#555;font-weight:normal;text-align:right;padding:3px 8px;position:sticky;top:0;background:#111;border-bottom:1px solid #222;z-index:1}
td{text-align:right;padding:2px 8px;border-bottom:1px solid #161616;cursor:pointer}
tr:hover td{background:#1a1e1a}
td.st{color:#888;text-align:center;background:#161616}
td.ac{color:#4caf50!important;font-weight:bold}
td.ap{color:#2196f3!important;font-weight:bold}
.logp{height:150px;overflow-y:auto;padding:5px 10px;background:#0a0a0a;border-top:1px solid #1e1e1e;font-size:11px;color:#777;flex-shrink:0}
.logp div{padding:1px 0}
.le{color:#f44336!important}
</style></head>
<body>
<header>
  <h1>BADEA EXECUTION v10.0</h1>
  <span id="b-ib"  class="badge off">IB</span>
  <span id="b-td"  class="badge off">THETA</span>
  <span id="b-str" class="badge off">IDLE</span>
  <span id="hc" style="margin-left:auto;font-size:10px;color:#444">● connecting...</span>
</header>
<div class="layout">
<div class="sidebar">

<div class="sec">
  <div class="sec-title">Interactive Brokers</div>
  <div class="fld"><label>Host</label><input id="ib-h" value="127.0.0.1"><input id="ib-p" value="4002" style="width:46px;flex:none"></div>
  <div class="btn-row"><button class="btn" onclick="cmd('cib',{host:v('ib-h'),port:+v('ib-p')})">Connect</button><button class="btn d" onclick="cmd('dib')">Disc</button></div>
</div>

<div class="sec">
  <div class="sec-title">ThetaData</div>
  <div class="fld"><label>Host</label><input id="td-h" value="127.0.0.1"><input id="td-p" value="25520" style="width:46px;flex:none"></div>
  <div class="btn-row"><button class="btn" onclick="cmd('ciq',{host:v('td-h'),port:+v('td-p')})">Connect</button><button class="btn d" onclick="cmd('diq')">Disc</button></div>
</div>

<div class="sec">
  <div class="sec-title">Symbol</div>
  <div class="fld"><label>Symbol</label><input id="sym" value="SPY" style="text-transform:uppercase"><button class="btn" onclick="searchSym()">Search</button></div>
  <div class="fld"><label>Expiry</label><select id="exp-sel" style="flex:1"></select></div>
  <div class="btn-row"><button class="btn" onclick="loadChain()">Load Chain</button></div>
</div>

<div class="sec">
  <div class="sec-title">Arm Contract</div>
  <div class="fld"><label>Strike</label><input id="a-strike" type="number" step="0.5"></div>
  <div class="fld"><label>Type</label><select id="a-right"><option>C</option><option>P</option></select></div>
  <div class="btn-row"><button class="btn" onclick="armContract()">ARM</button></div>
</div>

<div class="sec">
  <div class="sec-title">Parameters</div>
  <div class="fld"><label>Mode</label><select id="p-mode" onchange="sp('entryMode',+this.value)"><option value="1">1 — Static</option><option value="2" selected>2 — Dynamic</option></select></div>
  <div class="fld"><label>Static $</label><input id="p-st" type="number" step="0.01" value="5.00" onchange="sp('staticPrice',+this.value)"></div>
  <div class="fld"><label>Discount</label><input id="p-dc" type="number" step="0.01" value="0.10" onchange="sp('dynDiscount',+this.value)"></div>
  <div class="fld"><label>Opt Qty</label><input id="p-oq" type="number" step="1" value="1" onchange="sp('optQty',+this.value)"></div>
  <div class="fld"><label>Hedge Qty</label><input id="p-hq" type="number" step="10" value="100" onchange="sp('hedgeQty',+this.value)"></div>
  <div class="fld"><label>Delay ms</label><input id="p-dl" type="number" step="100" value="2000" onchange="sp('updateDelayMs',+this.value)"></div>
  <div class="fld"><label>Spot Sens</label><input id="p-ss" type="number" step="0.01" value="0.05" onchange="sp('spotSens',+this.value)"></div>
  <div class="fld"><label>Hdg Off</label><input id="p-ho" type="number" step="0.01" value="0.05" onchange="sp('hedgeOffset',+this.value)"></div>
</div>

<div class="sec">
  <div class="sec-title">Strategy</div>
  <div class="btn-row"><button class="btn" style="font-weight:bold" onclick="cmd('start')">▶ START</button><button class="btn d" onclick="cmd('stop')">■ STOP</button></div>
  <div style="margin-top:8px;text-align:center"><button class="btn panic" onclick="doPanic()">⚡ PANIC STOP</button></div>
</div>

</div><!-- /sidebar -->
<div class="main">

<div class="prices">
  <div class="pc"><div class="pl">SPOT BID</div><div class="pv" id="v-sb">—</div></div>
  <div class="pc"><div class="pl">SPOT ASK</div><div class="pv" id="v-sa">—</div></div>
  <div class="pc"><div class="pl">OPT BID</div><div class="pv" id="v-ob">—</div></div>
  <div class="pc"><div class="pl">OPT ASK</div><div class="pv" id="v-oa">—</div></div>
  <div class="pc"><div class="pl">TARGET BID</div><div class="pv" id="v-tg">—</div></div>
  <div class="pc"><div class="pl">EDGE</div><div class="pv" id="v-ed">—</div></div>
  <div class="pc"><div class="pl">IB PING</div><div class="pv" id="v-pg">—</div></div>
  <div class="pc"><div class="pl">ACCOUNT</div><div class="pv" id="v-ac" style="font-size:12px">—</div></div>
</div>

<div class="chain-area">
<table>
  <thead><tr><th>CALL BID</th><th>CALL ASK</th><th class="st">STRIKE</th><th>PUT BID</th><th>PUT ASK</th></tr></thead>
  <tbody id="chain-body"></tbody>
</table>
</div>

<div class="logp" id="log"></div>
</div><!-- /main -->
</div><!-- /layout -->

<script>
const MAX_LOG=300;
let armedStrike=null,armedRight=null;

const es=new EventSource('/api/events');
const hc=document.getElementById('hc');
es.onopen =()=>{hc.textContent='● connected';hc.style.color='#4caf50'};
es.onerror=()=>{hc.textContent='● offline';hc.style.color='#f44336'};
es.onmessage=e=>{try{handle(JSON.parse(e.data))}catch(_){}};

function handle(m){
  if(m.type==='log')    addLog(m.msg,m.err);
  else if(m.type==='status') upStatus(m);
  else if(m.type==='exps')   upExps(m.list);
  else if(m.type==='chain')  upChain(m.rows);
}

function addLog(msg,isErr){
  const log=document.getElementById('log');
  const d=document.createElement('div');
  if(isErr) d.className='le';
  const t=new Date();
  const ts=[t.getHours(),t.getMinutes(),t.getSeconds()].map(x=>String(x).padStart(2,'0')).join(':')+'.'+String(t.getMilliseconds()).padStart(3,'0');
  d.textContent=`[${ts}] ${msg}`;
  log.appendChild(d);
  while(log.children.length>MAX_LOG) log.removeChild(log.firstChild);
  log.scrollTop=log.scrollHeight;
}

function upStatus(s){
  setBadge('b-ib', s.ibConnected,false,'IB');
  setBadge('b-td', s.tdConnected,false,'THETA');
  setBadge('b-str',s.stratRunning,true,s.stratRunning?'RUNNING':'IDLE');
  sv('v-sb', s.stockBid>0?s.stockBid.toFixed(2):'—');
  sv('v-sa', s.stockAsk>0?s.stockAsk.toFixed(2):'—');
  sv('v-ob', s.optBid>0?s.optBid.toFixed(2):'—');
  sv('v-oa', s.optAsk>0?s.optAsk.toFixed(2):'—');
  sv('v-tg', s.targetBid>0?s.targetBid.toFixed(2):'—');
  const ed=document.getElementById('v-ed');
  if(s.edge!==undefined){ed.textContent=s.edge.toFixed(2);ed.className='pv'+(s.edge<0?' neg':'')}
  sv('v-pg',s.ibPingMs!==undefined?s.ibPingMs+' ms':'—');
  if(s.account) sv('v-ac',s.account);
}

function setBadge(id,on,orange,text){
  const el=document.getElementById(id);
  el.textContent=text;
  el.className='badge '+(on?(orange?'run':'on'):'off');
}

function sv(id,val){const e=document.getElementById(id);if(e)e.textContent=val;}
function v(id){return document.getElementById(id).value;}

function upExps(list){
  const sel=document.getElementById('exp-sel');
  const prev=sel.value;
  sel.innerHTML=list.map(e=>`<option value="${e}">${e}</option>`).join('');
  if(prev&&list.includes(prev))sel.value=prev;
}

function upChain(rows){
  const tbody=document.getElementById('chain-body');
  tbody.innerHTML='';
  rows.forEach(r=>{
    const ca=armedStrike===r.strike&&armedRight==='C';
    const pa=armedStrike===r.strike&&armedRight==='P';
    const s=Number.isInteger(r.strike)?r.strike:r.strike.toFixed(2);
    const tr=document.createElement('tr');
    tr.innerHTML=
      `<td class="${ca?'ac':''}" onclick="pick(${r.strike},'C')" ondblclick="quickArm(${r.strike},'C')">${r.callBid>0?r.callBid.toFixed(2):'—'}</td>`+
      `<td class="${ca?'ac':''}" onclick="pick(${r.strike},'C')" ondblclick="quickArm(${r.strike},'C')">${r.callAsk>0?r.callAsk.toFixed(2):'—'}</td>`+
      `<td class="st">${s}</td>`+
      `<td class="${pa?'ap':''}" onclick="pick(${r.strike},'P')" ondblclick="quickArm(${r.strike},'P')">${r.putBid>0?r.putBid.toFixed(2):'—'}</td>`+
      `<td class="${pa?'ap':''}" onclick="pick(${r.strike},'P')" ondblclick="quickArm(${r.strike},'P')">${r.putAsk>0?r.putAsk.toFixed(2):'—'}</td>`;
    tbody.appendChild(tr);
  });
}

function pick(strike,right){
  document.getElementById('a-strike').value=strike;
  document.getElementById('a-right').value=right;
}

function quickArm(strike,right){
  pick(strike,right);
  armContract();
}

async function cmd(c,extra){
  const body=JSON.stringify({cmd:c,...(extra||{})});
  try{await fetch('/api/command',{method:'POST',headers:{'Content-Type':'application/json'},body})}catch(_){}
}

function sp(key,value){cmd('setparam',{key,value});}

function searchSym(){
  const sym=v('sym').trim().toUpperCase();
  document.getElementById('sym').value=sym;
  if(sym)cmd('search',{symbol:sym});
}

function loadChain(){
  const exp=v('exp-sel');
  if(exp)cmd('loadchain',{exp,pct:0.15});
}

function armContract(){
  const sym=v('sym').trim().toUpperCase();
  const exp=v('exp-sel');
  const strike=parseFloat(v('a-strike'));
  const right=v('a-right');
  if(sym&&exp&&strike){
    armedStrike=strike;
    armedRight=right;
    cmd('arm',{symbol:sym,exp,strike,right});
  }
}

function doPanic(){
  if(confirm('EMERGENCY STOP — cancel all orders?'))cmd('panic');
}
</script></body></html>
)HTML";

// ============================================================

HttpServer::HttpServer(EngineProxy& proxy, uint16_t port)
    : _proxy(proxy), _port(port) {}

HttpServer::~HttpServer() { stop(); }

bool HttpServer::start() {
    _listenSock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_listenSock == INVALID_SOCK) return false;

    int one = 1;
    setsockopt(_listenSock, SOL_SOCKET, SO_REUSEADDR, (char*)&one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(_listenSock, (sockaddr*)&addr, sizeof(addr)) == SOCK_ERR ||
        listen(_listenSock, 16) == SOCK_ERR) {
        plat_close_socket(_listenSock);
        _listenSock = INVALID_SOCK;
        return false;
    }

    _running.store(true);
    _acceptThread = std::thread([this]{ acceptLoop(); });
    return true;
}

void HttpServer::stop() {
    if (!_running.exchange(false)) return;
    if (_listenSock != INVALID_SOCK) {
        plat_close_socket(_listenSock);
        _listenSock = INVALID_SOCK;
    }
    {
        std::lock_guard<std::mutex> lk(_sseMux);
        for (socket_t s : _sseClients) plat_close_socket(s);
        _sseClients.clear();
    }
    if (_acceptThread.joinable()) _acceptThread.join();
}

void HttpServer::acceptLoop() {
    while (_running.load()) {
        socket_t c = accept(_listenSock, nullptr, nullptr);
        if (c == INVALID_SOCK) break;
        std::thread t([this, c]{ handleClient(c); });
        t.detach();
    }
}

// Minimal HTTP request parser: extract method, path, body.
void HttpServer::handleClient(socket_t s) {
    char req[8192] = {};
    int  total     = 0;

    // Read until we have the full HTTP headers
    while (total < (int)sizeof(req) - 1) {
        int r = recv(s, req + total, sizeof(req) - 1 - total, 0);
        if (r <= 0) { closesocket(s); return; }
        total += r;
        if (strstr(req, "\r\n\r\n")) break;
    }

    // Parse method and path
    char method[8] = {}, path[256] = {};
    sscanf(req, "%7s %255s", method, path);

    if (strcmp(method, "GET") == 0) {
        if (strcmp(path, "/") == 0 || strcmp(path, "/index.html") == 0)
            serveHtml(s);
        else if (strcmp(path, "/api/events") == 0)
            serveEvents(s);  // blocks until client disconnects
        else {
            const char* r404 = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
            sendAll(s, r404, (int)strlen(r404));
            plat_close_socket(s);
        }

    } else if (strcmp(method, "POST") == 0) {
        // Find Content-Length and locate body
        int contentLength = 0;
        const char* cl = strstr(req, "Content-Length:");
        if (!cl) cl = strstr(req, "content-length:");
        if (cl) contentLength = atoi(cl + 15);

        const char* bodyStart = strstr(req, "\r\n\r\n");
        if (bodyStart) {
            bodyStart += 4;
            int bodyInHeader = total - (int)(bodyStart - req);
            // Read remaining body if needed
            while (bodyInHeader < contentLength && total < (int)sizeof(req)-1) {
                int want = contentLength - bodyInHeader;
                int room = (int)sizeof(req) - 1 - total;   // never write past req[]
                if (want > room) want = room;
                if (want <= 0) break;
                int r = recv(s, req + total, want, 0);
                if (r <= 0) break;
                total += r; bodyInHeader += r;
            }
            if (strcmp(path, "/api/command") == 0)
                handleCommand(s, bodyStart, bodyInHeader);
        }

        const char* r200 = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nAccess-Control-Allow-Origin: *\r\n\r\n";
        sendAll(s, r200, (int)strlen(r200));
        plat_close_socket(s);

    } else {
        const char* r405 = "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n";
        sendAll(s, r405, (int)strlen(r405));
        plat_close_socket(s);
    }
}

void HttpServer::serveHtml(socket_t s) {
    int htmlLen = (int)strlen(HTML_PAGE);
    char hdr[256];
    int hdrLen = snprintf(hdr, sizeof(hdr),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/html; charset=utf-8\r\n"
        "Content-Length: %d\r\n"
        "Cache-Control: no-cache\r\n"
        "\r\n", htmlLen);
    sendAll(s, hdr, hdrLen);
    sendAll(s, HTML_PAGE, htmlLen);
    closesocket(s);
}

void HttpServer::serveEvents(socket_t s) {
    const char* hdr =
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/event-stream\r\n"
        "Cache-Control: no-cache\r\n"
        "Connection: keep-alive\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "\r\n";
    if (!sendAll(s, hdr, (int)strlen(hdr))) { closesocket(s); return; }

    // Register as SSE subscriber
    {
        std::lock_guard<std::mutex> lk(_sseMux);
        _sseClients.push_back(s);
    }

    // Block until browser disconnects (recv returns 0/error)
    char discard[64];
    while (recv(s, discard, sizeof(discard), 0) > 0) {}

    // Remove from list and close
    {
        std::lock_guard<std::mutex> lk(_sseMux);
        auto it = std::find(_sseClients.begin(), _sseClients.end(), s);
        if (it != _sseClients.end()) _sseClients.erase(it);
    }
    plat_close_socket(s);
}

void HttpServer::handleCommand(socket_t /*s*/, const char* body, int bodyLen) {
    if (bodyLen <= 0) return;
    char buf[2048];
    int n = bodyLen < (int)sizeof(buf)-1 ? bodyLen : (int)sizeof(buf)-1;
    memcpy(buf, body, n); buf[n] = '\0';
    _proxy.sendCommand(buf);
}

void HttpServer::pushEvent(const char* json) {
    // SSE frame format: "data: <json>\n\n"
    char frame[8192];
    int n = snprintf(frame, sizeof(frame), "data: %s\n\n", json);
    if (n <= 0) return;

    std::lock_guard<std::mutex> lk(_sseMux);
    for (socket_t s : _sseClients)
        send(s, frame, n, 0); // failures detected when browser disconnects via recv loop
}

bool HttpServer::sendAll(socket_t s, const char* data, int len) {
    int sent = 0;
    while (sent < len) {
        int r = send(s, data + sent, len - sent, 0);
        if (r <= 0) return false;
        sent += r;
    }
    return true;
}
