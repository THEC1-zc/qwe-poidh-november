import fs from 'node:fs';
const key=process.env.LASSO_RPC_KEY;
if(!key) throw Error('Set LASSO_RPC_KEY to your own free Lasso key');
const HTTP=`https://lasso.sh/rpc/k/${key}/profile/public/base`;
const WS=`wss://lasso.sh/ws/rpc/k/${key}/profile/public/base`;
const direct='https://base-rpc.publicnode.com';
const token='0x833589fcd6edb6e08f4c7c32d4f71b54bda02913';
const topic='0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';
const out={startedAt:new Date().toISOString(),chainId:8453,profile:'public',paidCalls:0,token,topic,phases:[],httpCalls:[]};
let id=10;const logs=[];let currentPhase='';let cap=false;
async function rpc(endpoint,method,params){const start=Date.now();const response=await fetch(endpoint,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({jsonrpc:'2.0',id:id++,method,params}),signal:AbortSignal.timeout(25000)});const j=await response.json();out.httpCalls.push({provider:endpoint===direct?'independent direct':'lasso public',method,status:response.status,milliseconds:Date.now()-start,error:j.error??null});if(j.error)throw Error('RPC_'+j.error.code);return j.result;}
async function phase(name,ms){currentPhase=name;const before=logs.length;const head=await rpc(direct,'eth_blockNumber',[]);return await new Promise((resolve,reject)=>{const socket=new WebSocket(WS);const record={name,startedAt:new Date().toISOString(),startHead:head,heads:[],closedByClient:false,logCount:0};out.phases.push(record);const timer=setTimeout(()=>{record.closedByClient=true;socket.close();resolve(record)},ms);socket.addEventListener('open',()=>{socket.send(JSON.stringify({jsonrpc:'2.0',id:1,method:'eth_subscribe',params:['logs',{address:token,topics:[topic]}]}));socket.send(JSON.stringify({jsonrpc:'2.0',id:2,method:'eth_subscribe',params:['newHeads']}));});socket.addEventListener('message',e=>{const j=JSON.parse(e.data);if(j.error){record.rpcError=j.error;return;}if(j.id){(record.acknowledgments??=[]).push({id:j.id,result:typeof j.result==='string'?'subscription-created':j.result});return;}const r=j.params?.result;if(r?.topics){if(logs.length>=15000){cap=true;socket.close();return;}logs.push({...r,phase:name});record.logCount++;}else if(r?.number)record.heads.push({number:r.number,hash:r.hash});});socket.addEventListener('error',()=>{record.socketError=true;clearTimeout(timer);socket.close();reject(Error('WebSocketError'))});socket.addEventListener('close',e=>{record.closeCode=e.code;record.endedAt=new Date().toISOString();record.collected=logs.length-before;if(!record.closedByClient){clearTimeout(timer);resolve(record);}});});}
try{
 out.beginHead=await rpc(direct,'eth_blockNumber',[]);
 await phase('baseline',15000);
 out.disconnect={startedAt:new Date().toISOString(),startHead:await rpc(direct,'eth_blockNumber',[])};
 await new Promise(r=>setTimeout(r,15000));
 out.disconnect.endHead=await rpc(direct,'eth_blockNumber',[]);out.disconnect.endedAt=new Date().toISOString();
 await phase('reconnected',15000);
 out.endHead=await rpc(direct,'eth_blockNumber',[]);
 const from=Number(BigInt(out.beginHead))+1,to=Number(BigInt(out.endHead))-2;
 out.range={from,to,fromBlock:'0x'+from.toString(16),toBlock:'0x'+to.toString(16),finality:'current canonical read; two-head tail excluded, not L1 finalized'};
 const filter={address:token,topics:[topic],fromBlock:out.range.fromBlock,toBlock:out.range.toBlock};
 const canonical=await rpc(direct,'eth_getLogs',[filter]);const backfill=await rpc(HTTP,'eth_getLogs',[filter]);
 const identity=l=>[l.blockHash,l.transactionHash,l.logIndex].join(':').toLowerCase();
 const truth=new Set(canonical.map(identity));const repair=new Set(backfill.map(identity));const live=logs.filter(l=>Number(BigInt(l.blockNumber))>=from&&Number(BigInt(l.blockNumber))<=to&&!l.removed);const received=new Set(live.map(identity));
 const absent=[...truth].filter(k=>!received.has(k));const extra=[...received].filter(k=>!truth.has(k));
 out.result={capReached:cap,liveNotifications:live.length,liveUnique:received.size,liveDuplicates:live.length-received.size,canonicalCount:truth.size,backfillCount:repair.size,missingLive:absent.length,extraLive:extra.length,backfillMatchesCanonical:truth.size===repair.size&&[...truth].every(k=>repair.has(k)),dedupedUnion:new Set([...received,...repair]).size,removedNotifications:logs.filter(l=>l.removed).length};
 out.canonicalLogs=canonical;out.backfillLogs=backfill;out.liveLogs=logs;
}catch(e){out.error={name:e.name,message:String(e.message).replaceAll(key,'[REDACTED]')};}
out.endedAt=new Date().toISOString();fs.writeFileSync(process.env.RESULT_FILE ?? 'reconnect-result.json',JSON.stringify(out,null,2));console.log(JSON.stringify({status:out.error?'error':'completed',result:out.result,error:out.error}));
