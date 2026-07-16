'use strict';

const $ = id => document.getElementById(id);
const roomInput=$('roomInput'),randomRoomButton=$('randomRoomButton'),joinButton=$('joinButton');
const copyLinkButton=$('copyLinkButton'),muteButton=$('muteButton'),muteButtonText=$('muteButtonText');
const hangupButton=$('hangupButton'),statusBadge=$('statusBadge'),callTitle=$('callTitle');
const localState=$('localState'),remoteState=$('remoteState'),message=$('message'),remoteAudio=$('remoteAudio');

const state={socket:null,peer:null,localStream:null,room:'',muted:false,pendingCandidates:[],joined:false};
const rtcConfig={iceServers:[{urls:'stun:stun.l.google.com:19302'}]};

function normalizeRoom(v){return String(v||'').trim().toUpperCase().replace(/[^A-Z0-9_-]/g,'').slice(0,32)}
function createRoomCode(){const chars='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';const bytes=crypto.getRandomValues(new Uint8Array(6));return Array.from(bytes,v=>chars[v%chars.length]).join('')}
function setMessage(text,type=''){message.textContent=text;message.className=`message${type?` ${type}`:''}`}
function setStatus(text,name){statusBadge.textContent=text;statusBadge.dataset.state=name}
function updateUrl(room){const url=new URL(location.href);room?url.searchParams.set('room',room):url.searchParams.delete('room');history.replaceState({},'',url)}
function setJoinedUi(joined){state.joined=joined;joinButton.disabled=joined;roomInput.disabled=joined;randomRoomButton.disabled=joined;muteButton.disabled=!joined||!state.localStream;hangupButton.disabled=!joined}
function send(payload){if(state.socket?.readyState===WebSocket.OPEN)state.socket.send(JSON.stringify(payload))}

function connectSocket(){return new Promise((resolve,reject)=>{
  if(state.socket?.readyState===WebSocket.OPEN)return resolve();
  const protocol=location.protocol==='https:'?'wss:':'ws:';
  const socket=new WebSocket(`${protocol}//${location.host}`);state.socket=socket;
  const timer=setTimeout(()=>{socket.close();reject(new Error('连接服务器超时。'))},10000);
  socket.addEventListener('open',()=>{clearTimeout(timer);resolve()},{once:true});
  socket.addEventListener('error',()=>{clearTimeout(timer);reject(new Error('无法连接信令服务器。'))},{once:true});
  socket.addEventListener('message',handleSocketMessage);
  socket.addEventListener('close',()=>{if(state.joined){setStatus('服务器断开','error');setMessage('服务器连接已断开，请重新加入。','error');cleanupCall(false)}})
})}

async function getMicrophone(){
  if(state.localStream)return state.localStream;
  if(!window.isSecureContext&&location.hostname!=='localhost')throw new Error('麦克风只能在 HTTPS 网站或 localhost 使用。');
  if(!navigator.mediaDevices?.getUserMedia)throw new Error('当前浏览器不支持网页麦克风。');
  state.localStream=await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true,autoGainControl:true},video:false});
  localState.textContent='已启用';return state.localStream
}

function createPeerConnection(){
  if(state.peer)return state.peer;
  const peer=new RTCPeerConnection(rtcConfig);state.peer=peer;
  for(const track of state.localStream.getTracks())peer.addTrack(track,state.localStream);

  peer.addEventListener('icecandidate',event=>{
    if(event.candidate)send({type:'signal',data:{kind:'candidate',candidate:event.candidate}})
  });
  peer.addEventListener('track',event=>{
    const [stream]=event.streams;if(stream){remoteAudio.srcObject=stream;remoteAudio.play().catch(()=>{})}
  });
  peer.addEventListener('connectionstatechange',()=>{
    if(peer.connectionState==='connected'){setStatus('通话中','connected');callTitle.textContent='已接通';remoteState.textContent='语音已连接';localState.textContent=state.muted?'已静音':'正在传输';setMessage('通话已经建立。','success')}
    else if(peer.connectionState==='connecting'){setStatus('正在连接','connecting');remoteState.textContent='正在建立语音'}
    else if(peer.connectionState==='disconnected'){setStatus('连接中断','error');remoteState.textContent='连接暂时中断';setMessage('网络连接暂时中断。','error')}
    else if(peer.connectionState==='failed'){setStatus('连接失败','error');remoteState.textContent='无法建立连接';setMessage('无法点对点连接。当前版本没有 TURN，请换网络后重试。','error')}
  });
  return peer
}

async function createOffer(){const peer=createPeerConnection();const offer=await peer.createOffer();await peer.setLocalDescription(offer);send({type:'signal',data:{kind:'description',description:peer.localDescription}})}
async function handleDescription(description){
  const peer=createPeerConnection();await peer.setRemoteDescription(description);
  for(const candidate of state.pendingCandidates.splice(0))await peer.addIceCandidate(candidate);
  if(description.type==='offer'){const answer=await peer.createAnswer();await peer.setLocalDescription(answer);send({type:'signal',data:{kind:'description',description:peer.localDescription}})}
}
async function handleSignal(data){
  try{
    if(data.kind==='description')await handleDescription(data.description);
    else if(data.kind==='candidate'){const peer=createPeerConnection();if(peer.remoteDescription)await peer.addIceCandidate(data.candidate);else state.pendingCandidates.push(data.candidate)}
  }catch(error){console.error(error);setMessage(`连接协商失败：${error.message}`,'error')}
}

function handleSocketMessage(event){
  let p;try{p=JSON.parse(event.data)}catch{return}
  if(p.type==='joined'){setJoinedUi(true);setStatus('等待对方','connecting');callTitle.textContent=`房间 ${p.room}`;remoteState.textContent='等待朋友加入';setMessage(p.role==='host'?'房间已创建，把邀请链接发给朋友。':'已加入房间，正在连接对方。')}
  else if(p.type==='peer-joined'){remoteState.textContent='朋友已加入';createOffer().catch(e=>setMessage(`创建通话失败：${e.message}`,'error'))}
  else if(p.type==='signal')handleSignal(p.data);
  else if(p.type==='peer-left'){remoteState.textContent='对方已离开';setStatus('等待对方','connecting');callTitle.textContent=`房间 ${state.room}`;setMessage('对方已经离开房间。');closePeer()}
  else if(p.type==='room-full'){setStatus('房间已满','error');setMessage('这个房间已经有两个人。','error');cleanupCall(false)}
  else if(p.type==='error'){setStatus('发生错误','error');setMessage(p.message||'服务器返回错误。','error')}
}

function closePeer(){if(state.peer){state.peer.close();state.peer=null}state.pendingCandidates=[];remoteAudio.srcObject=null}
function stopMicrophone(){if(state.localStream)for(const track of state.localStream.getTracks())track.stop();state.localStream=null;state.muted=false;muteButtonText.textContent='麦克风已开';localState.textContent='尚未启用'}
function cleanupCall(closeSocket=false){closePeer();stopMicrophone();state.room='';setJoinedUi(false);setStatus('未连接','idle');callTitle.textContent='准备加入';remoteState.textContent='等待加入';updateUrl('');if(closeSocket&&state.socket){state.socket.close();state.socket=null}}

async function joinRoom(){
  const room=normalizeRoom(roomInput.value);if(!room){setMessage('请先输入或生成房间码。','error');return}
  joinButton.disabled=true;setStatus('正在准备','connecting');setMessage('正在请求麦克风权限……');
  try{await getMicrophone();await connectSocket();state.room=room;roomInput.value=room;updateUrl(room);send({type:'join',room})}
  catch(error){setStatus('无法加入','error');setMessage(error.message||'无法加入通话。','error');stopMicrophone();joinButton.disabled=false}
}
function toggleMute(){const [track]=state.localStream?.getAudioTracks()||[];if(!track)return;state.muted=!state.muted;track.enabled=!state.muted;muteButtonText.textContent=state.muted?'麦克风已关':'麦克风已开';localState.textContent=state.muted?'已静音':'正在传输'}
function hangup(){send({type:'leave'});cleanupCall(false);setMessage('你已离开通话。')}
async function copyInviteLink(){let room=normalizeRoom(roomInput.value);if(!room){room=createRoomCode();roomInput.value=room}const url=new URL(location.href);url.searchParams.set('room',room);try{await navigator.clipboard.writeText(url.toString());setMessage('邀请链接已经复制。','success')}catch{setMessage(`请手动复制：${url}`,'error')}}

randomRoomButton.addEventListener('click',()=>{roomInput.value=createRoomCode();setMessage('已生成新的房间码。')});
roomInput.addEventListener('input',()=>{roomInput.value=normalizeRoom(roomInput.value)});
roomInput.addEventListener('keydown',e=>{if(e.key==='Enter')joinRoom()});
joinButton.addEventListener('click',joinRoom);copyLinkButton.addEventListener('click',copyInviteLink);
muteButton.addEventListener('click',toggleMute);hangupButton.addEventListener('click',hangup);
window.addEventListener('beforeunload',()=>{send({type:'leave'});stopMicrophone()});

const invitedRoom=normalizeRoom(new URL(location.href).searchParams.get('room'));
if(invitedRoom){roomInput.value=invitedRoom;setMessage('邀请房间已填入，点击“加入通话”即可。')}else roomInput.value=createRoomCode();
