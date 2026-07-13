const http = require('http');
const { Server } = require('socket.io');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Boin server OK 🧡 v3 privado');
});

const io = new Server(server, { cors: { origin: '*' } });

// ===== Estado en memoria (siguiente etapa: Postgres en Railway) =====
const users = {};        // id -> { id, n, usuario, num, sockets:Set }
const socketDe = {};     // socketId -> userId
const amigos = {};       // id -> Set(ids)
const solicitudes = {};  // id -> [ { de, deN, usuario } ]
const comparto = {};     // id -> Set(ids a los que LE comparto mi ubi)

const setDe = (obj, id) => (obj[id] = obj[id] || new Set());
const listaDe = (obj, id) => (obj[id] = obj[id] || []);

function sendTo(userId, event, payload) {
  const u = users[userId];
  if (!u) return;
  u.sockets.forEach(sid => io.to(sid).emit(event, payload));
}

function estadoDe(id) {
  return {
    amigos: [...setDe(amigos, id)].map(a => ({
      id: a,
      n: users[a] ? users[a].n : 'Pata',
      usuario: users[a] ? users[a].usuario : '',
      online: !!(users[a] && users[a].sockets.size > 0),
      leComparto: setDe(comparto, id).has(a),
    })),
    solicitudes: listaDe(solicitudes, id),
  };
}

function avisarEstado(id) { sendTo(id, 'estado', estadoDe(id)); }
function avisarAmigos(id) { setDe(amigos, id).forEach(a => avisarEstado(a)); }

io.on('connection', (socket) => {

  socket.on('hola', (d) => {
    if (!d || !d.id) return;
    if (!users[d.id]) users[d.id] = { id: d.id, n: d.n || 'Pata', usuario: d.usuario || '', num: d.num || '', sockets: new Set() };
    users[d.id].n = d.n || users[d.id].n;
    users[d.id].usuario = d.usuario || users[d.id].usuario;
    users[d.id].num = d.num || users[d.id].num;
    users[d.id].sockets.add(socket.id);
    socketDe[socket.id] = d.id;
    avisarEstado(d.id);
    avisarAmigos(d.id);
  });

  // Buscar usuario por nombre, @usuario o número
  socket.on('buscar', (q) => {
    const yo = socketDe[socket.id];
    if (!yo || !q) return;
    const s = String(q).toLowerCase().replace('@', '').trim();
    const digitos = s.replace(/\D/g, '');
    const res = Object.values(users)
      .filter(u => u.id !== yo)
      .filter(u =>
        u.n.toLowerCase().includes(s) ||
        (u.usuario || '').toLowerCase().replace('@', '').includes(s) ||
        (digitos.length >= 4 && (u.num || '').endsWith(digitos))
      )
      .slice(0, 10)
      .map(u => ({
        id: u.id, n: u.n, usuario: u.usuario,
        num: u.num ? '*** ' + u.num.slice(-3) : '',
        esAmigo: setDe(amigos, yo).has(u.id),
        pendiente: listaDe(solicitudes, u.id).some(x => x.de === yo),
      }));
    socket.emit('resultados', res);
  });

  // Enviar solicitud de amistad
  socket.on('solicitud', (d) => {
    const yo = socketDe[socket.id];
    if (!yo || !d || !d.para || d.para === yo) return;
    if (setDe(amigos, yo).has(d.para)) return;
    const lista = listaDe(solicitudes, d.para);
    if (lista.some(x => x.de === yo)) return;
    const u = users[yo];
    lista.push({ de: yo, deN: u.n, usuario: u.usuario });
    avisarEstado(d.para);
    sendTo(d.para, 'aviso', '🧡 ' + u.n + ' te envió una solicitud');
  });

  // Aceptar o rechazar solicitud
  socket.on('responder', (d) => {
    const yo = socketDe[socket.id];
    if (!yo || !d || !d.de) return;
    solicitudes[yo] = listaDe(solicitudes, yo).filter(x => x.de !== d.de);
    if (d.acepta) {
      setDe(amigos, yo).add(d.de);
      setDe(amigos, d.de).add(yo);
      // Al ser amigos, por defecto ambos comparten su ubi entre sí
      setDe(comparto, yo).add(d.de);
      setDe(comparto, d.de).add(yo);
      sendTo(d.de, 'aviso', '🎉 ' + (users[yo] ? users[yo].n : 'Tu pata') + ' aceptó tu solicitud: ¡ya son patas!');
    }
    avisarEstado(yo);
    avisarEstado(d.de);
  });

  // Activar/apagar compartir MI ubi con un amigo (modelo a→b,c)
  socket.on('compartir', (d) => {
    const yo = socketDe[socket.id];
    if (!yo || !d || !d.con) return;
    if (d.on) setDe(comparto, yo).add(d.con);
    else { setDe(comparto, yo).delete(d.con); sendTo(d.con, 'ubi-off', { id: yo }); }
    avisarEstado(yo);
  });

  // UBI PRIVADA: solo llega a los amigos con los que compartes
  socket.on('ubi', (data) => {
    const yo = socketDe[socket.id];
    if (!yo || !data) return;
    setDe(comparto, yo).forEach(con => {
      if (setDe(amigos, yo).has(con)) sendTo(con, 'ubi', { ...data, id: yo, n: users[yo].n });
    });
  });

  // CHAT: solo entre amigos
  socket.on('chat', (msg) => {
    const yo = socketDe[socket.id];
    if (!yo || !msg || !msg.para) return;
    if (!setDe(amigos, yo).has(msg.para)) {
      socket.emit('aviso', 'Primero deben ser patas: envíale una solicitud 🤝');
      return;
    }
    const m = { de: yo, deN: users[yo].n, para: msg.para, texto: msg.texto, ts: Date.now() };
    sendTo(msg.para, 'chat', m);
    sendTo(yo, 'chat', m);
  });

  socket.on('escribiendo', (d) => {
    const yo = socketDe[socket.id];
    if (yo && d && d.para) sendTo(d.para, 'escribiendo', { de: yo, para: d.para });
  });

  socket.on('disconnect', () => {
    const yo = socketDe[socket.id];
    delete socketDe[socket.id];
    if (yo && users[yo]) {
      users[yo].sockets.delete(socket.id);
      avisarAmigos(yo);
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Boin server v3 en puerto', PORT));
