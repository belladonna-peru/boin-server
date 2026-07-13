const http = require('http');
const { Server } = require('socket.io');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Boin server OK 🧡');
});

const io = new Server(server, { cors: { origin: '*' } });

// Lista de patas conectados: { socketId: { id, n } }
const patas = {};

function avisarPatas() {
  io.emit('patas', Object.values(patas));
}

io.on('connection', (socket) => {
  // Un celular se presenta con su id y nombre
  socket.on('hola', (data) => {
    if (!data || !data.id) return;
    patas[socket.id] = { id: data.id, n: data.n || 'Pata' };
    avisarPatas();
  });

  // Ubicación en tiempo real (igual que antes)
  socket.on('ubi', (data) => {
    socket.broadcast.emit('ubi', data);
  });

  // Chat: reenvía el mensaje a todos (cada celular filtra los suyos)
  socket.on('chat', (msg) => {
    if (!msg || !msg.de || !msg.para) return;
    io.emit('chat', { ...msg, ts: Date.now() });
  });

  // Aviso de "escribiendo..."
  socket.on('escribiendo', (data) => {
    socket.broadcast.emit('escribiendo', data);
  });

  socket.on('disconnect', () => {
    delete patas[socket.id];
    avisarPatas();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Boin server en puerto', PORT));
