const http = require('http');
const { Server } = require('socket.io');

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Boin server OK 🧡');
});

const io = new Server(server, { cors: { origin: '*' } });

io.on('connection', (socket) => {
  console.log('pata conectado:', socket.id);

  // Recibe la ubi de un celular y la reenvía a todos los demás
  socket.on('ubi', (data) => {
    socket.broadcast.emit('ubi', data);
  });

  socket.on('disconnect', () => console.log('pata desconectado:', socket.id));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Boin server en puerto', PORT));