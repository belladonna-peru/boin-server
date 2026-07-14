const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: (process.env.DATABASE_URL || '').includes('railway.internal') ? false : { rejectUnauthorized: false },
});

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS usuarios (
      id TEXT PRIMARY KEY, n TEXT, usuario TEXT, num TEXT
    );
    CREATE TABLE IF NOT EXISTS amistades (
      a TEXT, b TEXT, PRIMARY KEY (a, b)
    );
    CREATE TABLE IF NOT EXISTS solicitudes (
      de TEXT, para TEXT, PRIMARY KEY (de, para)
    );
    CREATE TABLE IF NOT EXISTS comparto (
      de TEXT, con TEXT, PRIMARY KEY (de, con)
    );
    CREATE TABLE IF NOT EXISTS mensajes (
      id SERIAL PRIMARY KEY, de TEXT, para TEXT, texto TEXT, ts BIGINT
    );
  `);
  console.log('Base de datos lista ✅');
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Boin server OK 🧡 v4 con Postgres');
});
const io = new Server(server, { cors: { origin: '*' } });

// En memoria solo lo volátil: quién está conectado ahora
const online = {};    // userId -> Set(socketIds)
const socketDe = {};  // socketId -> userId

function sendTo(userId, event, payload) {
  const set = online[userId];
  if (set) set.forEach(sid => io.to(sid).emit(event, payload));
}

async function estadoDe(id) {
  const am = await pool.query(
    `SELECT u.id, u.n, u.usuario,
            EXISTS(SELECT 1 FROM comparto c WHERE c.de=$1 AND c.con=u.id) AS lecomparto
     FROM amistades a JOIN usuarios u ON u.id=a.b WHERE a.a=$1`, [id]);
  const so = await pool.query(
    `SELECT s.de, u.n AS den, u.usuario FROM solicitudes s JOIN usuarios u ON u.id=s.de WHERE s.para=$1`, [id]);
  return {
    amigos: am.rows.map(r => ({ id: r.id, n: r.n, usuario: r.usuario, online: !!(online[r.id] && online[r.id].size), leComparto: r.lecomparto })),
    solicitudes: so.rows.map(r => ({ de: r.de, deN: r.den, usuario: r.usuario })),
  };
}
async function avisarEstado(id) { sendTo(id, 'estado', await estadoDe(id)); }
async function avisarAmigos(id) {
  const r = await pool.query(`SELECT b FROM amistades WHERE a=$1`, [id]);
  for (const row of r.rows) await avisarEstado(row.b);
}

io.on('connection', (socket) => {

  socket.on('hola', async (d) => {
    try {
      if (!d || !d.id) return;
      await pool.query(
        `INSERT INTO usuarios (id, n, usuario, num) VALUES ($1,$2,$3,$4)
         ON CONFLICT (id) DO UPDATE SET n=COALESCE(NULLIF($2,''),usuarios.n),
           usuario=COALESCE(NULLIF($3,''),usuarios.usuario), num=COALESCE(NULLIF($4,''),usuarios.num)`,
        [d.id, d.n || 'Pata', d.usuario || '', d.num || '']);
      online[d.id] = online[d.id] || new Set();
      online[d.id].add(socket.id);
      socketDe[socket.id] = d.id;
      await avisarEstado(d.id);
      await avisarAmigos(d.id);
    } catch (e) { console.log('hola error', e.message); }
  });

  socket.on('buscar', async (q) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !q) return;
      const s = '%' + String(q).toLowerCase().replace('@', '').trim() + '%';
      const digitos = String(q).replace(/\D/g, '');
      const r = await pool.query(
        `SELECT id, n, usuario, num,
           EXISTS(SELECT 1 FROM amistades a WHERE a.a=$1 AND a.b=usuarios.id) AS esamigo,
           EXISTS(SELECT 1 FROM solicitudes so WHERE so.de=$1 AND so.para=usuarios.id) AS pendiente
         FROM usuarios
         WHERE id<>$1 AND (LOWER(n) LIKE $2 OR LOWER(REPLACE(usuario,'@','')) LIKE $2
           OR ($3<>'' AND LENGTH($3)>=4 AND num LIKE '%'||$3))
         LIMIT 10`, [yo, s, digitos]);
      socket.emit('resultados', r.rows.map(u => ({
        id: u.id, n: u.n, usuario: u.usuario,
        num: u.num ? '*** ' + u.num.slice(-3) : '',
        esAmigo: u.esamigo, pendiente: u.pendiente,
      })));
    } catch (e) { console.log('buscar error', e.message); }
  });

  socket.on('solicitud', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.para || d.para === yo) return;
      const ya = await pool.query(`SELECT 1 FROM amistades WHERE a=$1 AND b=$2`, [yo, d.para]);
      if (ya.rowCount) return;
      await pool.query(`INSERT INTO solicitudes (de,para) VALUES ($1,$2) ON CONFLICT DO NOTHING`, [yo, d.para]);
      const u = await pool.query(`SELECT n FROM usuarios WHERE id=$1`, [yo]);
      await avisarEstado(d.para);
      sendTo(d.para, 'aviso', '🧡 ' + (u.rows[0] ? u.rows[0].n : 'Alguien') + ' te envió una solicitud');
    } catch (e) { console.log('solicitud error', e.message); }
  });

  socket.on('responder', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.de) return;
      await pool.query(`DELETE FROM solicitudes WHERE de=$1 AND para=$2`, [d.de, yo]);
      if (d.acepta) {
        await pool.query(`INSERT INTO amistades VALUES ($1,$2),($2,$1) ON CONFLICT DO NOTHING`, [yo, d.de]);
        await pool.query(`INSERT INTO comparto VALUES ($1,$2),($2,$1) ON CONFLICT DO NOTHING`, [yo, d.de]);
        const u = await pool.query(`SELECT n FROM usuarios WHERE id=$1`, [yo]);
        sendTo(d.de, 'aviso', '🎉 ' + (u.rows[0] ? u.rows[0].n : 'Tu pata') + ' aceptó tu solicitud: ¡ya son patas!');
      }
      await avisarEstado(yo);
      await avisarEstado(d.de);
    } catch (e) { console.log('responder error', e.message); }
  });

  socket.on('compartir', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.con) return;
      if (d.on) await pool.query(`INSERT INTO comparto VALUES ($1,$2) ON CONFLICT DO NOTHING`, [yo, d.con]);
      else {
        await pool.query(`DELETE FROM comparto WHERE de=$1 AND con=$2`, [yo, d.con]);
        sendTo(d.con, 'ubi-off', { id: yo });
      }
      await avisarEstado(yo);
    } catch (e) { console.log('compartir error', e.message); }
  });

  socket.on('ubi', async (data) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !data) return;
      const r = await pool.query(
        `SELECT c.con, u.n FROM comparto c JOIN usuarios u ON u.id=$1
         WHERE c.de=$1 AND EXISTS(SELECT 1 FROM amistades a WHERE a.a=$1 AND a.b=c.con)`, [yo]);
      r.rows.forEach(row => sendTo(row.con, 'ubi', { ...data, id: yo, n: row.n }));
    } catch (e) { console.log('ubi error', e.message); }
  });

  socket.on('chat', async (msg) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !msg || !msg.para || !msg.texto) return;
      const ok = await pool.query(`SELECT 1 FROM amistades WHERE a=$1 AND b=$2`, [yo, msg.para]);
      if (!ok.rowCount) { socket.emit('aviso', 'Primero deben ser patas: envíale una solicitud 🤝'); return; }
      const ts = Date.now();
      await pool.query(`INSERT INTO mensajes (de,para,texto,ts) VALUES ($1,$2,$3,$4)`, [yo, msg.para, msg.texto, ts]);
      const m = { de: yo, para: msg.para, texto: msg.texto, ts };
      sendTo(msg.para, 'chat', m);
      sendTo(yo, 'chat', m);
    } catch (e) { console.log('chat error', e.message); }
  });

  socket.on('historial', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.con) return;
      const r = await pool.query(
        `SELECT de, para, texto, ts FROM mensajes
         WHERE (de=$1 AND para=$2) OR (de=$2 AND para=$1)
         ORDER BY ts DESC LIMIT 50`, [yo, d.con]);
      socket.emit('historial', { con: d.con, lista: r.rows.reverse() });
    } catch (e) { console.log('historial error', e.message); }
  });

  socket.on('escribiendo', (d) => {
    const yo = socketDe[socket.id];
    if (yo && d && d.para) sendTo(d.para, 'escribiendo', { de: yo, para: d.para });
  });

  socket.on('disconnect', async () => {
    const yo = socketDe[socket.id];
    delete socketDe[socket.id];
    if (yo && online[yo]) {
      online[yo].delete(socket.id);
      try { await avisarAmigos(yo); } catch (e) {}
    }
  });
});

const PORT = process.env.PORT || 3000;
initDb().then(() => server.listen(PORT, () => console.log('Boin server v4 en puerto', PORT)))
  .catch(e => { console.log('Error de base de datos:', e.message); server.listen(PORT); });
