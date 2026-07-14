const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: (process.env.DATABASE_URL || '').includes('railway.internal') ? false : { rejectUnauthorized: false },
});

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS usuarios ( id TEXT PRIMARY KEY, n TEXT, usuario TEXT, num TEXT );
    CREATE TABLE IF NOT EXISTS amistades ( a TEXT, b TEXT, PRIMARY KEY (a, b) );
    CREATE TABLE IF NOT EXISTS solicitudes ( de TEXT, para TEXT, PRIMARY KEY (de, para) );
    CREATE TABLE IF NOT EXISTS comparto ( de TEXT, con TEXT, PRIMARY KEY (de, con) );
    CREATE TABLE IF NOT EXISTS mensajes ( id SERIAL PRIMARY KEY, de TEXT, para TEXT, texto TEXT, ts BIGINT );
    CREATE TABLE IF NOT EXISTS momentos ( id SERIAL PRIMARY KEY, de TEXT, texto TEXT, color INT DEFAULT 0, ts BIGINT );
    CREATE TABLE IF NOT EXISTS likes ( momento_id INT, de TEXT, PRIMARY KEY (momento_id, de) );
    ALTER TABLE momentos ADD COLUMN IF NOT EXISTS foto TEXT;
    ALTER TABLE momentos ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;
    ALTER TABLE momentos ADD COLUMN IF NOT EXISTS lng DOUBLE PRECISION;
    ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS bio TEXT DEFAULT '';
    ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS tipo TEXT DEFAULT 'personal';
    CREATE TABLE IF NOT EXISTS notifs ( id SERIAL PRIMARY KEY, para TEXT, tipo TEXT, texto TEXT, ts BIGINT, leida BOOLEAN DEFAULT FALSE );
    CREATE TABLE IF NOT EXISTS negocios ( id TEXT PRIMARY KEY, nombre TEXT, descr TEXT, cat TEXT );
    ALTER TABLE negocios ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;
    ALTER TABLE negocios ADD COLUMN IF NOT EXISTS lng DOUBLE PRECISION;
    CREATE TABLE IF NOT EXISTS productos ( id SERIAL PRIMARY KEY, negocio TEXT, nombre TEXT, precio NUMERIC );
    CREATE TABLE IF NOT EXISTS comentarios ( id SERIAL PRIMARY KEY, momento_id INT, de TEXT, texto TEXT, padre INT, ts BIGINT );
    CREATE TABLE IF NOT EXISTS grupos ( id SERIAL PRIMARY KEY, nombre TEXT, creador TEXT, ts BIGINT );
    CREATE TABLE IF NOT EXISTS grupo_miembros ( grupo INT, uid TEXT, PRIMARY KEY (grupo, uid) );
    CREATE TABLE IF NOT EXISTS mensajes_grupo ( id SERIAL PRIMARY KEY, grupo INT, de TEXT, texto TEXT, ts BIGINT );
    CREATE TABLE IF NOT EXISTS sugerencias ( id SERIAL PRIMARY KEY, de TEXT, texto TEXT, ts BIGINT );
    CREATE TABLE IF NOT EXISTS feat_votos ( feat TEXT, de TEXT, PRIMARY KEY (feat, de) );
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS leido BOOLEAN DEFAULT FALSE;
    CREATE TABLE IF NOT EXISTS bloqueos ( de TEXT, a TEXT, PRIMARY KEY (de, a) );
    CREATE TABLE IF NOT EXISTS dias_activos ( uid TEXT, dia TEXT, PRIMARY KEY (uid, dia) );
    CREATE TABLE IF NOT EXISTS wallet_mov ( id SERIAL PRIMARY KEY, uid TEXT, tipo TEXT, monto NUMERIC, detalle TEXT, ts BIGINT );
  `);
  console.log('Base de datos lista ✅');
}

const io = new Server(undefined, { cors: { origin: '*' } });
const online = {};
const socketDe = {};
const ubiCache = {};

function sendTo(userId, event, payload) {
  const set = online[userId];
  if (set) set.forEach(sid => io.to(sid).emit(event, payload));
}

async function nombreDe(id) {
  const r = await pool.query(`SELECT n FROM usuarios WHERE id=$1`, [id]);
  return r.rowCount ? r.rows[0].n : 'Pata';
}

function diaHoy() {
  return new Date(Date.now() - 5 * 3600 * 1000).toISOString().slice(0, 10);
}

async function gruposDe(id) {
  const r = await pool.query(
    `SELECT g.id, g.nombre,
       (SELECT COUNT(*) FROM grupo_miembros m WHERE m.grupo=g.id)::int AS miembros
     FROM grupos g JOIN grupo_miembros mm ON mm.grupo=g.id AND mm.uid=$1
     ORDER BY g.ts DESC`, [id]);
  return r.rows;
}

async function crearNotif(para, tipo, texto) {
  const ts = Date.now();
  await pool.query(`INSERT INTO notifs (para,tipo,texto,ts) VALUES ($1,$2,$3,$4)`, [para, tipo, texto, ts]);
  sendTo(para, 'notif', { tipo, texto, ts });
}

async function repartirUbi(yo, lat, lng) {
  ubiCache[yo] = { lat, lng, ts: Date.now() };
  const n = await nombreDe(yo);
  const r = await pool.query(
    `SELECT c.con FROM comparto c
     WHERE c.de=$1 AND EXISTS(SELECT 1 FROM amistades a WHERE a.a=$1 AND a.b=c.con)`, [yo]);
  r.rows.forEach(row => sendTo(row.con, 'ubi', { lat, lng, id: yo, n }));
}

async function hayBloqueo(a, b) {
  const r = await pool.query(
    `SELECT 1 FROM bloqueos WHERE (de=$1 AND a=$2) OR (de=$2 AND a=$1)`, [a, b]);
  return !!r.rowCount;
}

async function puedenChatear(a, b) {
  if (await hayBloqueo(a, b)) return false;
  const am = await pool.query(`SELECT 1 FROM amistades WHERE a=$1 AND b=$2`, [a, b]);
  if (am.rowCount) return true;
  const biz = await pool.query(`SELECT 1 FROM usuarios WHERE (id=$1 OR id=$2) AND tipo='business'`, [a, b]);
  return !!biz.rowCount;
}

async function estadoDe(id) {
  const am = await pool.query(
    `SELECT u.id, u.n, u.usuario,
            EXISTS(SELECT 1 FROM comparto c WHERE c.de=$1 AND c.con=u.id) AS lecomparto,
            EXISTS(SELECT 1 FROM comparto c2 WHERE c2.de=u.id AND c2.con=$1) AS mecomparte,
            (SELECT COUNT(*) FROM mensajes ms WHERE ms.de=u.id AND ms.para=$1 AND ms.leido=FALSE)::int AS noleidos
     FROM amistades a JOIN usuarios u ON u.id=a.b WHERE a.a=$1`, [id]);
  const so = await pool.query(
    `SELECT s.de, u.n AS den, u.usuario FROM solicitudes s JOIN usuarios u ON u.id=s.de WHERE s.para=$1`, [id]);
  const pe = await pool.query(
    `SELECT DISTINCT cid FROM (
       SELECT CASE WHEN m.de=$1 THEN m.para ELSE m.de END AS cid FROM mensajes m WHERE m.de=$1 OR m.para=$1
     ) x
     WHERE cid<>$1 AND NOT EXISTS(SELECT 1 FROM amistades a WHERE a.a=$1 AND a.b=cid)`, [id]);
  const extras = [];
  for (const row of pe.rows) {
    const u = await pool.query(
      `SELECT u.n, u.usuario, u.tipo, ng.nombre AS bizn FROM usuarios u
       LEFT JOIN negocios ng ON ng.id=u.id WHERE u.id=$1`, [row.cid]);
    if (u.rowCount) {
      extras.push({
        id: row.cid,
        n: u.rows[0].bizn || u.rows[0].n,
        usuario: u.rows[0].usuario,
        online: !!(online[row.cid] && online[row.cid].size),
        leComparto: false,
        biz: u.rows[0].tipo === 'business',
        noLeidos: 0,
      });
    }
  }
  return {
    amigos: [
      ...am.rows.map(r => ({ id: r.id, n: r.n, usuario: r.usuario, online: !!(online[r.id] && online[r.id].size), leComparto: r.lecomparto, meComparte: r.mecomparte, noLeidos: r.noleidos })),
      ...extras,
    ],
    solicitudes: so.rows.map(r => ({ de: r.de, deN: r.den, usuario: r.usuario })),
  };
}
async function avisarEstado(id) { sendTo(id, 'estado', await estadoDe(id)); }
async function avisarAmigos(id) {
  const r = await pool.query(`SELECT b FROM amistades WHERE a=$1`, [id]);
  for (const row of r.rows) await avisarEstado(row.b);
}

async function perfilDe(yo) {
  const u = await pool.query(`SELECT n, usuario, bio, tipo FROM usuarios WHERE id=$1`, [yo]);
  const pa = await pool.query(`SELECT COUNT(*)::int AS c FROM amistades WHERE a=$1`, [yo]);
  const mo = await pool.query(`SELECT COUNT(*)::int AS c FROM momentos WHERE de=$1`, [yo]);
  const li = await pool.query(
    `SELECT COUNT(*)::int AS c FROM likes l JOIN momentos m ON m.id=l.momento_id WHERE m.de=$1`, [yo]);
  return {
    n: u.rows[0] ? u.rows[0].n : 'Pata',
    usuario: u.rows[0] ? u.rows[0].usuario : '',
    bio: u.rows[0] ? (u.rows[0].bio || '') : '',
    tipo: u.rows[0] ? (u.rows[0].tipo || 'personal') : 'personal',
    patas: pa.rows[0].c, momentos: mo.rows[0].c, likes: li.rows[0].c,
  };
}

async function saldoDe(yo) {
  const r = await pool.query(
    `SELECT COALESCE(SUM(CASE WHEN tipo IN ('recarga','recibido') THEN monto ELSE -monto END),0)::float AS s
     FROM wallet_mov WHERE uid=$1`, [yo]);
  return r.rows[0].s;
}

async function walletDe(yo) {
  const movs = await pool.query(
    `SELECT tipo, monto::float, detalle, ts FROM wallet_mov WHERE uid=$1 ORDER BY ts DESC LIMIT 30`, [yo]);
  return { saldo: await saldoDe(yo), movs: movs.rows.map(x => ({ ...x, ts: Number(x.ts) })) };
}

async function bizMio(yo) {
  const n = await pool.query(`SELECT * FROM negocios WHERE id=$1`, [yo]);
  if (!n.rowCount) return { negocio: null, productos: [] };
  const p = await pool.query(`SELECT id, nombre, precio::float FROM productos WHERE negocio=$1 ORDER BY id`, [yo]);
  return { negocio: n.rows[0], productos: p.rows };
}

const FEATS = ['llamadas', 'wallet', 'streamer', 'mascotas'];

async function contarVotos(yo) {
  const r = await pool.query(`SELECT feat, COUNT(*)::int AS n FROM feat_votos GROUP BY feat`);
  const mios = await pool.query(`SELECT feat FROM feat_votos WHERE de=$1`, [yo]);
  const votados = mios.rows.map(x => x.feat);
  const conteo = {};
  FEATS.forEach(f => conteo[f] = 0);
  r.rows.forEach(x => { if (conteo[x.feat] !== undefined) conteo[x.feat] = x.n; });
  return { conteo, votados };
}

const server = http.createServer((req, res) => {
  if (req.method === 'POST' && req.url === '/ubi') {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
      try {
        const d = JSON.parse(body);
        if (d && d.id && d.lat) await repartirUbi(d.id, d.lat, d.lng);
        res.writeHead(200); res.end('ok');
      } catch (e) { res.writeHead(400); res.end('error'); }
    });
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Boin server OK 🧡 v10 wallet+panel');
});
io.attach(server);

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
      await pool.query(`INSERT INTO dias_activos VALUES ($1,$2) ON CONFLICT DO NOTHING`, [d.id, diaHoy()]);
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
      if (await hayBloqueo(yo, d.para)) return;
      await pool.query(`INSERT INTO solicitudes (de,para) VALUES ($1,$2) ON CONFLICT DO NOTHING`, [yo, d.para]);
      await avisarEstado(d.para);
      await crearNotif(d.para, 'solicitud', '🧡 ' + (await nombreDe(yo)) + ' te envió una solicitud');
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
        await crearNotif(d.de, 'pata', '🎉 ' + (await nombreDe(yo)) + ' aceptó tu solicitud: ¡ya son patas!');
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

  socket.on('amistad-quitar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      await pool.query(`DELETE FROM amistades WHERE (a=$1 AND b=$2) OR (a=$2 AND b=$1)`, [yo, d.id]);
      await pool.query(`DELETE FROM comparto WHERE (de=$1 AND con=$2) OR (de=$2 AND con=$1)`, [yo, d.id]);
      sendTo(d.id, 'ubi-off', { id: yo });
      sendTo(yo, 'ubi-off', { id: d.id });
      await avisarEstado(yo);
      await avisarEstado(d.id);
      socket.emit('aviso', '💔 Dejaron de ser patas (y de compartir ubi)');
    } catch (e) { console.log('quitar error', e.message); }
  });

  socket.on('bloquear', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      await pool.query(`INSERT INTO bloqueos VALUES ($1,$2) ON CONFLICT DO NOTHING`, [yo, d.id]);
      await pool.query(`DELETE FROM amistades WHERE (a=$1 AND b=$2) OR (a=$2 AND b=$1)`, [yo, d.id]);
      await pool.query(`DELETE FROM comparto WHERE (de=$1 AND con=$2) OR (de=$2 AND con=$1)`, [yo, d.id]);
      await pool.query(`DELETE FROM solicitudes WHERE (de=$1 AND para=$2) OR (de=$2 AND para=$1)`, [yo, d.id]);
      sendTo(d.id, 'ubi-off', { id: yo });
      sendTo(yo, 'ubi-off', { id: d.id });
      await avisarEstado(yo);
      await avisarEstado(d.id);
      socket.emit('aviso', '⛔ Usuario bloqueado: no podrá verte, escribirte ni enviarte solicitudes');
    } catch (e) { console.log('bloquear error', e.message); }
  });

  socket.on('ubi', async (data) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !data) return;
      await repartirUbi(yo, data.lat, data.lng);
    } catch (e) { console.log('ubi error', e.message); }
  });

  socket.on('zonas', () => {
    try {
      const ahora = Date.now();
      const celdas = {};
      Object.values(ubiCache).forEach(u => {
        if (ahora - u.ts > 120000) return;
        const k = Math.round(u.lat / 0.003) + '_' + Math.round(u.lng / 0.003);
        celdas[k] = celdas[k] || { lat: 0, lng: 0, n: 0 };
        celdas[k].lat += u.lat; celdas[k].lng += u.lng; celdas[k].n++;
      });
      const zonas = Object.values(celdas)
        .filter(c => c.n >= 2)
        .map(c => ({ lat: c.lat / c.n, lng: c.lng / c.n, n: c.n }));
      socket.emit('zonas', zonas);
    } catch (e) {}
  });

  socket.on('diario', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const dias = await pool.query(
        `SELECT dia FROM dias_activos WHERE uid=$1 ORDER BY dia DESC LIMIT 60`, [yo]);
      const set = new Set(dias.rows.map(x => x.dia));
      let racha = 0;
      let cursor = new Date(Date.now() - 5 * 3600 * 1000);
      while (set.has(cursor.toISOString().slice(0, 10))) {
        racha++;
        cursor = new Date(cursor.getTime() - 24 * 3600 * 1000);
      }
      const inicioHoy = new Date(diaHoy() + 'T05:00:00Z').getTime();
      const m1 = await pool.query(`SELECT 1 FROM momentos WHERE de=$1 AND ts>=$2 LIMIT 1`, [yo, inicioHoy]);
      const m2 = await pool.query(`SELECT 1 FROM mensajes WHERE de=$1 AND ts>=$2 LIMIT 1`, [yo, inicioHoy]);
      const m3 = await pool.query(`SELECT 1 FROM comentarios WHERE de=$1 AND ts>=$2 LIMIT 1`, [yo, inicioHoy]);
      socket.emit('diario', {
        racha,
        misiones: [
          { t: '📸 Publica un momento', done: !!m1.rowCount },
          { t: '💬 Escríbele a un pata', done: !!m2.rowCount },
          { t: '🗨️ Comenta un momento', done: !!m3.rowCount },
        ],
      });
    } catch (e) { console.log('diario error', e.message); }
  });

  socket.on('chat', async (msg) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !msg || !msg.para || !msg.texto) return;
      if (!(await puedenChatear(yo, msg.para))) {
        socket.emit('aviso', 'Primero deben ser patas: envíale una solicitud 🤝');
        return;
      }
      const ts = Date.now();
      await pool.query(`INSERT INTO mensajes (de,para,texto,ts) VALUES ($1,$2,$3,$4)`, [yo, msg.para, msg.texto, ts]);
      const m = { de: yo, para: msg.para, texto: msg.texto, ts };
      sendTo(msg.para, 'chat', m);
      sendTo(yo, 'chat', m);
      await avisarEstado(msg.para);
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
      socket.emit('historial', { con: d.con, lista: r.rows.reverse().map(x => ({ ...x, ts: Number(x.ts) })) });
      await pool.query(`UPDATE mensajes SET leido=TRUE WHERE de=$2 AND para=$1 AND leido=FALSE`, [yo, d.con]);
      await avisarEstado(yo);
    } catch (e) { console.log('historial error', e.message); }
  });

  socket.on('grupo-crear', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.nombre || !d.nombre.trim() || !Array.isArray(d.miembros) || !d.miembros.length) return;
      const ts = Date.now();
      const g = await pool.query(
        `INSERT INTO grupos (nombre,creador,ts) VALUES ($1,$2,$3) RETURNING id`,
        [d.nombre.trim().slice(0, 30), yo, ts]);
      const gid = g.rows[0].id;
      await pool.query(`INSERT INTO grupo_miembros VALUES ($1,$2)`, [gid, yo]);
      const yoN = await nombreDe(yo);
      for (const m of d.miembros.slice(0, 20)) {
        const am = await pool.query(`SELECT 1 FROM amistades WHERE a=$1 AND b=$2`, [yo, m]);
        if (!am.rowCount) continue;
        await pool.query(`INSERT INTO grupo_miembros VALUES ($1,$2) ON CONFLICT DO NOTHING`, [gid, m]);
        sendTo(m, 'grupos', await gruposDe(m));
        await crearNotif(m, 'grupo', '👥 ' + yoN + ' te agregó al grupo «' + d.nombre.trim() + '»');
      }
      socket.emit('grupos', await gruposDe(yo));
      socket.emit('aviso', '👥 Grupo «' + d.nombre.trim() + '» creado 🎉');
    } catch (e) { console.log('grupo-crear error', e.message); }
  });

  socket.on('grupos', async () => {
    try {
      const yo = socketDe[socket.id];
      if (yo) socket.emit('grupos', await gruposDe(yo));
    } catch (e) {}
  });

  socket.on('grupo-chat', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.grupo || !d.texto || !d.texto.trim()) return;
      const soy = await pool.query(`SELECT 1 FROM grupo_miembros WHERE grupo=$1 AND uid=$2`, [d.grupo, yo]);
      if (!soy.rowCount) return;
      const ts = Date.now();
      await pool.query(`INSERT INTO mensajes_grupo (grupo,de,texto,ts) VALUES ($1,$2,$3,$4)`,
        [d.grupo, yo, d.texto.trim().slice(0, 500), ts]);
      const m = { grupo: d.grupo, de: yo, deN: await nombreDe(yo), texto: d.texto.trim().slice(0, 500), ts };
      const miembros = await pool.query(`SELECT uid FROM grupo_miembros WHERE grupo=$1`, [d.grupo]);
      miembros.rows.forEach(row => sendTo(row.uid, 'grupo-chat', m));
    } catch (e) { console.log('grupo-chat error', e.message); }
  });

  socket.on('grupo-historial', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.grupo) return;
      const soy = await pool.query(`SELECT 1 FROM grupo_miembros WHERE grupo=$1 AND uid=$2`, [d.grupo, yo]);
      if (!soy.rowCount) return;
      const r = await pool.query(
        `SELECT mg.de, u.n AS den, mg.texto, mg.ts
         FROM mensajes_grupo mg JOIN usuarios u ON u.id=mg.de
         WHERE mg.grupo=$1 ORDER BY mg.ts DESC LIMIT 50`, [d.grupo]);
      socket.emit('grupo-historial', { grupo: d.grupo, lista: r.rows.reverse().map(x => ({ ...x, deN: x.den, ts: Number(x.ts) })) });
    } catch (e) { console.log('grupo-historial error', e.message); }
  });

  socket.on('momento-publicar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d) return;
      const texto = (d.texto || '').trim().slice(0, 200);
      const foto = (d.foto && String(d.foto).startsWith('https://')) ? String(d.foto).slice(0, 500) : null;
      if (!texto && !foto) return;
      const lat = (typeof d.lat === 'number') ? d.lat : null;
      const lng = (typeof d.lng === 'number') ? d.lng : null;
      const ts = Date.now();
      const r = await pool.query(
        `INSERT INTO momentos (de,texto,color,ts,foto,lat,lng) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING id`,
        [yo, texto, d.color || 0, ts, foto, lat, lng]);
      const n = await nombreDe(yo);
      const m = { id: r.rows[0].id, de: yo, n, texto, color: d.color || 0, ts, foto, lat, lng, likes: 0, coms: 0, meGusta: false };
      sendTo(yo, 'momento-nuevo', m);
      const ams = await pool.query(`SELECT b FROM amistades WHERE a=$1`, [yo]);
      for (const row of ams.rows) {
        sendTo(row.b, 'momento-nuevo', m);
        await crearNotif(row.b, 'momento', '📸 ' + n + ' publicó un momento');
      }
    } catch (e) { console.log('momento error', e.message); }
  });

  socket.on('feed', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const r = await pool.query(
        `SELECT m.id, m.de, u.n, m.texto, m.color, m.ts, m.foto,
           (SELECT COUNT(*) FROM likes l WHERE l.momento_id=m.id)::int AS likes,
           (SELECT COUNT(*) FROM comentarios c WHERE c.momento_id=m.id)::int AS coms,
           EXISTS(SELECT 1 FROM likes l WHERE l.momento_id=m.id AND l.de=$1) AS megusta
         FROM momentos m JOIN usuarios u ON u.id=m.de
         WHERE m.de=$1 OR EXISTS(SELECT 1 FROM amistades a WHERE a.a=$1 AND a.b=m.de)
         ORDER BY m.ts DESC LIMIT 30`, [yo]);
      socket.emit('feed', r.rows.map(x => ({ id: x.id, de: x.de, n: x.n, texto: x.texto, color: x.color, ts: Number(x.ts), foto: x.foto, likes: x.likes, coms: x.coms, meGusta: x.megusta })));
    } catch (e) { console.log('feed error', e.message); }
  });

  socket.on('momentos-mapa', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const hace24h = Date.now() - 24 * 3600 * 1000;
      const r = await pool.query(
        `SELECT m.id, m.de, u.n, m.texto, m.foto, m.lat, m.lng, m.ts
         FROM momentos m JOIN usuarios u ON u.id=m.de
         WHERE m.lat IS NOT NULL AND m.ts >= $2
           AND (m.de=$1 OR EXISTS(SELECT 1 FROM amistades a WHERE a.a=$1 AND a.b=m.de))
         ORDER BY m.ts DESC LIMIT 50`, [yo, hace24h]);
      socket.emit('momentos-mapa', r.rows.map(x => ({ ...x, ts: Number(x.ts) })));
    } catch (e) { console.log('momentos-mapa error', e.message); }
  });

  socket.on('historias', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const hace24h = Date.now() - 24 * 3600 * 1000;
      const r = await pool.query(
        `SELECT m.id, m.de, u.n, m.texto, m.color, m.foto, m.ts
         FROM momentos m JOIN usuarios u ON u.id=m.de
         WHERE m.ts >= $2
           AND (m.de=$1 OR EXISTS(SELECT 1 FROM amistades a WHERE a.a=$1 AND a.b=m.de))
         ORDER BY m.ts ASC LIMIT 100`, [yo, hace24h]);
      socket.emit('historias', r.rows.map(x => ({ ...x, ts: Number(x.ts) })));
    } catch (e) { console.log('historias error', e.message); }
  });

  socket.on('momento-like', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const ya = await pool.query(`SELECT 1 FROM likes WHERE momento_id=$1 AND de=$2`, [d.id, yo]);
      if (ya.rowCount) await pool.query(`DELETE FROM likes WHERE momento_id=$1 AND de=$2`, [d.id, yo]);
      else await pool.query(`INSERT INTO likes VALUES ($1,$2) ON CONFLICT DO NOTHING`, [d.id, yo]);
      const c = await pool.query(`SELECT COUNT(*)::int AS n FROM likes WHERE momento_id=$1`, [d.id]);
      socket.emit('momento-like', { id: d.id, likes: c.rows[0].n, meGusta: !ya.rowCount });
      const own = await pool.query(`SELECT de FROM momentos WHERE id=$1`, [d.id]);
      if (own.rowCount && own.rows[0].de !== yo) {
        sendTo(own.rows[0].de, 'momento-like', { id: d.id, likes: c.rows[0].n });
        if (!ya.rowCount) await crearNotif(own.rows[0].de, 'like', '♥ A ' + (await nombreDe(yo)) + ' le gustó tu momento');
      }
    } catch (e) { console.log('like error', e.message); }
  });

  socket.on('momento-borrar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const own = await pool.query(`SELECT de FROM momentos WHERE id=$1`, [d.id]);
      if (!own.rowCount || own.rows[0].de !== yo) return;
      await pool.query(`DELETE FROM likes WHERE momento_id=$1`, [d.id]);
      await pool.query(`DELETE FROM comentarios WHERE momento_id=$1`, [d.id]);
      await pool.query(`DELETE FROM momentos WHERE id=$1`, [d.id]);
      socket.emit('momento-borrado', { id: d.id });
      const ams = await pool.query(`SELECT b FROM amistades WHERE a=$1`, [yo]);
      ams.rows.forEach(row => sendTo(row.b, 'momento-borrado', { id: d.id }));
    } catch (e) { console.log('borrar error', e.message); }
  });

  socket.on('comentarios', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const r = await pool.query(
        `SELECT c.id, c.de, u.n, c.texto, c.padre, c.ts
         FROM comentarios c JOIN usuarios u ON u.id=c.de
         WHERE c.momento_id=$1 ORDER BY c.ts ASC LIMIT 100`, [d.id]);
      socket.emit('comentarios', { id: d.id, lista: r.rows.map(x => ({ ...x, ts: Number(x.ts) })) });
    } catch (e) { console.log('comentarios error', e.message); }
  });

  socket.on('comentar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id || !d.texto || !d.texto.trim()) return;
      const ts = Date.now();
      await pool.query(
        `INSERT INTO comentarios (momento_id,de,texto,padre,ts) VALUES ($1,$2,$3,$4,$5)`,
        [d.id, yo, d.texto.trim().slice(0, 200), d.padre || null, ts]);
      const r = await pool.query(
        `SELECT c.id, c.de, u.n, c.texto, c.padre, c.ts
         FROM comentarios c JOIN usuarios u ON u.id=c.de
         WHERE c.momento_id=$1 ORDER BY c.ts ASC LIMIT 100`, [d.id]);
      const lista = r.rows.map(x => ({ ...x, ts: Number(x.ts) }));
      const c = await pool.query(`SELECT COUNT(*)::int AS n FROM comentarios WHERE momento_id=$1`, [d.id]);
      socket.emit('comentarios', { id: d.id, lista });
      socket.emit('momento-coms', { id: d.id, coms: c.rows[0].n });
      const own = await pool.query(`SELECT de FROM momentos WHERE id=$1`, [d.id]);
      if (own.rowCount && own.rows[0].de !== yo) {
        sendTo(own.rows[0].de, 'comentarios', { id: d.id, lista });
        sendTo(own.rows[0].de, 'momento-coms', { id: d.id, coms: c.rows[0].n });
        await crearNotif(own.rows[0].de, 'coment', '💬 ' + (await nombreDe(yo)) + ' comentó tu momento');
      }
    } catch (e) { console.log('comentar error', e.message); }
  });

  socket.on('votos', async () => {
    try {
      const yo = socketDe[socket.id];
      if (yo) socket.emit('votos', await contarVotos(yo));
    } catch (e) {}
  });

  socket.on('votar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !FEATS.includes(d.feat)) return;
      await pool.query(`INSERT INTO feat_votos VALUES ($1,$2) ON CONFLICT DO NOTHING`, [d.feat, yo]);
      socket.emit('votos', await contarVotos(yo));
    } catch (e) {}
  });

  socket.on('sugerir', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.texto || !d.texto.trim()) return;
      await pool.query(`INSERT INTO sugerencias (de,texto,ts) VALUES ($1,$2,$3)`,
        [yo, d.texto.trim().slice(0, 300), Date.now()]);
      const r = await pool.query(`SELECT texto, ts FROM sugerencias WHERE de=$1 ORDER BY ts DESC LIMIT 10`, [yo]);
      socket.emit('sugerencias', r.rows.map(x => ({ ...x, ts: Number(x.ts) })));
      socket.emit('aviso', '💡 ¡Gracias! Tu idea alimenta a Boinci 🧡');
    } catch (e) {}
  });

  socket.on('sugerencias', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const r = await pool.query(`SELECT texto, ts FROM sugerencias WHERE de=$1 ORDER BY ts DESC LIMIT 10`, [yo]);
      socket.emit('sugerencias', r.rows.map(x => ({ ...x, ts: Number(x.ts) })));
    } catch (e) {}
  });

  socket.on('notifs', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const r = await pool.query(
        `SELECT tipo, texto, ts, leida FROM notifs WHERE para=$1 ORDER BY ts DESC LIMIT 30`, [yo]);
      socket.emit('notifs', r.rows.map(x => ({ ...x, ts: Number(x.ts) })));
    } catch (e) { console.log('notifs error', e.message); }
  });

  socket.on('notifs-leer', async () => {
    try {
      const yo = socketDe[socket.id];
      if (yo) await pool.query(`UPDATE notifs SET leida=TRUE WHERE para=$1`, [yo]);
    } catch (e) {}
  });

  socket.on('perfil', async () => {
    try {
      const yo = socketDe[socket.id];
      if (yo) socket.emit('perfil', await perfilDe(yo));
    } catch (e) { console.log('perfil error', e.message); }
  });

  socket.on('bio', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d) return;
      await pool.query(`UPDATE usuarios SET bio=$2 WHERE id=$1`, [yo, String(d.texto || '').slice(0, 120)]);
      socket.emit('aviso', 'Descripción actualizada ✏️');
    } catch (e) {}
  });

  socket.on('editar-perfil', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d) return;
      const n = String(d.n || '').trim().slice(0, 30);
      const usuario = String(d.usuario || '').trim().slice(0, 20);
      if (!n) return;
      await pool.query(`UPDATE usuarios SET n=$2, usuario=$3 WHERE id=$1`, [yo, n, usuario]);
      socket.emit('aviso', '✏️ Perfil actualizado');
      socket.emit('perfil', await perfilDe(yo));
      await avisarAmigos(yo);
    } catch (e) { console.log('editar error', e.message); }
  });

  socket.on('perfil-de', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const u = await pool.query(
        `SELECT u.n, u.usuario, u.bio, u.tipo, ng.nombre AS bizn, ng.descr AS bizd, ng.cat AS bizc
         FROM usuarios u LEFT JOIN negocios ng ON ng.id=u.id WHERE u.id=$1`, [d.id]);
      if (!u.rowCount) return;
      const x = u.rows[0];
      const pa = await pool.query(`SELECT COUNT(*)::int AS c FROM amistades WHERE a=$1`, [d.id]);
      const mo = await pool.query(`SELECT COUNT(*)::int AS c FROM momentos WHERE de=$1`, [d.id]);
      const li = await pool.query(
        `SELECT COUNT(*)::int AS c FROM likes l JOIN momentos m ON m.id=l.momento_id WHERE m.de=$1`, [d.id]);
      const am = await pool.query(`SELECT 1 FROM amistades WHERE a=$1 AND b=$2`, [yo, d.id]);
      const pe = await pool.query(`SELECT 1 FROM solicitudes WHERE de=$1 AND para=$2`, [yo, d.id]);
      const esAmigo = !!am.rowCount;
      let moms = [];
      if (esAmigo || d.id === yo) {
        const r = await pool.query(
          `SELECT id, texto, color, foto, ts FROM momentos WHERE de=$1 ORDER BY ts DESC LIMIT 12`, [d.id]);
        moms = r.rows.map(m => ({ ...m, ts: Number(m.ts) }));
      }
      socket.emit('perfil-de', {
        id: d.id, n: x.n, usuario: x.usuario, bio: x.bio || '', tipo: x.tipo || 'personal',
        biz: x.bizn ? { nombre: x.bizn, descr: x.bizd, cat: x.bizc } : null,
        patas: pa.rows[0].c, momentos: mo.rows[0].c, likes: li.rows[0].c,
        esAmigo, pendiente: !!pe.rowCount,
        online: !!(online[d.id] && online[d.id].size),
        moms,
      });
    } catch (e) { console.log('perfil-de error', e.message); }
  });

  // ===== WALLET (billetera digital) =====

  socket.on('wallet', async () => {
    try {
      const yo = socketDe[socket.id];
      if (yo) socket.emit('wallet', await walletDe(yo));
    } catch (e) { console.log('wallet error', e.message); }
  });

  socket.on('wallet-recargar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d) return;
      const monto = Math.min(500, Math.max(1, Number(d.monto) || 0));
      if (!monto) return;
      await pool.query(
        `INSERT INTO wallet_mov (uid,tipo,monto,detalle,ts) VALUES ($1,'recarga',$2,$3,$4)`,
        [yo, monto, 'Recarga (demo, sin dinero real)', Date.now()]);
      socket.emit('wallet', await walletDe(yo));
      socket.emit('aviso', '💳 Recarga de S/ ' + monto.toFixed(2) + ' acreditada (demo)');
    } catch (e) { console.log('recargar error', e.message); }
  });

  socket.on('wallet-enviar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.para) return;
      const monto = Math.round((Number(d.monto) || 0) * 100) / 100;
      if (monto <= 0) return;
      const am = await pool.query(`SELECT 1 FROM amistades WHERE a=$1 AND b=$2`, [yo, d.para]);
      if (!am.rowCount) { socket.emit('aviso', 'Solo puedes boinear plata a tus patas 🤝'); return; }
      const saldo = await saldoDe(yo);
      if (saldo < monto) { socket.emit('aviso', '😅 Saldo insuficiente: tienes S/ ' + saldo.toFixed(2)); return; }
      const yoN = await nombreDe(yo);
      const paraN = await nombreDe(d.para);
      const ts = Date.now();
      await pool.query(`INSERT INTO wallet_mov (uid,tipo,monto,detalle,ts) VALUES ($1,'envio',$2,$3,$4)`,
        [yo, monto, 'Enviaste a ' + paraN, ts]);
      await pool.query(`INSERT INTO wallet_mov (uid,tipo,monto,detalle,ts) VALUES ($1,'recibido',$2,$3,$4)`,
        [d.para, monto, 'Te envió ' + yoN + ' 🧡', ts]);
      socket.emit('wallet', await walletDe(yo));
      sendTo(d.para, 'wallet', await walletDe(d.para));
      await crearNotif(d.para, 'wallet', '💸 ' + yoN + ' te boineó S/ ' + monto.toFixed(2));
      socket.emit('aviso', '💸 Le boineaste S/ ' + monto.toFixed(2) + ' a ' + paraN);
    } catch (e) { console.log('enviar error', e.message); }
  });

  // ===== MERCADO / BUSINESS =====

  socket.on('biz-crear', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.nombre) return;
      await pool.query(`UPDATE usuarios SET tipo='business' WHERE id=$1`, [yo]);
      await pool.query(
        `INSERT INTO negocios (id,nombre,descr,cat) VALUES ($1,$2,$3,$4)
         ON CONFLICT (id) DO UPDATE SET nombre=$2, descr=$3, cat=$4`,
        [yo, String(d.nombre).slice(0, 40), String(d.descr || '').slice(0, 120), d.cat || 'tienda']);
      socket.emit('biz-mio', await bizMio(yo));
      socket.emit('perfil', await perfilDe(yo));
      socket.emit('aviso', '🎉 ¡Tu negocio está en el Mercado de Boin!');
    } catch (e) { console.log('biz-crear error', e.message); }
  });

  socket.on('biz-mio', async () => {
    try {
      const yo = socketDe[socket.id];
      if (yo) socket.emit('biz-mio', await bizMio(yo));
    } catch (e) { console.log('biz-mio error', e.message); }
  });

  socket.on('producto-agregar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.nombre || !d.precio) return;
      const biz = await pool.query(`SELECT 1 FROM negocios WHERE id=$1`, [yo]);
      if (!biz.rowCount) return;
      await pool.query(`INSERT INTO productos (negocio,nombre,precio) VALUES ($1,$2,$3)`,
        [yo, String(d.nombre).slice(0, 40), Math.max(0, Number(d.precio) || 0)]);
      socket.emit('biz-mio', await bizMio(yo));
    } catch (e) { console.log('producto error', e.message); }
  });

  socket.on('producto-editar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const nombre = String(d.nombre || '').slice(0, 40);
      const precio = Math.max(0, Number(d.precio) || 0);
      if (!nombre || !precio) return;
      await pool.query(`UPDATE productos SET nombre=$2, precio=$3 WHERE id=$1 AND negocio=$4`,
        [d.id, nombre, precio, yo]);
      socket.emit('biz-mio', await bizMio(yo));
      socket.emit('aviso', '✏️ Producto actualizado');
    } catch (e) { console.log('editar producto error', e.message); }
  });

  socket.on('producto-borrar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      await pool.query(`DELETE FROM productos WHERE id=$1 AND negocio=$2`, [d.id, yo]);
      socket.emit('biz-mio', await bizMio(yo));
    } catch (e) {}
  });

  socket.on('negocios', async () => {
    try {
      const r = await pool.query(
        `SELECT n.id, n.nombre, n.descr, n.cat, n.lat, n.lng,
           (SELECT COUNT(*) FROM productos p WHERE p.negocio=n.id)::int AS productos
         FROM negocios n ORDER BY n.nombre`);
      socket.emit('negocios', r.rows.map(x => ({ ...x, online: !!(online[x.id] && online[x.id].size) })));
    } catch (e) { console.log('negocios error', e.message); }
  });

  socket.on('tienda', async (d) => {
    try {
      if (!d || !d.id) return;
      const n = await pool.query(`SELECT * FROM negocios WHERE id=$1`, [d.id]);
      if (!n.rowCount) return;
      const p = await pool.query(`SELECT id, nombre, precio::float FROM productos WHERE negocio=$1 ORDER BY id`, [d.id]);
      socket.emit('tienda', { negocio: n.rows[0], productos: p.rows });
    } catch (e) { console.log('tienda error', e.message); }
  });

  socket.on('pedido', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.negocio || !d.items || !d.items.length) return;
      const total = d.items.reduce((a, it) => a + (Number(it.precio) || 0) * (it.cant || 1), 0);
      const lineas = d.items.map(it => it.cant + 'x ' + it.nombre).join(', ');
      const texto = '🛍️ PEDIDO: ' + lineas + ' — Total S/ ' + total.toFixed(2);
      const ts = Date.now();
      await pool.query(`INSERT INTO mensajes (de,para,texto,ts) VALUES ($1,$2,$3,$4)`, [yo, d.negocio, texto, ts]);
      const m = { de: yo, para: d.negocio, texto, ts };
      sendTo(d.negocio, 'chat', m);
      sendTo(yo, 'chat', m);
      await crearNotif(d.negocio, 'pedido', '🛍️ Nuevo pedido de ' + (await nombreDe(yo)) + ' — S/ ' + total.toFixed(2));
      await avisarEstado(yo);
      await avisarEstado(d.negocio);
      socket.emit('aviso', '✅ Pedido enviado: coordina el pago (Yape) y la entrega en el Chat');
    } catch (e) { console.log('pedido error', e.message); }
  });

  socket.on('mis-pedidos', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const env = await pool.query(
        `SELECT m.texto, m.ts, COALESCE(ng.nombre, u.n) AS nombre
         FROM mensajes m JOIN usuarios u ON u.id=m.para
         LEFT JOIN negocios ng ON ng.id=m.para
         WHERE m.de=$1 AND m.texto LIKE '🛍️ PEDIDO%'
         ORDER BY m.ts DESC LIMIT 20`, [yo]);
      const rec = await pool.query(
        `SELECT m.texto, m.ts, u.n AS nombre
         FROM mensajes m JOIN usuarios u ON u.id=m.de
         WHERE m.para=$1 AND m.texto LIKE '🛍️ PEDIDO%'
         ORDER BY m.ts DESC LIMIT 20`, [yo]);
      socket.emit('mis-pedidos', {
        enviados: env.rows.map(x => ({ ...x, ts: Number(x.ts) })),
        recibidos: rec.rows.map(x => ({ ...x, ts: Number(x.ts) })),
      });
    } catch (e) { console.log('mis-pedidos error', e.message); }
  });

  socket.on('biz-ubi', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.lat || !d.lng) return;
      const r = await pool.query(`UPDATE negocios SET lat=$2, lng=$3 WHERE id=$1`, [yo, d.lat, d.lng]);
      if (r.rowCount) {
        socket.emit('aviso', '📍 ¡Tu local quedó fijado en el mapa de Boin!');
        socket.emit('biz-mio', await bizMio(yo));
      }
    } catch (e) { console.log('biz-ubi error', e.message); }
  });

  socket.on('negocios-mapa', async () => {
    try {
      const r = await pool.query(
        `SELECT n.id, n.nombre, n.descr, n.cat, n.lat, n.lng,
           (SELECT COUNT(*) FROM productos p WHERE p.negocio=n.id)::int AS productos
         FROM negocios n WHERE n.lat IS NOT NULL`);
      socket.emit('negocios-mapa', r.rows.map(x => ({ ...x, online: !!(online[x.id] && online[x.id].size) })));
    } catch (e) { console.log('negocios-mapa error', e.message); }
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
initDb().then(() => server.listen(PORT, () => console.log('Boin server v10 en puerto', PORT)))
  .catch(e => { console.log('Error de base de datos:', e.message); server.listen(PORT); });