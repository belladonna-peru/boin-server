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
    CREATE TABLE IF NOT EXISTS pedidos ( id SERIAL PRIMARY KEY, de TEXT, negocio TEXT, detalle TEXT, total NUMERIC, estado TEXT DEFAULT 'recibido', pagado BOOLEAN DEFAULT FALSE, ts BIGINT );
    CREATE INDEX IF NOT EXISTS idx_mensajes_para ON mensajes(para, leido);
    CREATE INDEX IF NOT EXISTS idx_mensajes_par ON mensajes(de, para, ts);
    CREATE INDEX IF NOT EXISTS idx_momentos_de ON momentos(de, ts);
    CREATE INDEX IF NOT EXISTS idx_notifs_para ON notifs(para, ts);
    CREATE INDEX IF NOT EXISTS idx_amistades_a ON amistades(a);
    CREATE INDEX IF NOT EXISTS idx_comentarios_mid ON comentarios(momento_id);
    CREATE INDEX IF NOT EXISTS idx_pedidos_de ON pedidos(de);
    CREATE INDEX IF NOT EXISTS idx_pedidos_neg ON pedidos(negocio);
    CREATE INDEX IF NOT EXISTS idx_wallet_uid ON wallet_mov(uid, ts);
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS foto TEXT;
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS cita TEXT;
    ALTER TABLE mensajes_grupo ADD COLUMN IF NOT EXISTS foto TEXT;
    ALTER TABLE mensajes_grupo ADD COLUMN IF NOT EXISTS cita TEXT;
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS reaccion TEXT;
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS audio TEXT;
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS dur INT;
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS plata NUMERIC;
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS editado BOOLEAN DEFAULT FALSE;
    ALTER TABLE mensajes_grupo ADD COLUMN IF NOT EXISTS audio TEXT;
    ALTER TABLE mensajes_grupo ADD COLUMN IF NOT EXISTS dur INT;
    CREATE TABLE IF NOT EXISTS chat_prefs ( uid TEXT, con TEXT, fijado BOOLEAN DEFAULT FALSE, silencio BOOLEAN DEFAULT FALSE, PRIMARY KEY (uid, con) );
    ALTER TABLE chat_prefs ADD COLUMN IF NOT EXISTS archivado BOOLEAN DEFAULT FALSE;
    ALTER TABLE negocios ADD COLUMN IF NOT EXISTS abierto BOOLEAN DEFAULT TRUE;
    ALTER TABLE momentos ADD COLUMN IF NOT EXISTS op1 TEXT;
    ALTER TABLE momentos ADD COLUMN IF NOT EXISTS op2 TEXT;
    CREATE TABLE IF NOT EXISTS enc_votos ( momento INT, de TEXT, op INT, PRIMARY KEY (momento, de) );
    CREATE TABLE IF NOT EXISTS guardados ( uid TEXT, momento INT, ts BIGINT, PRIMARY KEY (uid, momento) );
    ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS foto TEXT;
    ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS animal TEXT DEFAULT 'cuy';
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS lng DOUBLE PRECISION;
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS cobro NUMERIC;
    ALTER TABLE mensajes ADD COLUMN IF NOT EXISTS cobrado BOOLEAN DEFAULT FALSE;
    ALTER TABLE mensajes_grupo ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION;
    ALTER TABLE mensajes_grupo ADD COLUMN IF NOT EXISTS lng DOUBLE PRECISION;
    ALTER TABLE mensajes_grupo ADD COLUMN IF NOT EXISTS plan TEXT;
    ALTER TABLE mensajes_grupo ADD COLUMN IF NOT EXISTS plan_hora TEXT;
    CREATE TABLE IF NOT EXISTS plan_votos ( msg INT, uid TEXT, voy BOOLEAN, PRIMARY KEY (msg, uid) );
    CREATE TABLE IF NOT EXISTS resenas ( negocio TEXT, de TEXT, estrellas INT, texto TEXT, ts BIGINT, PRIMARY KEY (negocio, de) );
    ALTER TABLE mensajes_grupo ADD COLUMN IF NOT EXISTS chancha TEXT;
    ALTER TABLE mensajes_grupo ADD COLUMN IF NOT EXISTS chancha_meta NUMERIC;
    CREATE TABLE IF NOT EXISTS chancha_aportes ( msg INT, uid TEXT, monto NUMERIC, ts BIGINT, PRIMARY KEY (msg, uid) );
    ALTER TABLE productos ADD COLUMN IF NOT EXISTS foto TEXT;
    ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS visto BIGINT;
    ALTER TABLE chat_prefs ADD COLUMN IF NOT EXISTS favorito BOOLEAN DEFAULT FALSE;
ALTER TABLE chat_prefs ADD COLUMN IF NOT EXISTS restringido BOOLEAN DEFAULT FALSE;
ALTER TABLE mensajes_grupo ADD COLUMN IF NOT EXISTS reaccion TEXT;

    
  `);
  console.log('Base de datos lista ✅');
}

const io = new Server(undefined, { cors: { origin: '*' } });
const online = {};
const socketDe = {};
const ubiCache = {};
const puntosCache = {}; // punto de encuentro activo por usuario (memoria, expira en 2 h)

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
    `SELECT u.id, u.n, u.usuario, u.foto, u.visto,
            EXISTS(SELECT 1 FROM comparto c WHERE c.de=$1 AND c.con=u.id) AS lecomparto,
            EXISTS(SELECT 1 FROM comparto c2 WHERE c2.de=u.id AND c2.con=$1) AS mecomparte,
            (SELECT COUNT(*) FROM mensajes ms WHERE ms.de=u.id AND ms.para=$1 AND ms.leido=FALSE)::int AS noleidos,
            (SELECT MAX(ms2.ts) FROM mensajes ms2 WHERE (ms2.de=u.id AND ms2.para=$1) OR (ms2.de=$1 AND ms2.para=u.id)) AS ultimo
     FROM amistades a JOIN usuarios u ON u.id=a.b WHERE a.a=$1`, [id]);
  const so = await pool.query(
    `SELECT s.de, u.n AS den, u.usuario, u.foto FROM solicitudes s JOIN usuarios u ON u.id=s.de WHERE s.para=$1`, [id]);
  const pe = await pool.query(
    `SELECT DISTINCT cid FROM (
       SELECT CASE WHEN m.de=$1 THEN m.para ELSE m.de END AS cid FROM mensajes m WHERE m.de=$1 OR m.para=$1
     ) x
     WHERE cid<>$1 AND NOT EXISTS(SELECT 1 FROM amistades a WHERE a.a=$1 AND a.b=cid)`, [id]);
  const extras = [];
  for (const row of pe.rows) {
    const u = await pool.query(
      `SELECT u.n, u.usuario, u.tipo, u.foto, ng.nombre AS bizn FROM usuarios u
       LEFT JOIN negocios ng ON ng.id=u.id WHERE u.id=$1`, [row.cid]);
    if (u.rowCount) {
      extras.push({
        id: row.cid,
        n: u.rows[0].bizn || u.rows[0].n,
        usuario: u.rows[0].usuario,
        foto: u.rows[0].foto,
        online: !!(online[row.cid] && online[row.cid].size),
        leComparto: false,
        biz: u.rows[0].tipo === 'business',
        noLeidos: 0,
        ultimo: 0,
      });
    }
  }
  return {
    amigos: [
      ...am.rows.map(r => ({ id: r.id, n: r.n, usuario: r.usuario, foto: r.foto, online: !!(online[r.id] && online[r.id].size), leComparto: r.lecomparto, meComparte: r.mecomparte, visto: r.visto ? Number(r.visto) : null, noLeidos: r.noleidos, ultimo: r.ultimo ? Number(r.ultimo) : 0, })), 
      ...extras,
    ],
    solicitudes: so.rows.map(r => ({ de: r.de, deN: r.den, usuario: r.usuario, foto: r.foto })),
  };
}
async function avisarEstado(id) { sendTo(id, 'estado', await estadoDe(id)); }
async function avisarAmigos(id) {
  const r = await pool.query(`SELECT b FROM amistades WHERE a=$1`, [id]);
  for (const row of r.rows) await avisarEstado(row.b);
}

async function perfilDe(yo) {
  const u = await pool.query(`SELECT n, usuario, bio, tipo, foto FROM usuarios WHERE id=$1`, [yo]);
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
    foto: u.rows[0] ? u.rows[0].foto : null,
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
  const p = await pool.query(`SELECT id, nombre, precio::float, foto FROM productos WHERE negocio=$1 ORDER BY id`, [yo]);
  return { negocio: n.rows[0], productos: p.rows };
}

async function pedidosDe(yo) {
  const r = await pool.query(
    `SELECT p.id, p.detalle, p.total::float, p.estado, p.pagado, p.ts, COALESCE(ng.nombre, u.n) AS nombre
     FROM pedidos p JOIN usuarios u ON u.id=p.negocio LEFT JOIN negocios ng ON ng.id=p.negocio
     WHERE p.de=$1 ORDER BY p.ts DESC LIMIT 20`, [yo]);
  return r.rows.map(x => ({ ...x, ts: Number(x.ts) }));
}

async function pedidosNegocio(yo) {
  const r = await pool.query(
    `SELECT p.id, p.de, p.detalle, p.total::float, p.estado, p.pagado, p.ts, u.n AS nombre
     FROM pedidos p JOIN usuarios u ON u.id=p.de
     WHERE p.negocio=$1 ORDER BY p.ts DESC LIMIT 30`, [yo]);
  return r.rows.map(x => ({ ...x, ts: Number(x.ts) }));
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
  let eventosSeg = 0;
  const limitador = setInterval(() => { eventosSeg = 0; }, 1000);
  socket.use((_, next) => {
    eventosSeg++;
    if (eventosSeg > 30) return; // silenciosamente ignora el exceso
    next();
  });
  socket.on('disconnect', () => clearInterval(limitador));
  socket.on('boinear', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.para) return;
      const am = await pool.query(`SELECT 1 FROM amistades WHERE a=$1 AND b=$2`, [yo, d.para]);
      if (!am.rowCount) return;
      const yoN = await nombreDe(yo);
      const ts = Date.now();
      const texto = '⚡ ¡BOIN! Ya voy en camino 📍';
      await pool.query(`INSERT INTO mensajes (de,para,texto,ts) VALUES ($1,$2,$3,$4)`, [yo, d.para, texto, ts]);
      sendTo(d.para, 'chat', { de: yo, para: d.para, texto, ts });
      sendTo(yo, 'chat', { de: yo, para: d.para, texto, ts });
      await crearNotif(d.para, 'boin', '⚡🧡 ¡' + yoN + ' va en camino hacia ti!');
      await avisarEstado(d.para);
      socket.emit('aviso', '⚡ ¡Boin enviado! ' + (await nombreDe(d.para)) + ' sabe que vas en camino');
    } catch (e) { console.log('boinear error', e.message); }
  });

  // ---- contenido de las pestañas del perfil ----
  socket.on('perfil-tab', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.tab) return;
      let items = [];
      if (d.tab === 'momentos') {
        const r = await pool.query(`SELECT id, foto, texto, ts FROM momentos WHERE de=$1 ORDER BY ts DESC`, [yo]);
        items = r.rows;
      } else if (d.tab === 'guardados') {
        const r = await pool.query(`SELECT m.id, m.foto, m.texto, m.ts FROM guardados g JOIN momentos m ON m.id=g.momento WHERE g.uid=$1 ORDER BY g.ts DESC`, [yo]);
        items = r.rows;
      } else if (d.tab === 'gustas') {
        const r = await pool.query(`SELECT m.id, m.foto, m.texto, m.ts FROM likes l JOIN momentos m ON m.id=l.momento_id WHERE l.de=$1 ORDER BY m.ts DESC`, [yo]);
        items = r.rows;
      } else if (d.tab === 'compartidos') {
        const r = await pool.query(`SELECT id, foto, texto, ts FROM momentos WHERE de=$1 AND op1 IS NOT NULL ORDER BY ts DESC`, [yo]);
        items = r.rows;
      } else if (d.tab === 'destacados') {
        const r = await pool.query(`SELECT id, foto, nombre, precio FROM productos WHERE negocio=$1 AND foto IS NOT NULL ORDER BY id DESC`, [yo]);
        items = r.rows.map(x => ({ id: x.id, foto: x.foto, texto: x.nombre, precio: x.precio }));
      }
      socket.emit('perfil-tab', { tab: d.tab, items });
    } catch (e) {}
  });
  // ---- guardar / quitar de guardados ----
  socket.on('guardar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const ya = await pool.query(`SELECT 1 FROM guardados WHERE uid=$1 AND momento=$2`, [yo, d.id]);
      if (ya.rowCount) await pool.query(`DELETE FROM guardados WHERE uid=$1 AND momento=$2`, [yo, d.id]);
      else await pool.query(`INSERT INTO guardados VALUES ($1,$2,$3)`, [yo, d.id, Date.now()]);
      socket.emit('guardado', { id: d.id, on: !ya.rowCount });
    } catch (e) {}
  });

  socket.on('guardados', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const r = await pool.query(
        `SELECT m.id, m.de, u.n, m.texto, m.color, m.foto, m.ts
         FROM guardados g JOIN momentos m ON m.id=g.momento LEFT JOIN usuarios u ON u.id=m.de
         WHERE g.uid=$1 ORDER BY g.ts DESC LIMIT 50`, [yo]);
      socket.emit('guardados', r.rows.map(x => ({ ...x, ts: Number(x.ts) })));
    } catch (e) {}
  });

  // ---- quién dio like ----
  socket.on('likes-de', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const r = await pool.query(
        `SELECT u.id, u.n, u.foto FROM likes l JOIN usuarios u ON u.id=l.de WHERE l.momento_id=$1 LIMIT 50`, [d.id]);
      socket.emit('likes-de', { id: d.id, lista: r.rows });
    } catch (e) {}
  });

  // ---- encuestas ----
  socket.on('encuesta-set', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id || !d.op1 || !d.op2) return;
      await pool.query(`UPDATE momentos SET op1=$2, op2=$3 WHERE id=$1 AND de=$4`,
        [d.id, String(d.op1).slice(0, 40), String(d.op2).slice(0, 40), yo]);
    } catch (e) {}
  });

  socket.on('encuestas', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const r = await pool.query(
        `SELECT m.id, m.op1, m.op2,
           (SELECT COUNT(*) FROM enc_votos v WHERE v.momento=m.id AND v.op=1)::int AS v1,
           (SELECT COUNT(*) FROM enc_votos v WHERE v.momento=m.id AND v.op=2)::int AS v2,
           (SELECT op FROM enc_votos v WHERE v.momento=m.id AND v.de=$1) AS mivoto
         FROM momentos m WHERE m.op1 IS NOT NULL AND m.ts > $2`, [yo, Date.now() - 86400000]);
      socket.emit('encuestas', r.rows);
    } catch (e) {}
  });

  socket.on('encuesta-votar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const op = d.op === 2 ? 2 : 1;
      await pool.query(
        `INSERT INTO enc_votos (momento,de,op) VALUES ($1,$2,$3)
         ON CONFLICT (momento,de) DO UPDATE SET op=$3`, [d.id, yo, op]);
      const c = await pool.query(
        `SELECT (SELECT COUNT(*) FROM enc_votos WHERE momento=$1 AND op=1)::int AS v1,
                (SELECT COUNT(*) FROM enc_votos WHERE momento=$1 AND op=2)::int AS v2`, [d.id]);
      io.emit('encuesta', { id: d.id, v1: c.rows[0].v1, v2: c.rows[0].v2 });
    } catch (e) {}
  });

  socket.on('punto', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.lat || !d.lng) return;
      const yoN = await nombreDe(yo);
      puntosCache[yo] = { id: yo, n: yoN, lat: d.lat, lng: d.lng, ts: Date.now() };
      const ams = await pool.query(`SELECT b FROM amistades WHERE a=$1`, [yo]);
      for (const row of ams.rows) {
        sendTo(row.b, 'punto', puntosCache[yo]);
        await crearNotif(row.b, 'punto', '🚩 ' + yoN + ' marcó un punto de encuentro: ¡caigan ahí!');
      }
      socket.emit('punto', puntosCache[yo]);
      socket.emit('aviso', '🚩 Punto marcado: tus patas ya lo ven en su mapa (dura 2 h)');
    } catch (e) { console.log('punto error', e.message); }
  });

  socket.on('punto-off', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !puntosCache[yo]) return;
      delete puntosCache[yo];
      const ams = await pool.query(`SELECT b FROM amistades WHERE a=$1`, [yo]);
      ams.rows.forEach(row => sendTo(row.b, 'punto-off', { id: yo }));
      socket.emit('punto-off', { id: yo });
      socket.emit('aviso', '🚩 Punto de encuentro retirado');
    } catch (e) {}
  });

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
        `SELECT id, n, usuario, num, foto,
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
        foto: u.foto,
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
  socket.on('termometro', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const lat = Number(d && d.lat), lng = Number(d && d.lng);
      const tengoUbi = !isNaN(lat) && !isNaN(lng);
      const dist = (aLat, aLng, bLat, bLng) => {
        const R = 6371000, rad = Math.PI / 180;
        const dLat = (bLat - aLat) * rad, dLng = (bLng - aLng) * rad;
        const x = Math.sin(dLat / 2) ** 2 + Math.cos(aLat * rad) * Math.cos(bLat * rad) * Math.sin(dLng / 2) ** 2;
        return Math.round(R * 2 * Math.atan2(Math.sqrt(x), Math.sqrt(1 - x)));
      };
      const ahora = Date.now();
      const celdas = {};
      Object.values(ubiCache).forEach(u => {
        if (ahora - u.ts > 120000) return;
        const k = Math.round(u.lat / 0.003) + '_' + Math.round(u.lng / 0.003);
        celdas[k] = celdas[k] || { lat: 0, lng: 0, n: 0 };
        celdas[k].lat += u.lat; celdas[k].lng += u.lng; celdas[k].n++;
      });
      let zonas = Object.values(celdas).filter(c => c.n >= 2)
        .map(c => ({ lat: c.lat / c.n, lng: c.lng / c.n, n: c.n }));
      if (tengoUbi) zonas = zonas.map(z => ({ ...z, dist: dist(lat, lng, z.lat, z.lng) })).filter(z => z.dist < 5000);
      zonas.sort((a, b) => b.n - a.n);
      const m = await pool.query(`SELECT lat, lng FROM momentos WHERE ts > $1 AND lat IS NOT NULL`, [ahora - 3 * 3600 * 1000]);
      const momentos = tengoUbi ? m.rows.filter(x => dist(lat, lng, x.lat, x.lng) < 3000).length : m.rowCount;
      const ng = await pool.query(`SELECT lat, lng FROM negocios WHERE abierto=TRUE AND lat IS NOT NULL`);
      const abiertos = tengoUbi ? ng.rows.filter(x => dist(lat, lng, x.lat, x.lng) < 3000).length : ng.rowCount;
      const am = await pool.query(`SELECT b FROM amistades WHERE a=$1`, [yo]);
      const patasOnline = am.rows.filter(r => online[r.b] && online[r.b].size).length;
      const nivel = Math.min(100, zonas.reduce((s, z) => s + z.n * 12, 0) + momentos * 8 + abiertos * 4 + patasOnline * 10);
      socket.emit('termometro', { nivel, zonas: zonas.slice(0, 3), momentos, abiertos, patasOnline });
    } catch (e) { console.log('termometro error', e.message); }
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
      if (!yo || !msg || !msg.para) return;
      const texto = (msg.texto || '').slice(0, 500);
      const foto = (msg.foto && String(msg.foto).startsWith('https://')) ? String(msg.foto).slice(0, 500) : null;
      const audio = (msg.audio && String(msg.audio).startsWith('https://')) ? String(msg.audio).slice(0, 500) : null;
      const dur = audio ? Math.min(Math.round(Number(msg.dur) || 0), 600) : null;
      const cita = msg.cita ? String(msg.cita).slice(0, 140) : null;
      const lat = (typeof msg.lat === 'number' && typeof msg.lng === 'number') ? msg.lat : null;
      const lng = lat != null ? msg.lng : null;
      if (!texto && !foto && !audio && lat == null) return;
      if (!(await puedenChatear(yo, msg.para))) {
        socket.emit('aviso', 'Primero deben ser patas: envíale una solicitud 🤝');
        return;
      }
      const ts = Date.now();
      const r = await pool.query(
        `INSERT INTO mensajes (de,para,texto,ts,foto,cita,audio,dur,lat,lng) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
        [yo, msg.para, texto, ts, foto, cita, audio, dur, lat, lng]);
      const m = { id: r.rows[0].id, de: yo, para: msg.para, texto, ts, foto, cita, audio, dur, lat, lng, leido: false };
      sendTo(yo, 'chat', m);
      if (msg.para !== yo) sendTo(msg.para, 'chat', m);
      await avisarEstado(msg.para);
    } catch (e) { console.log('chat error', e.message); }
  });

  socket.on('qr-agregar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id || d.id === yo) return;
      const existe = await pool.query(`SELECT 1 FROM usuarios WHERE id=$1`, [d.id]);
      if (!existe.rowCount) { socket.emit('aviso', 'Ese código no es de TOKA 🤔'); return; }
      if (await hayBloqueo(yo, d.id)) return;
      const ya = await pool.query(`SELECT 1 FROM amistades WHERE a=$1 AND b=$2`, [yo, d.id]);
      if (ya.rowCount) { socket.emit('aviso', '¡Ya son patas! 🧡'); return; }
      await pool.query(`INSERT INTO amistades VALUES ($1,$2),($2,$1) ON CONFLICT DO NOTHING`, [yo, d.id]);
      await pool.query(`INSERT INTO comparto VALUES ($1,$2),($2,$1) ON CONFLICT DO NOTHING`, [yo, d.id]);
      await pool.query(`DELETE FROM solicitudes WHERE (de=$1 AND para=$2) OR (de=$2 AND para=$1)`, [yo, d.id]);
      const yoN = await nombreDe(yo), otroN = await nombreDe(d.id);
      await crearNotif(d.id, 'pata', '🤝 ' + yoN + ' escaneó tu huella: ¡ya son patas!');
      await avisarEstado(yo);
      await avisarEstado(d.id);
      socket.emit('aviso', '🤝 ¡Listo! Tú y ' + otroN + ' ya son patas');
    } catch (e) { console.log('qr-agregar error', e.message); }
  });

  socket.on('chats', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const est = await estadoDe(yo);
      const prefs = await pool.query(`SELECT con, fijado, silencio, archivado FROM chat_prefs WHERE uid=$1`, [yo]);
      const P = {}; prefs.rows.forEach(x => { P[x.con] = x; });
      const lista = [];
      for (const a of (est.amigos || [])) {
        const um = await pool.query(
          `SELECT de, texto, foto, audio, dur, plata, ts, lat, lng, cobro, cobrado, leido FROM mensajes
           WHERE (de=$1 AND para=$2) OR (de=$2 AND para=$1) ORDER BY ts DESC LIMIT 1`, [yo, a.id]);
        const u = um.rows[0];
        lista.push({
          tipo: 'amigo', ...a,
          ultimoMsg: u ? { de: u.de, texto: u.texto, foto: u.foto, audio: u.audio, dur: u.dur, plata: u.plata ? Number(u.plata) : null, ts: Number(u.ts), leido: u.leido, lat: u.lat, lng: u.lng, cobro: u.cobro ? Number(u.cobro) : null, cobrado: u.cobrado } : null,
          fijado: !!(P[a.id] && P[a.id].fijado),
          silencio: !!(P[a.id] && P[a.id].silencio),
          archivado: !!(P[a.id] && P[a.id].archivado),
        });
      }
      const gs = await gruposDe(yo);
      for (const g of (gs || [])) {
        const um = await pool.query(
          `SELECT mg.de, u.n AS den, mg.texto, mg.foto, mg.audio, mg.dur, mg.ts, mg.lat, mg.lng, mg.plan, mg.plan_hora, mg.chancha
           FROM mensajes_grupo mg LEFT JOIN usuarios u ON u.id=mg.de
           WHERE mg.grupo=$1 ORDER BY mg.ts DESC LIMIT 1`, [g.id]);
        const u = um.rows[0];
        const gk = 'g' + g.id;
        lista.push({
          tipo: 'grupo', ...g,
          ultimoMsg: u ? { de: u.de, deN: u.den, texto: u.texto, foto: u.foto, audio: u.audio, dur: u.dur, ts: Number(u.ts), lat: u.lat, lng: u.lng, plan: u.plan, plan_hora: u.plan_hora, chancha: u.chancha } : null,
          fijado: !!(P[gk] && P[gk].fijado),
          silencio: !!(P[gk] && P[gk].silencio),
          archivado: !!(P[gk] && P[gk].archivado),
        });
      }
      socket.emit('chats', lista);
    } catch (e) { console.log('chats error', e.message); }
  });

  socket.on('chat-media', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.con) return;
      const f = await pool.query(
        `SELECT foto FROM mensajes WHERE ((de=$1 AND para=$2) OR (de=$2 AND para=$1)) AND foto IS NOT NULL
         ORDER BY ts DESC LIMIT 60`, [yo, d.con]);
      const c = await pool.query(
        `SELECT COUNT(*)::int AS total,
                COUNT(*) FILTER (WHERE audio IS NOT NULL)::int AS audios,
                COUNT(*) FILTER (WHERE plata IS NOT NULL)::int AS platas
         FROM mensajes WHERE (de=$1 AND para=$2) OR (de=$2 AND para=$1)`, [yo, d.con]);
      socket.emit('chat-media', { con: d.con, fotos: f.rows.map(x => x.foto), ...c.rows[0] });
    } catch (e) {}
  });

  socket.on('chat-vaciar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.con) return;
      await pool.query(`DELETE FROM mensajes WHERE (de=$1 AND para=$2) OR (de=$2 AND para=$1)`, [yo, d.con]);
      sendTo(yo, 'chat-vaciado', { con: d.con });
      sendTo(d.con, 'chat-vaciado', { con: yo });
      await avisarEstado(yo);
      await avisarEstado(d.con);
    } catch (e) {}
  });

  socket.on('chat-pref', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.con) return;
      await pool.query(
        `INSERT INTO chat_prefs (uid,con,fijado,silencio,archivado,favorito,restringido) VALUES ($1,$2,$3,$4,$5,$6,$7)
         ON CONFLICT (uid,con) DO UPDATE SET fijado=$3, silencio=$4, archivado=$5, favorito=$6, restringido=$7`,
        [yo, String(d.con), !!d.fijado, !!d.silencio, !!d.archivado, !!d.favorito, !!d.restringido]);
    } catch (e) {}
  });

  // ---- reacción a mensaje de grupo ----
  socket.on('gmsg-reaccion', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      await pool.query(`UPDATE mensajes_grupo SET reaccion=$1 WHERE id=$2`, [d.emoji || null, d.id]);
      const g = await pool.query(`SELECT grupo FROM mensajes_grupo WHERE id=$1`, [d.id]);
      if (!g.rows[0]) return;
      const miembros = await pool.query(`SELECT uid FROM grupo_miembros WHERE grupo=$1`, [g.rows[0].grupo]);
      miembros.rows.forEach(m => sendTo(m.uid, 'gmsg-reaccion', { id: d.id, emoji: d.emoji || null }));
    } catch (e) {}
  });

  // ---- eliminar chat SOLO para mí (el otro no se entera) ----
  socket.on('chat-eliminar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.con) return;
      await pool.query(
        `DELETE FROM mensajes WHERE (de=$1 AND para=$2) OR (de=$2 AND para=$1)`,
        [yo, String(d.con)]);
      sendTo(yo, 'chat-eliminado', { con: String(d.con) });
    } catch (e) {}
  });

  socket.on('chat-plata', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.para) return;
      const monto = Math.round((Number(d.monto) || 0) * 100) / 100;
      if (monto <= 0 || monto > 500) { socket.emit('aviso', 'Monto inválido (máx S/ 500 por chat)'); return; }
      if (!(await puedenChatear(yo, d.para))) { socket.emit('aviso', 'Solo puedes boinear plata a tus patas 🤝'); return; }
      const saldo = await saldoDe(yo);
      if (saldo < monto) { socket.emit('aviso', '😅 Saldo insuficiente: tienes S/ ' + saldo.toFixed(2)); return; }
      const yoN = await nombreDe(yo);
      const paraN = await nombreDe(d.para);
      const ts = Date.now();
      await pool.query(`INSERT INTO wallet_mov (uid,tipo,monto,detalle,ts) VALUES ($1,'envio',$2,$3,$4)`,
        [yo, monto, 'Enviaste a ' + paraN + ' 💬', ts]);
      await pool.query(`INSERT INTO wallet_mov (uid,tipo,monto,detalle,ts) VALUES ($1,'recibido',$2,$3,$4)`,
        [d.para, monto, 'Te envió ' + yoN + ' 💬🧡', ts]);
      const r = await pool.query(
        `INSERT INTO mensajes (de,para,texto,ts,plata) VALUES ($1,$2,'',$3,$4) RETURNING id`,
        [yo, d.para, ts, monto]);
      const m = { id: r.rows[0].id, de: yo, para: d.para, texto: '', ts, plata: monto, leido: false };
      sendTo(yo, 'chat', m);
      sendTo(d.para, 'chat', m);
      socket.emit('wallet', await walletDe(yo));
      sendTo(d.para, 'wallet', await walletDe(d.para));
      await crearNotif(d.para, 'wallet', '💸 ' + yoN + ' te boineó S/ ' + monto.toFixed(2) + ' por el chat');
      await avisarEstado(d.para);
    } catch (e) { console.log('chat-plata error', e.message); }
  });

  socket.on('msg-editar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id || !d.texto) return;
      const m = await pool.query(`SELECT de, para, ts, plata FROM mensajes WHERE id=$1`, [d.id]);
      if (!m.rowCount || m.rows[0].de !== yo || m.rows[0].plata) return;
      if (Date.now() - Number(m.rows[0].ts) > 15 * 60 * 1000) {
        socket.emit('aviso', '⏱️ Solo puedes editar mensajes de los últimos 15 min');
        return;
      }
      const texto = String(d.texto).slice(0, 500);
      await pool.query(`UPDATE mensajes SET texto=$2, editado=TRUE WHERE id=$1`, [d.id, texto]);
      sendTo(yo, 'msg-editado', { id: d.id, texto });
      sendTo(m.rows[0].para, 'msg-editado', { id: d.id, texto });
    } catch (e) {}
  });

  socket.on('historial', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.con) return;
      const r = await pool.query(
        `SELECT id, de, para, texto, ts, foto, cita, leido, reaccion, audio, dur, plata, editado, lat, lng, cobro, cobrado FROM mensajes
         WHERE (de=$1 AND para=$2) OR (de=$2 AND para=$1)
         ORDER BY ts DESC LIMIT 50`, [yo, d.con]);
      socket.emit('historial', {
        con: d.con,
        lista: r.rows.reverse().map(x => ({ ...x, ts: Number(x.ts), plata: x.plata ? Number(x.plata) : null, cobro: x.cobro ? Number(x.cobro) : null, dur: x.dur ? Number(x.dur) : null, lat: x.lat, lng: x.lng })),
      });
      await pool.query(`UPDATE mensajes SET leido=TRUE WHERE de=$2 AND para=$1 AND leido=FALSE`, [yo, d.con]);
      sendTo(d.con, 'leidos', { por: yo });
      await avisarEstado(yo);
    } catch (e) { console.log('historial error', e.message); }
  });
  socket.on('msg-borrar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const m = await pool.query(`SELECT de, para FROM mensajes WHERE id=$1`, [d.id]);
      if (!m.rowCount || m.rows[0].de !== yo) return;
      await pool.query(`UPDATE mensajes SET texto='🚫 Mensaje eliminado', foto=NULL, cita=NULL WHERE id=$1`, [d.id]);
      sendTo(yo, 'msg-borrado', { id: d.id });
      sendTo(m.rows[0].para, 'msg-borrado', { id: d.id });
    } catch (e) {}
  });

  socket.on('plan-crear', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.grupo || !d.titulo || !d.titulo.trim()) return;
      const soy = await pool.query(`SELECT 1 FROM grupo_miembros WHERE grupo=$1 AND uid=$2`, [d.grupo, yo]);
      if (!soy.rowCount) return;
      const lat = (typeof d.lat === 'number' && typeof d.lng === 'number') ? d.lat : null;
      const lng = lat != null ? d.lng : null;
      const ts = Date.now();
      const titulo = String(d.titulo).trim().slice(0, 60);
      const hora = String(d.hora || '').slice(0, 20);
      const r = await pool.query(
        `INSERT INTO mensajes_grupo (grupo,de,texto,ts,lat,lng,plan,plan_hora) VALUES ($1,$2,'',$3,$4,$5,$6,$7) RETURNING id`,
        [d.grupo, yo, ts, lat, lng, titulo, hora]);
      const m = { id: r.rows[0].id, grupo: d.grupo, de: yo, deN: await nombreDe(yo), texto: '', ts, lat, lng, plan: titulo, plan_hora: hora, si: 0, no: 0, mivoto: null };
      const miembros = await pool.query(`SELECT uid FROM grupo_miembros WHERE grupo=$1`, [d.grupo]);
      for (const row of miembros.rows) {
        sendTo(row.uid, 'grupo-chat', m);
        if (row.uid !== yo) await crearNotif(row.uid, 'punto', '📍 ' + m.deN + ' propuso un plan: «' + titulo + '»' + (hora ? ' a las ' + hora : ''));
      }
    } catch (e) { console.log('plan-crear error', e.message); }
  });

  socket.on('plan-votar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const m = await pool.query(`SELECT grupo, plan, lat, lng FROM mensajes_grupo WHERE id=$1`, [d.id]);
      if (!m.rowCount || !m.rows[0].plan) return;
      const soy = await pool.query(`SELECT 1 FROM grupo_miembros WHERE grupo=$1 AND uid=$2`, [m.rows[0].grupo, yo]);
      if (!soy.rowCount) return;
      await pool.query(
        `INSERT INTO plan_votos (msg,uid,voy) VALUES ($1,$2,$3) ON CONFLICT (msg,uid) DO UPDATE SET voy=$3`,
        [d.id, yo, !!d.voy]);
      const c = await pool.query(
        `SELECT COUNT(*) FILTER (WHERE voy)::int AS si, COUNT(*) FILTER (WHERE NOT voy)::int AS no FROM plan_votos WHERE msg=$1`, [d.id]);
      const miembros = await pool.query(`SELECT uid FROM grupo_miembros WHERE grupo=$1`, [m.rows[0].grupo]);
      miembros.rows.forEach(row => sendTo(row.uid, 'plan-votos', { id: d.id, si: c.rows[0].si, no: c.rows[0].no }));
      if (d.voy && m.rows[0].lat != null) {
        sendTo(yo, 'punto', { id: 'plan' + d.id, n: m.rows[0].plan, lat: m.rows[0].lat, lng: m.rows[0].lng, ts: Date.now() });
        socket.emit('aviso', '🚩 El punto del plan ya está en tu mapa');
      }
    } catch (e) { console.log('plan-votar error', e.message); }
  });

  socket.on('chancha-crear', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.grupo || !d.motivo || !d.motivo.trim()) return;
      const soy = await pool.query(`SELECT 1 FROM grupo_miembros WHERE grupo=$1 AND uid=$2`, [d.grupo, yo]);
      if (!soy.rowCount) return;
      const meta = Math.min(2000, Math.max(0, Math.round((Number(d.meta) || 0) * 100) / 100)) || null;
      const motivo = String(d.motivo).trim().slice(0, 60);
      const ts = Date.now();
      const r = await pool.query(
        `INSERT INTO mensajes_grupo (grupo,de,texto,ts,chancha,chancha_meta) VALUES ($1,$2,'',$3,$4,$5) RETURNING id`,
        [d.grupo, yo, ts, motivo, meta]);
      const yoN = await nombreDe(yo);
      const m = { id: r.rows[0].id, grupo: d.grupo, de: yo, deN: yoN, texto: '', ts, chancha: motivo, chanchaMeta: meta, chanchaTotal: 0, miAporte: 0 };
      const miembros = await pool.query(`SELECT uid FROM grupo_miembros WHERE grupo=$1`, [d.grupo]);
      for (const row of miembros.rows) {
        sendTo(row.uid, 'grupo-chat', m);
        if (row.uid !== yo) await crearNotif(row.uid, 'wallet', '🐷 ' + yoN + ' abrió una chanchita: «' + motivo + '»' + (meta ? ' — meta S/ ' + meta.toFixed(2) : ''));
      }
    } catch (e) { console.log('chancha-crear error', e.message); }
  });

  socket.on('chancha-aportar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const monto = Math.round((Number(d.monto) || 0) * 100) / 100;
      if (monto <= 0 || monto > 500) { socket.emit('aviso', 'Monto inválido (máx S/ 500 por aporte)'); return; }
      const m = await pool.query(`SELECT grupo, de, chancha FROM mensajes_grupo WHERE id=$1`, [d.id]);
      if (!m.rowCount || !m.rows[0].chancha) return;
      const soy = await pool.query(`SELECT 1 FROM grupo_miembros WHERE grupo=$1 AND uid=$2`, [m.rows[0].grupo, yo]);
      if (!soy.rowCount) return;
      const duenio = m.rows[0].de;
      if (duenio === yo) { socket.emit('aviso', 'La chanchita es tuya: los aportes llegan directo a tu Wallet 🐷'); return; }
      const saldo = await saldoDe(yo);
      if (saldo < monto) { socket.emit('aviso', '😅 Saldo insuficiente: tienes S/ ' + saldo.toFixed(2)); return; }
      const yoN = await nombreDe(yo);
      const ts = Date.now();
      await pool.query(`INSERT INTO wallet_mov (uid,tipo,monto,detalle,ts) VALUES ($1,'envio',$2,$3,$4)`,
        [yo, monto, '🐷 Aporte a «' + m.rows[0].chancha + '»', ts]);
      await pool.query(`INSERT INTO wallet_mov (uid,tipo,monto,detalle,ts) VALUES ($1,'recibido',$2,$3,$4)`,
        [duenio, monto, '🐷 ' + yoN + ' aportó a «' + m.rows[0].chancha + '»', ts]);
      await pool.query(
        `INSERT INTO chancha_aportes (msg,uid,monto,ts) VALUES ($1,$2,$3,$4)
         ON CONFLICT (msg,uid) DO UPDATE SET monto = chancha_aportes.monto + $3, ts=$4`,
        [d.id, yo, monto, ts]);
      const tot = await pool.query(`SELECT COALESCE(SUM(monto),0)::float AS total, COUNT(*)::int AS n FROM chancha_aportes WHERE msg=$1`, [d.id]);
      const miembros = await pool.query(`SELECT uid FROM grupo_miembros WHERE grupo=$1`, [m.rows[0].grupo]);
      miembros.rows.forEach(row => sendTo(row.uid, 'chancha-total', { id: d.id, total: tot.rows[0].total, aportantes: tot.rows[0].n }));
      socket.emit('wallet', await walletDe(yo));
      sendTo(duenio, 'wallet', await walletDe(duenio));
      await crearNotif(duenio, 'wallet', '🐷 ' + yoN + ' aportó S/ ' + monto.toFixed(2) + ' a tu chanchita');
    } catch (e) { console.log('chancha-aportar error', e.message); }
  });

  // ===== FASE 6: VAMOS JUNTOS =====
  socket.on('encamino', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const yoN = await nombreDe(yo);
      if (String(d.id).startsWith('plan')) {
        const msgId = Number(String(d.id).slice(4));
        const m = await pool.query(`SELECT grupo, plan FROM mensajes_grupo WHERE id=$1`, [msgId]);
        if (!m.rowCount) return;
        const miembros = await pool.query(`SELECT uid FROM grupo_miembros WHERE grupo=$1`, [m.rows[0].grupo]);
        for (const row of miembros.rows) {
          if (row.uid === yo) continue;
          sendTo(row.uid, 'pata-rumbo', { n: yoN, estado: 'encamino' });
          await crearNotif(row.uid, 'punto', '🏃 ' + yoN + ' ya va en camino a «' + m.rows[0].plan + '»');
        }
      } else {
        sendTo(d.id, 'pata-rumbo', { n: yoN, estado: 'encamino' });
        await crearNotif(d.id, 'punto', '🏃 ' + yoN + ' va en camino a tu punto de encuentro');
      }
    } catch (e) { console.log('encamino error', e.message); }
  });

  socket.on('llegue', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const yoN = await nombreDe(yo);
      if (String(d.id).startsWith('plan')) {
        const msgId = Number(String(d.id).slice(4));
        const m = await pool.query(`SELECT grupo, plan FROM mensajes_grupo WHERE id=$1`, [msgId]);
        if (!m.rowCount) return;
        const miembros = await pool.query(`SELECT uid FROM grupo_miembros WHERE grupo=$1`, [m.rows[0].grupo]);
        for (const row of miembros.rows) {
          if (row.uid === yo) continue;
          sendTo(row.uid, 'pata-rumbo', { n: yoN, estado: 'llego' });
          await crearNotif(row.uid, 'punto', '🎉 ' + yoN + ' YA LLEGÓ a «' + m.rows[0].plan + '»');
        }
      } else {
        sendTo(d.id, 'pata-rumbo', { n: yoN, estado: 'llego' });
        await crearNotif(d.id, 'punto', '🎉 ' + yoN + ' YA LLEGÓ a tu punto de encuentro');
      }
    } catch (e) { console.log('llegue error', e.message); }
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

  socket.on('chat-cobrar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.para) return;
      const monto = Math.round((Number(d.monto) || 0) * 100) / 100;
      if (monto <= 0 || monto > 500) { socket.emit('aviso', 'Monto inválido (máx S/ 500)'); return; }
      if (!(await puedenChatear(yo, d.para))) return;
      const ts = Date.now();
      const r = await pool.query(
        `INSERT INTO mensajes (de,para,texto,ts,cobro) VALUES ($1,$2,'',$3,$4) RETURNING id`,
        [yo, d.para, ts, monto]);
      const m = { id: r.rows[0].id, de: yo, para: d.para, texto: '', ts, cobro: monto, cobrado: false, leido: false };
      sendTo(yo, 'chat', m);
      sendTo(d.para, 'chat', m);
      await crearNotif(d.para, 'wallet', '💰 ' + (await nombreDe(yo)) + ' te cobra S/ ' + monto.toFixed(2));
      await avisarEstado(d.para);
    } catch (e) { console.log('chat-cobrar error', e.message); }
  });

  socket.on('cobro-pagar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const m = await pool.query(`SELECT de, para, cobro, cobrado FROM mensajes WHERE id=$1`, [d.id]);
      if (!m.rowCount || !m.rows[0].cobro || m.rows[0].cobrado || m.rows[0].para !== yo) return;
      const monto = Number(m.rows[0].cobro);
      const cobrador = m.rows[0].de;
      const saldo = await saldoDe(yo);
      if (saldo < monto) { socket.emit('aviso', '😅 Saldo insuficiente: tienes S/ ' + saldo.toFixed(2)); return; }
      const yoN = await nombreDe(yo);
      const cobN = await nombreDe(cobrador);
      const ts = Date.now();
      await pool.query(`INSERT INTO wallet_mov (uid,tipo,monto,detalle,ts) VALUES ($1,'envio',$2,$3,$4)`,
        [yo, monto, 'Pagaste a ' + cobN + ' 💰', ts]);
      await pool.query(`INSERT INTO wallet_mov (uid,tipo,monto,detalle,ts) VALUES ($1,'recibido',$2,$3,$4)`,
        [cobrador, monto, yoN + ' te pagó 💰', ts]);
      await pool.query(`UPDATE mensajes SET cobrado=TRUE WHERE id=$1`, [d.id]);
      sendTo(yo, 'cobro-pagado', { id: d.id });
      sendTo(cobrador, 'cobro-pagado', { id: d.id });
      socket.emit('wallet', await walletDe(yo));
      sendTo(cobrador, 'wallet', await walletDe(cobrador));
      await crearNotif(cobrador, 'wallet', '💰 ' + yoN + ' pagó tu cobro de S/ ' + monto.toFixed(2));
    } catch (e) { console.log('cobro-pagar error', e.message); }
  });

  socket.on('grupo-chat', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.grupo) return;
      const texto = (d.texto || '').trim().slice(0, 500);
      const foto = (d.foto && String(d.foto).startsWith('https://')) ? String(d.foto).slice(0, 500) : null;
      const audio = (d.audio && String(d.audio).startsWith('https://')) ? String(d.audio).slice(0, 500) : null;
      const dur = audio ? Math.min(Math.round(Number(d.dur) || 0), 600) : null;
      const cita = d.cita ? String(d.cita).slice(0, 140) : null;
      if (!texto && !foto && !audio && d.lat == null) return;
      const soy = await pool.query(`SELECT 1 FROM grupo_miembros WHERE grupo=$1 AND uid=$2`, [d.grupo, yo]);
      if (!soy.rowCount) return;
      const lat = (typeof d.lat === 'number' && typeof d.lng === 'number') ? d.lat : null;
      const lng = lat != null ? d.lng : null;

      const ts = Date.now();
      const r = await pool.query(
        `INSERT INTO mensajes_grupo (grupo,de,texto,ts,foto,cita,audio,dur,lat,lng) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
        [d.grupo, yo, texto, ts, foto, cita, audio, dur, lat, lng]);
      const m = { id: r.rows[0].id, grupo: d.grupo, de: yo, deN: await nombreDe(yo), texto, ts, foto, cita, audio, dur, lat, lng };
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
        `SELECT mg.id, mg.de, u.n AS den, mg.texto, mg.ts, mg.foto, mg.cita, mg.audio, mg.dur, mg.lat, mg.lng, mg.plan, mg.plan_hora, mg.chancha, mg.chancha_meta,
           (SELECT COUNT(*) FROM plan_votos v WHERE v.msg=mg.id AND v.voy)::int AS si,
           (SELECT COUNT(*) FROM plan_votos v WHERE v.msg=mg.id AND NOT v.voy)::int AS no,
           (SELECT voy FROM plan_votos v WHERE v.msg=mg.id AND v.uid=$2) AS mivoto,
           (SELECT COALESCE(SUM(monto),0)::float FROM chancha_aportes ca WHERE ca.msg=mg.id) AS chancha_total,
           (SELECT COUNT(*)::int FROM chancha_aportes ca WHERE ca.msg=mg.id) AS aportantes,
           (SELECT monto::float FROM chancha_aportes ca WHERE ca.msg=mg.id AND ca.uid=$2) AS mi_aporte
         FROM mensajes_grupo mg LEFT JOIN usuarios u ON u.id=mg.de
         WHERE mg.grupo=$1 ORDER BY mg.ts DESC LIMIT 50`, [d.grupo, yo]);
      socket.emit('grupo-historial', {
        grupo: d.grupo,
        lista: r.rows.reverse().map(x => ({
          id: x.id, de: x.de, deN: x.den, texto: x.texto, ts: Number(x.ts),
          foto: x.foto, cita: x.cita, audio: x.audio, dur: x.dur, lat: x.lat, lng: x.lng,
          plan: x.plan, plan_hora: x.plan_hora, si: x.si, no: x.no, mivoto: x.mivoto,
          chancha: x.chancha, chanchaMeta: x.chancha_meta ? Number(x.chancha_meta) : null,
          chanchaTotal: x.chancha_total, aportantes: x.aportantes, miAporte: x.mi_aporte ? Number(x.mi_aporte) : 0,
        })),
      });
    } catch (e) { console.log('grupo-historial error', e.message); }
  });

  socket.on('gmsg-borrar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const m = await pool.query(`SELECT de, grupo FROM mensajes_grupo WHERE id=$1`, [d.id]);
      if (!m.rowCount || m.rows[0].de !== yo) return;
      await pool.query(`UPDATE mensajes_grupo SET texto='🚫 Mensaje eliminado', foto=NULL, cita=NULL WHERE id=$1`, [d.id]);
      const miembros = await pool.query(`SELECT uid FROM grupo_miembros WHERE grupo=$1`, [m.rows[0].grupo]);
      miembros.rows.forEach(row => sendTo(row.uid, 'gmsg-borrado', { id: d.id }));
    } catch (e) {}
  });

  socket.on('msg-reaccion', async (d) => {
    try {
      const yo = socketDe[socket.id];
      const OK = ['🧡', '😂', '😮', '👍'];
      if (!yo || !d || !d.id || !OK.includes(d.emoji)) return;
      const m = await pool.query(`SELECT de, para FROM mensajes WHERE id=$1`, [d.id]);
      if (!m.rowCount || (m.rows[0].de !== yo && m.rows[0].para !== yo)) return;
      await pool.query(`UPDATE mensajes SET reaccion=$2 WHERE id=$1`, [d.id, d.emoji]);
      sendTo(m.rows[0].de, 'msg-reaccion', { id: d.id, emoji: d.emoji });
      sendTo(m.rows[0].para, 'msg-reaccion', { id: d.id, emoji: d.emoji });
    } catch (e) {}
  });

  socket.on('grupo-miembros', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.grupo) return;
      const soy = await pool.query(`SELECT 1 FROM grupo_miembros WHERE grupo=$1 AND uid=$2`, [d.grupo, yo]);
      if (!soy.rowCount) return;
      const r = await pool.query(
        `SELECT gm.uid, u.n, u.foto FROM grupo_miembros gm JOIN usuarios u ON u.id=gm.uid WHERE gm.grupo=$1`, [d.grupo]);
      socket.emit('grupo-miembros', {
        grupo: d.grupo,
        lista: r.rows.map(x => ({ id: x.uid, n: x.n, foto: x.foto, online: !!(online[x.uid] && online[x.uid].size) })),
      });
    } catch (e) {}
  });

  socket.on('grupo-salir', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.grupo) return;
      await pool.query(`DELETE FROM grupo_miembros WHERE grupo=$1 AND uid=$2`, [d.grupo, yo]);
      socket.emit('grupos', await gruposDe(yo));
      socket.emit('aviso', '👋 Saliste del grupo');
      const miembros = await pool.query(`SELECT uid FROM grupo_miembros WHERE grupo=$1`, [d.grupo]);
      for (const row of miembros.rows) sendTo(row.uid, 'grupos', await gruposDe(row.uid));
    } catch (e) {}
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
      const uq = await pool.query(`SELECT n, foto FROM usuarios WHERE id=$1`, [yo]);
      const n = uq.rowCount ? uq.rows[0].n : 'Pata';
      const m = { id: r.rows[0].id, de: yo, n, ufoto: uq.rowCount ? uq.rows[0].foto : null, texto, color: d.color || 0, ts, foto, lat, lng, likes: 0, coms: 0, meGusta: false };
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
        `SELECT m.id, m.de, u.n, u.foto AS ufoto, m.texto, m.color, m.ts, m.foto, 
           (SELECT COUNT(*) FROM likes l WHERE l.momento_id=m.id)::int AS likes,
           (SELECT COUNT(*) FROM comentarios c WHERE c.momento_id=m.id)::int AS coms,
           EXISTS(SELECT 1 FROM likes l WHERE l.momento_id=m.id AND l.de=$1) AS megusta
         FROM momentos m JOIN usuarios u ON u.id=m.de
         WHERE m.de=$1 OR EXISTS(SELECT 1 FROM amistades a WHERE a.a=$1 AND a.b=m.de)
         ORDER BY m.ts DESC LIMIT 30`, [yo]);
      socket.emit('feed', r.rows.map(x => ({ id: x.id, de: x.de, n: x.n, ufoto: x.ufoto, texto: x.texto, color: x.color, ts: Number(x.ts), foto: x.foto, likes: x.likes, coms: x.coms, meGusta: x.megusta })));
    } catch (e) { console.log('feed error', e.message); }
  });

  socket.on('momentos-mapa', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const hace24h = Date.now() - 24 * 3600 * 1000;
      const r = await pool.query(
        `SELECT m.id, m.de, u.n, u.foto AS ufoto, m.texto, m.foto, m.lat, m.lng, m.ts
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
        `SELECT m.id, m.de, u.n, u.foto AS ufoto, m.texto, m.color, m.foto, m.ts
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
        `SELECT u.n, u.usuario, u.bio, u.tipo, u.foto, ng.nombre AS bizn, ng.descr AS bizd, ng.cat AS bizc
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
        id: d.id, n: x.n, usuario: x.usuario, bio: x.bio || '', tipo: x.tipo || 'personal', foto: x.foto || null,
        biz: x.bizn ? { nombre: x.bizn, descr: x.bizd, cat: x.bizc } : null,
        patas: pa.rows[0].c, momentos: mo.rows[0].c, likes: li.rows[0].c,
        esAmigo, pendiente: !!pe.rowCount,
        online: !!(online[d.id] && online[d.id].size),
        moms,
      });
    } catch (e) { console.log('perfil-de error', e.message); }
  });

  socket.on('foto-perfil', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.url || !String(d.url).startsWith('https://')) return;
      await pool.query(`UPDATE usuarios SET foto=$2 WHERE id=$1`, [yo, String(d.url).slice(0, 500)]);
      socket.emit('perfil', await perfilDe(yo));
      socket.emit('aviso', '📸 Foto de perfil actualizada');
    } catch (e) {}
  });

  socket.on('animal', async (d) => {
    try {
      const yo = socketDe[socket.id];
      const OK = ['cuy', 'pinguino', 'tiburon', 'leon', 'huron', 'cocodrilo'];
      if (!yo || !d || !OK.includes(d.k)) return;
      await pool.query(`UPDATE usuarios SET animal=$2 WHERE id=$1`, [yo, d.k]);
      socket.emit('aviso', '🐾 ¡Nuevo compañero elegido! Tus patas ya lo ven en su mapa');
    } catch (e) {}
  });

  socket.on('animales', async () => {
    try {
      const yo = socketDe[socket.id];
      if (!yo) return;
      const r = await pool.query(
        `SELECT u.id, u.animal FROM amistades a JOIN usuarios u ON u.id=a.b WHERE a.a=$1`, [yo]);
      const m = {};
      r.rows.forEach(x => { m[x.id] = x.animal || 'cuy'; });
      const me = await pool.query(`SELECT animal FROM usuarios WHERE id=$1`, [yo]);
      m[yo] = (me.rows[0] && me.rows[0].animal) || 'cuy';
      socket.emit('animales', m);
    } catch (e) {}
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

  socket.on('pedido-repetir', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const p = await pool.query(`SELECT de, negocio, detalle, total::float FROM pedidos WHERE id=$1`, [d.id]);
      if (!p.rowCount || p.rows[0].de !== yo) return;
      const negocio = p.rows[0].negocio, detalle = p.rows[0].detalle, total = p.rows[0].total;
      const ts = Date.now();
      const np = await pool.query(
        `INSERT INTO pedidos (de,negocio,detalle,total,ts) VALUES ($1,$2,$3,$4,$5) RETURNING id`,
        [yo, negocio, detalle, total, ts]);
      const pid = np.rows[0].id;
      const texto = '🛍️ PEDIDO #' + pid + ': ' + detalle + ' — Total S/ ' + total.toFixed(2) + ' 🔁';
      await pool.query(`INSERT INTO mensajes (de,para,texto,ts) VALUES ($1,$2,$3,$4)`, [yo, negocio, texto, ts]);
      sendTo(negocio, 'chat', { de: yo, para: negocio, texto, ts });
      sendTo(yo, 'chat', { de: yo, para: negocio, texto, ts });
      await crearNotif(negocio, 'pedido', '🔁 ' + (await nombreDe(yo)) + ' repitió su pedido — S/ ' + total.toFixed(2));
      socket.emit('pedidos-mios', await pedidosDe(yo));
      sendTo(negocio, 'pedidos-negocio', await pedidosNegocio(negocio));
      await avisarEstado(yo);
      await avisarEstado(negocio);
      socket.emit('aviso', '🔁 ¡Pedido repetido! Es el #' + pid);
    } catch (e) { console.log('pedido-repetir error', e.message); }
  });

  socket.on('resenas', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const r = await pool.query(
        `SELECT r.de, u.n, r.estrellas, r.texto, r.ts FROM resenas r JOIN usuarios u ON u.id=r.de
         WHERE r.negocio=$1 ORDER BY r.ts DESC LIMIT 30`, [d.id]);
      const puedo = await pool.query(`SELECT 1 FROM pedidos WHERE de=$1 AND negocio=$2 LIMIT 1`, [yo, d.id]);
      const mia = await pool.query(`SELECT estrellas, texto FROM resenas WHERE negocio=$1 AND de=$2`, [d.id, yo]);
      socket.emit('resenas', {
        id: d.id,
        lista: r.rows.map(x => ({ ...x, ts: Number(x.ts) })),
        puedo: !!puedo.rowCount,
        mia: mia.rowCount ? mia.rows[0] : null,
      });
    } catch (e) { console.log('resenas error', e.message); }
  });

  socket.on('resena-poner', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.negocio) return;
      const est = Math.min(5, Math.max(1, Math.round(Number(d.estrellas) || 0)));
      const pidio = await pool.query(`SELECT 1 FROM pedidos WHERE de=$1 AND negocio=$2 LIMIT 1`, [yo, d.negocio]);
      if (!pidio.rowCount) { socket.emit('aviso', '🔒 Solo pueden reseñar quienes ya pidieron aquí (huella verificada)'); return; }
      await pool.query(
        `INSERT INTO resenas (negocio,de,estrellas,texto,ts) VALUES ($1,$2,$3,$4,$5)
         ON CONFLICT (negocio,de) DO UPDATE SET estrellas=$3, texto=$4, ts=$5`,
        [d.negocio, yo, est, String(d.texto || '').slice(0, 200), Date.now()]);
      await crearNotif(d.negocio, 'resena', '⭐ ' + (await nombreDe(yo)) + ' dejó una reseña de ' + est + ' ⭐ con huella verificada');
      socket.emit('aviso', '⭐ ¡Gracias! Tu reseña lleva huella verificada 🧡');
    } catch (e) { console.log('resena error', e.message); }
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
      const pfoto = (d.foto && String(d.foto).startsWith('https://')) ? String(d.foto).slice(0, 500) : null;
      await pool.query(`INSERT INTO productos (negocio,nombre,precio,foto) VALUES ($1,$2,$3,$4)`,
        [yo, String(d.nombre).slice(0, 40), Math.max(0, Number(d.precio) || 0), pfoto]);   
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
        `SELECT n.id, n.nombre, n.descr, n.cat, n.lat, n.lng, n.abierto,
           (SELECT COUNT(*) FROM productos p WHERE p.negocio=n.id)::int AS productos,
           (SELECT ROUND(AVG(estrellas),1) FROM resenas r WHERE r.negocio=n.id)::float AS rating,
           (SELECT COUNT(*) FROM resenas r WHERE r.negocio=n.id)::int AS nresenas
         FROM negocios n ORDER BY n.nombre`);
      socket.emit('negocios', r.rows.map(x => ({ ...x, online: !!(online[x.id] && online[x.id].size) })));
    } catch (e) { console.log('negocios error', e.message); }
  });

  socket.on('tienda', async (d) => {
    try {
      if (!d || !d.id) return;
      const n = await pool.query(`SELECT * FROM negocios WHERE id=$1`, [d.id]);
      if (!n.rowCount) return;
      const p = await pool.query(`SELECT id, nombre, precio::float, foto FROM productos WHERE negocio=$1 ORDER BY id`, [d.id]);
      socket.emit('tienda', { negocio: n.rows[0], productos: p.rows });
    } catch (e) { console.log('tienda error', e.message); }
  });

  socket.on('pedido', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.negocio || !d.items || !d.items.length) return;
      const total = d.items.reduce((a, it) => a + (Number(it.precio) || 0) * (it.cant || 1), 0);
      const lineas = d.items.map(it => it.cant + 'x ' + it.nombre).join(', ');
      const ts = Date.now();
      const p = await pool.query(
        `INSERT INTO pedidos (de,negocio,detalle,total,ts) VALUES ($1,$2,$3,$4,$5) RETURNING id`,
        [yo, d.negocio, lineas.slice(0, 300), total, ts]);
      const pid = p.rows[0].id;
      const texto = '🛍️ PEDIDO #' + pid + ': ' + lineas + ' — Total S/ ' + total.toFixed(2);
      await pool.query(`INSERT INTO mensajes (de,para,texto,ts) VALUES ($1,$2,$3,$4)`, [yo, d.negocio, texto, ts]);
      const m = { de: yo, para: d.negocio, texto, ts };
      sendTo(d.negocio, 'chat', m);
      sendTo(yo, 'chat', m);
      await crearNotif(d.negocio, 'pedido', '🛍️ Nuevo pedido #' + pid + ' de ' + (await nombreDe(yo)) + ' — S/ ' + total.toFixed(2));
      sendTo(yo, 'pedidos-mios', await pedidosDe(yo));
      sendTo(d.negocio, 'pedidos-negocio', await pedidosNegocio(d.negocio));
      await avisarEstado(yo);
      await avisarEstado(d.negocio);
      socket.emit('aviso', '✅ Pedido #' + pid + ' enviado: síguelo en Mercado → 📦 Mis pedidos');
    } catch (e) { console.log('pedido error', e.message); }
  });

  socket.on('pedidos-mios', async () => {
    try {
      const yo = socketDe[socket.id];
      if (yo) socket.emit('pedidos-mios', await pedidosDe(yo));
    } catch (e) {}
  });

  socket.on('pedidos-negocio', async () => {
    try {
      const yo = socketDe[socket.id];
      if (yo) socket.emit('pedidos-negocio', await pedidosNegocio(yo));
    } catch (e) {}
  });

  socket.on('pedido-estado', async (d) => {
    try {
      const yo = socketDe[socket.id];
      const ESTADOS = ['recibido', 'preparando', 'listo', 'entregado'];
      if (!yo || !d || !d.id || !ESTADOS.includes(d.estado)) return;
      const p = await pool.query(`SELECT de, negocio FROM pedidos WHERE id=$1`, [d.id]);
      if (!p.rowCount || p.rows[0].negocio !== yo) return;
      await pool.query(`UPDATE pedidos SET estado=$2 WHERE id=$1`, [d.id, d.estado]);
      const cliente = p.rows[0].de;
      const EMOJI = { recibido: '🕐', preparando: '👨‍🍳', listo: '✅', entregado: '📦' };
      const ts = Date.now();
      const texto = EMOJI[d.estado] + ' Pedido #' + d.id + ': ' + d.estado.toUpperCase();
      await pool.query(`INSERT INTO mensajes (de,para,texto,ts) VALUES ($1,$2,$3,$4)`, [yo, cliente, texto, ts]);
      sendTo(cliente, 'chat', { de: yo, para: cliente, texto, ts });
      sendTo(yo, 'chat', { de: yo, para: cliente, texto, ts });
      await crearNotif(cliente, 'pedido', EMOJI[d.estado] + ' Tu pedido #' + d.id + ' está ' + d.estado.toUpperCase());
      sendTo(cliente, 'pedidos-mios', await pedidosDe(cliente));
      socket.emit('pedidos-negocio', await pedidosNegocio(yo));
      await avisarEstado(cliente);
    } catch (e) { console.log('pedido-estado error', e.message); }
  });

  socket.on('pedido-pagar', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d || !d.id) return;
      const p = await pool.query(`SELECT de, negocio, total::float, pagado FROM pedidos WHERE id=$1`, [d.id]);
      if (!p.rowCount || p.rows[0].de !== yo) return;
      if (p.rows[0].pagado) { socket.emit('aviso', 'Ese pedido ya está pagado ✅'); return; }
      const total = p.rows[0].total;
      const negocio = p.rows[0].negocio;
      const saldo = await saldoDe(yo);
      if (saldo < total) { socket.emit('aviso', '😅 Saldo insuficiente: tienes S/ ' + saldo.toFixed(2) + ' y el pedido es S/ ' + total.toFixed(2)); return; }
      const yoN = await nombreDe(yo);
      const negN = await nombreDe(negocio);
      const ts = Date.now();
      await pool.query(`INSERT INTO wallet_mov (uid,tipo,monto,detalle,ts) VALUES ($1,'envio',$2,$3,$4)`,
        [yo, total, 'Pago pedido #' + d.id + ' a ' + negN, ts]);
      await pool.query(`INSERT INTO wallet_mov (uid,tipo,monto,detalle,ts) VALUES ($1,'recibido',$2,$3,$4)`,
        [negocio, total, 'Cobro pedido #' + d.id + ' de ' + yoN + ' 💳', ts]);
      await pool.query(`UPDATE pedidos SET pagado=TRUE WHERE id=$1`, [d.id]);
      const texto = '💳 Pagué el pedido #' + d.id + ' con mi Wallet Toka — S/ ' + total.toFixed(2);
      await pool.query(`INSERT INTO mensajes (de,para,texto,ts) VALUES ($1,$2,$3,$4)`, [yo, negocio, texto, ts]);
      sendTo(negocio, 'chat', { de: yo, para: negocio, texto, ts });
      sendTo(yo, 'chat', { de: yo, para: negocio, texto, ts });
      await crearNotif(negocio, 'wallet', '💳 ' + yoN + ' pagó el pedido #' + d.id + ' — S/ ' + total.toFixed(2) + ' a tu Wallet');
      socket.emit('wallet', await walletDe(yo));
      sendTo(negocio, 'wallet', await walletDe(negocio));
      socket.emit('pedidos-mios', await pedidosDe(yo));
      sendTo(negocio, 'pedidos-negocio', await pedidosNegocio(negocio));
      socket.emit('aviso', '💳 ¡Pagado! S/ ' + total.toFixed(2) + ' fueron a la Wallet de ' + negN);
    } catch (e) { console.log('pedido-pagar error', e.message); }
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
        socket.emit('aviso', '📍 ¡Tu local quedó fijado en el mapa de Toka!');
        socket.emit('biz-mio', await bizMio(yo));
      }
    } catch (e) { console.log('biz-ubi error', e.message); }
  });

  socket.on('negocios-mapa', async () => {
    try {
      const r = await pool.query(
        `SELECT n.id, n.nombre, n.descr, n.cat, n.lat, n.lng, n.abierto,
           (SELECT COUNT(*) FROM productos p WHERE p.negocio=n.id)::int AS productos,
           (SELECT ROUND(AVG(estrellas),1) FROM resenas r WHERE r.negocio=n.id)::float AS rating,
           (SELECT COUNT(*) FROM resenas r WHERE r.negocio=n.id)::int AS nresenas
         FROM negocios n WHERE n.lat IS NOT NULL ORDER BY n.nombre ASC`);
      socket.emit('negocios-mapa', r.rows.map(x => ({ ...x, abierto: x.abierto !== false, online: !!(online[x.id] && online[x.id].size) })));
    } catch (e) { console.log('negocios-mapa error', e.message); }
  });

  socket.on('biz-abierto', async (d) => {
    try {
      const yo = socketDe[socket.id];
      if (!yo || !d) return;
      await pool.query(`UPDATE negocios SET abierto=$2 WHERE id=$1`, [yo, !!d.on]);
      socket.emit('aviso', d.on ? '🟢 Tu negocio aparece ABIERTO en el mapa' : '⛔ Tu negocio aparece CERRADO en el mapa');
    } catch (e) {}
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
      if (!online[yo].size) pool.query(`UPDATE usuarios SET visto=$2 WHERE id=$1`, [yo, Date.now()]).catch(() => {});
      try { await avisarAmigos(yo); } catch (e) {}
    }
  });
});

const PORT = process.env.PORT || 3000;
// Limpieza diaria: notificaciones de +30 días y registros de actividad de +90 días
setInterval(async () => {
  try {
    await pool.query(`DELETE FROM notifs WHERE ts < $1`, [Date.now() - 30 * 24 * 3600 * 1000]);
    await pool.query(`DELETE FROM dias_activos WHERE dia < $1`,
      [new Date(Date.now() - 90 * 24 * 3600 * 1000).toISOString().slice(0, 10)]);
    console.log('🧹 Limpieza diaria completada');
  } catch (e) { console.log('limpieza error', e.message); }
}, 24 * 3600 * 1000);
initDb().then(() => server.listen(PORT, () => console.log('Boin server v10 en puerto', PORT)))
  .catch(e => { console.log('Error de base de datos:', e.message); server.listen(PORT); });