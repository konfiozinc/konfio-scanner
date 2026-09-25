/* ============================================================
   Puente WhatsApp AppALÍ GSR (Baileys — WhatsApp Web directo)
   - Recibe POST /enviar y publica el mensaje en el grupo configurado.
   - 1ª vez: muestra un QR para vincular el número dedicado.
   - Sesión persistida en ./sesion_baileys
   Config: bridge.config.json (grupo, puerto)
   ============================================================ */
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const qrcode = require('qrcode-terminal');
const pino = require('pino');
const makeWASocket = require('@whiskeysockets/baileys').default;
const { useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, Browsers } = require('@whiskeysockets/baileys');

const DIR = __dirname;
const CONFIG = (() => {
  try { return JSON.parse(fs.readFileSync(path.join(DIR, 'bridge.config.json'), 'utf8')); }
  catch (e) { return { grupo: '', puerto: 8120 }; }
})();
const PUERTO = CONFIG.puerto || 8120;
const OBJETIVO = (CONFIG.grupo || '').trim().toLowerCase();

let sock = null;
let groupJid = null;
let nombreGrupo = '';
let listo = false;

async function resolverGrupo() {
  try {
    const grupos = await sock.groupFetchAllParticipating();
    const lista = Object.values(grupos || {});
    const hallado = lista.find(g => (g.subject || '').toLowerCase().trim() === OBJETIVO) ||
                    lista.find(g => (g.subject || '').toLowerCase().includes(OBJETIVO));
    if (hallado) {
      groupJid = hallado.id;
      nombreGrupo = hallado.subject;
      console.log('[WA] Grupo resuelto:', hallado.subject, '->', hallado.id);
    } else {
      groupJid = null;
      nombreGrupo = '';
      console.log('[WA] No encontré el grupo "' + CONFIG.grupo + '". Grupos visibles:');
      lista.slice(0, 30).forEach(g => console.log('     - ' + g.subject));
      console.log('     Ajusta "grupo" en bridge.config.json o asegúrate de que');
      console.log('     el número bot sea miembro del grupo.');
    }
  } catch (e) {
    console.error('[WA] Error resolviendo grupo:', e.message || e);
  }
}

async function arrancar() {
  const { state, saveCreds } = await useMultiFileAuthState(path.join(DIR, 'sesion_baileys'));
  const { version } = await fetchLatestBaileysVersion().catch(() => ({ version: [2, 3000, 0] }));
  sock = makeWASocket({
    version,
    auth: state,
    browser: Browsers.windows('Chrome'),
    logger: pino({ level: 'silent' }),
    printQRInTerminal: false,
    syncFullHistory: false
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update;
    if (qr) {
      console.log('\n====================================================');
      console.log('  ESCANEA ESTE QR con WhatsApp del número BOT:');
      console.log('  WhatsApp > Dispositivos vinculados > Vincular');
      console.log('====================================================');
      qrcode.generate(qr, { small: true });
    }
    if (connection === 'open') {
      console.log('[WA] Conectado y listo.');
      listo = true;
      resolverGrupo();
    } else if (connection === 'close') {
      listo = false;
      const code = lastDisconnect && lastDisconnect.error ? lastDisconnect.error.output.statusCode : 0;
      if (code === DisconnectReason.loggedOut) {
        console.log('[WA] Sesión cerrada desde el teléfono. Borra la carpeta "sesion_baileys" para volver a vincular.');
      } else {
        console.log('[WA] Conexión cerrada, reconectando en 3s...');
        setTimeout(arrancar, 3000);
      }
    }
  });
}

/* ---------- Servidor HTTP ---------- */
const server = http.createServer((req, res) => {
  const envia = (code, obj) => {
    res.writeHead(code, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(obj));
  };

  if (req.method === 'POST' && req.url === '/enviar') {
    const pedazos = [];
    req.on('data', (d) => { pedazos.push(d); });
    req.on('end', async () => {
      try {
        const cuerpo = Buffer.concat(pedazos).toString('utf8');
        const data = JSON.parse(cuerpo || '{}');
        if (!data.mensaje) return envia(400, { ok: false, error: 'falta mensaje' });
        if (!sock || !listo) return envia(409, { ok: false, error: 'whatsapp no conectado aún' });
        if (!groupJid) {
          await resolverGrupo();
          if (!groupJid) return envia(409, { ok: false, error: 'grupo no resuelto' });
        }
        await sock.sendMessage(groupJid, { text: data.mensaje });
        envia(200, { ok: true });
      } catch (e) {
        envia(500, { ok: false, error: e.message });
      }
    });
    return;
  }

  if (req.method === 'GET' && req.url === '/estado') {
    envia(200, { conectado: listo, grupoConfigurado: CONFIG.grupo || '', grupoResuelto: nombreGrupo, puerto: PUERTO });
    return;
  }

  if (req.method === 'GET' && req.url === '/listar-grupos') {
    if (!sock || !listo) return envia(503, { ok: false, error: 'whatsapp no conectado aún' });
    sock.groupFetchAllParticipating().then((gs) => {
      envia(200, Object.values(gs || {}).map(g => g.subject));
    }).catch(() => envia(500, { ok: false, error: 'sin sesión' }));
    return;
  }

  envia(404, { ok: false, error: 'ruta no existe' });
});

server.listen(PUERTO, '127.0.0.1', () => {
  console.log('====================================================');
  console.log('  Puente WhatsApp AppALÍ escuchando en http://127.0.0.1:' + PUERTO);
  console.log('  Grupo configurado: "' + CONFIG.grupo + '"');
  console.log('====================================================');
});

process.on('unhandledRejection', (r) => {
  console.error('[WA] Rechazo no controlado:', r && r.message ? r.message : r);
});

arrancar();
