/**
 * baileys-server.js — Quiniela WFC 2026
 * Servidor Express que expone WhatsApp via Baileys.
 *
 * Endpoints (todos en localhost:WA_PORT, solo acceso interno):
 *   GET  /status              → { connected, phone, groupId, hasCode }
 *   POST /pair                → { ok, code }   body: { phone: "+34692890608" }
 *   POST /create-group        → { ok, groupId, added, notFound, message }
 *   POST /send                → { ok, message }
 *   POST /disconnect          → { ok }          body: { deleteGroup: false }
 *
 * Vinculación via pairing code (sin QR):
 *   1. POST /pair con el número del teléfono dedicado
 *   2. El servidor devuelve un código de 8 caracteres (ej: "ABCD-1234")
 *   3. En ese WhatsApp: Dispositivos vinculados → Vincular con número → ingresa el código
 *
 * Variables de entorno:
 *   WA_PORT        Puerto HTTP (default: 3001)
 *   SESSION_PATH   Carpeta para sesión Baileys (default: ./baileys_session)
 *                  En Railway apuntar a /data/baileys_session
 */

import express from 'express'
import {
  makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
} from '@whiskeysockets/baileys'
import { Boom } from '@hapi/boom'
import pino from 'pino'
import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

// ─── Config ──────────────────────────────────────────────────────────────────

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const SESSION_PATH = process.env.SESSION_PATH
  || path.join(__dirname, 'baileys_session')
const GROUP_ID_FILE   = path.join(SESSION_PATH, 'group_id.txt')
const OPT_OUT_FILE    = path.join(SESSION_PATH, 'opted_out.json')  // usuarios que se salieron
const PORT = parseInt(process.env.WA_PORT || '3001')

// Logo del grupo — busca logo.png o icon-192.png en la carpeta del script
const LOGO_PATH = [
  path.join(__dirname, 'logo.png'),
  path.join(__dirname, 'icon-192.png'),
  path.join(__dirname, 'icon-512.png'),
].find(p => fs.existsSync(p)) || null

const logger = pino({ level: 'silent' })

// ─── Estado global ───────────────────────────────────────────────────────────

let sock           = null
let isConnected    = false
let connectedPhone = null
let groupId        = null
let reconnecting   = false
let pairingCode    = null

// Cargar groupId persistido
if (fs.existsSync(GROUP_ID_FILE)) {
  groupId = fs.readFileSync(GROUP_ID_FILE, 'utf8').trim()
  console.log(`[WA] Group ID cargado: ${groupId}`)
}

// ─── Opted-out: usuarios que se salieron solos del grupo ─────────────────────

function loadOptedOut() {
  try {
    if (fs.existsSync(OPT_OUT_FILE))
      return new Set(JSON.parse(fs.readFileSync(OPT_OUT_FILE, 'utf8')))
  } catch {}
  return new Set()
}

function saveOptedOut(set) {
  fs.mkdirSync(SESSION_PATH, { recursive: true })
  fs.writeFileSync(OPT_OUT_FILE, JSON.stringify([...set]), 'utf8')
}

let optedOut = loadOptedOut()   // Set de JIDs que se salieron voluntariamente

// ─── Helpers ─────────────────────────────────────────────────────────────────

function toJID(phone) {
  let digits = phone.replace(/\D/g, '')
  if (!digits) return null
  // México: 52 + 10 dígitos sin el 1 → agregar 1
  if (digits.startsWith('52') && digits.length === 12) {
    digits = '521' + digits.slice(2)
  }
  return `${digits}@s.whatsapp.net`
}

function saveGroupId(gid) {
  groupId = gid
  fs.mkdirSync(SESSION_PATH, { recursive: true })
  fs.writeFileSync(GROUP_ID_FILE, gid, 'utf8')
  console.log(`[WA] Group ID guardado: ${gid}`)
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms))
}

// ─── Conexión Baileys ─────────────────────────────────────────────────────────

// pairingPhone: si se pasa, llama requestPairingCode inmediatamente tras crear el socket
// onCode: callback(code) cuando el código esté listo
async function connectToWhatsApp(pairingPhone = null, onCode = null) {
  fs.mkdirSync(SESSION_PATH, { recursive: true })
  const { state, saveCreds } = await useMultiFileAuthState(SESSION_PATH)
  const { version } = await fetchLatestBaileysVersion()

  console.log(`[WA] Baileys v${version.join('.')} | SESSION: ${SESSION_PATH}`)

  sock = makeWASocket({
    version,
    logger,
    auth: state,
    printQRInTerminal: false,
    generateHighQualityLinkPreview: false,
    getMessage: async () => undefined,
  })

  sock.ev.on('creds.update', saveCreds)

  // Si se pidió pairing code, solicitarlo INMEDIATAMENTE tras crear el socket
  // (debe hacerse antes de que WhatsApp cierre la conexión por timeout)
  if (pairingPhone && onCode) {
    // Pequeña espera para que el socket inicialice la capa WS
    await sleep(500)
    try {
      const rawCode = await sock.requestPairingCode(pairingPhone)
      const formatted = rawCode?.match(/.{1,4}/g)?.join('-') || rawCode
      pairingCode = formatted
      console.log(`[WA] Pairing code para +${pairingPhone}: ${formatted}`)
      onCode(null, formatted)
    } catch (e) {
      console.error('[WA] Error en requestPairingCode:', e.message)
      onCode(e, null)
    }
  }

  // ── Detectar cuando alguien se sale del grupo voluntariamente ────────────
  sock.ev.on('group-participants.update', (update) => {
    if (!groupId || update.id !== groupId) return
    if (update.action === 'remove') {
      // Pueden ser expulsados por admin o salirse solos.
      // Baileys no distingue fácilmente, pero si el que "removió" es el mismo
      // participante, se salió solo. Si no hay "author" o author === participant → salida voluntaria.
      for (const jid of update.participants) {
        const salioSolo = !update.author || update.author === jid
        if (salioSolo) {
          optedOut.add(jid)
          saveOptedOut(optedOut)
          console.log(`[WA] ${jid} se salió del grupo → bloqueado para re-agregar`)
        }
      }
    }
    // Si un admin lo vuelve a agregar manualmente, quitarlo del opted-out
    if (update.action === 'add') {
      for (const jid of update.participants) {
        if (optedOut.has(jid)) {
          optedOut.delete(jid)
          saveOptedOut(optedOut)
          console.log(`[WA] ${jid} re-agregado manualmente → removido de opted-out`)
        }
      }
    }
  })

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update

    if (connection === 'open') {
      isConnected    = true
      pairingCode    = null
      reconnecting   = false
      connectedPhone = sock.user?.id?.split(':')[0] || null
      console.log(`[WA] ✅ Conectado como: +${connectedPhone}`)
    }

    if (connection === 'close') {
      isConnected    = false
      connectedPhone = null
      const reason   = new Boom(lastDisconnect?.error)?.output?.statusCode
      const shouldReconnect = reason !== DisconnectReason.loggedOut

      console.log(`[WA] Desconectado. Razón: ${reason} | Reconectar: ${shouldReconnect}`)

      if (shouldReconnect && !reconnecting) {
        reconnecting = true
        setTimeout(connectToWhatsApp, 5000)
      } else if (!shouldReconnect) {
        // 401 = WhatsApp invalidó la sesión → borrar credenciales para no entrar en loop
        pairingCode = null
        groupId     = null
        if (fs.existsSync(GROUP_ID_FILE)) fs.unlinkSync(GROUP_ID_FILE)
        // Borrar archivos de credenciales (creds.json, keys, etc.) pero conservar group_id y opted_out
        const KEEP = new Set(['group_id.txt', 'opted_out.json'])
        if (fs.existsSync(SESSION_PATH)) {
          for (const f of fs.readdirSync(SESSION_PATH)) {
            if (!KEEP.has(f)) {
              try { fs.rmSync(path.join(SESSION_PATH, f), { recursive: true, force: true }) } catch {}
            }
          }
        }
        console.log('[WA] Credenciales de sesión borradas (401 logout)')
      }
    }
  })
}

// ─── Express API ─────────────────────────────────────────────────────────────

const app = express()
app.use(express.json())

// GET /status
app.get('/status', (req, res) => {
  res.json({
    connected:    isConnected,
    phone:        connectedPhone,
    groupId:      groupId,
    hasCode:      !!pairingCode,
    pairingCode:  pairingCode,  // lo mostramos directo en el admin
  })
})

// POST /pair  body: { phone: "+34692890608" }
// Solicita pairing code para vincular sin QR
app.post('/pair', async (req, res) => {
  if (isConnected) {
    return res.json({ ok: false, msg: 'Ya conectado, no necesitas vincular de nuevo' })
  }

  const { phone } = req.body || {}
  if (!phone) return res.status(400).json({ ok: false, msg: 'Falta el campo "phone"' })

  const digits = phone.replace(/\D/g, '')
  if (!digits) return res.status(400).json({ ok: false, msg: 'Número inválido' })

  try {
    // Matar socket existente y detener el loop de reconexión.
    // Necesario porque si hay un socket en loop de 408 (timeout de sesión vieja),
    // requestPairingCode fallará con "Connection Closed".
    reconnecting = true   // bloquea el setTimeout pendiente para que no relance
    if (sock) {
      try { sock.ev.removeAllListeners(); sock.end?.() } catch {}
      sock = null
    }
    isConnected = false

    // Borrar archivos de credenciales para forzar vinculación limpia.
    // Conservamos group_id.txt y opted_out.json.
    const KEEP = new Set(['group_id.txt', 'opted_out.json'])
    if (fs.existsSync(SESSION_PATH)) {
      for (const f of fs.readdirSync(SESSION_PATH)) {
        if (!KEEP.has(f)) {
          try { fs.rmSync(path.join(SESSION_PATH, f), { recursive: true, force: true }) } catch {}
        }
      }
    }

    reconnecting = false   // permitir reconexión normal después del pairing
    console.log('[WA] /pair: iniciando conexión limpia con pairing inmediato...')

    // Llamar connectToWhatsApp con pairingPhone para que solicite el código
    // justo al crear el socket (timing correcto para Baileys)
    await new Promise((resolve, reject) => {
      connectToWhatsApp(digits, (err, code) => {
        if (err) return reject(err)
        res.json({ ok: true, code })
        resolve()
      })
    })
  } catch (e) {
    console.error('[WA] Error generando pairing code:', e)
    res.status(500).json({ ok: false, msg: e.message })
  }
})

// POST /create-group  body: { name, phones: ["+521234...", ...] }
app.post('/create-group', async (req, res) => {
  if (!isConnected) return res.status(503).json({ ok: false, msg: 'WhatsApp no conectado' })

  const { name = 'Quiniela WFC 2026 🏆', phones = [] } = req.body
  if (!phones.length) return res.status(400).json({ ok: false, msg: 'Sin números para agregar' })

  // Convertir todos los teléfonos a JIDs sin verificar (onWhatsApp da falsos negativos)
  const added    = []
  const notFound = []

  for (const phone of phones) {
    const jid = toJID(phone)
    if (!jid) { notFound.push(phone); continue }
    added.push(jid)
  }

  if (!added.length) {
    return res.json({ ok: false, msg: 'Ningún número válido para agregar', notFound })
  }

  try {
    const group = await sock.groupCreate(name, added)
    const gid   = group.id

    await sleep(1000)
    await sock.groupSettingUpdate(gid, 'announcement')

    // ── Poner ícono del grupo ─────────────────────────────────────────────
    if (LOGO_PATH) {
      try {
        await sleep(500)
        const imgBuffer = fs.readFileSync(LOGO_PATH)
        await sock.updateProfilePicture(gid, imgBuffer)
        console.log(`[WA] Ícono del grupo actualizado desde ${path.basename(LOGO_PATH)}`)
      } catch (e) {
        console.warn('[WA] No se pudo poner el ícono:', e.message)
      }
    }

    saveGroupId(gid)
    console.log(`[WA] Grupo "${name}" creado | ${added.length} miembros | ID: ${gid}`)

    res.json({
      ok:      true,
      groupId: gid,
      added:   added.map(j => j.replace('@s.whatsapp.net', '')),
      notFound,
      message: `Grupo "${name}" creado con ${added.length} miembro(s)`,
    })
  } catch (e) {
    console.error('[WA] Error creando grupo:', e)
    res.status(500).json({ ok: false, msg: e.message })
  }
})

// POST /update-group  body: { name, phones?: [...] }
// Actualiza nombre, ícono y (opcionalmente) sincroniza miembros faltantes
app.post('/update-group', async (req, res) => {
  if (!isConnected) return res.status(503).json({ ok: false, msg: 'WhatsApp no conectado' })
  if (!groupId)     return res.status(404).json({ ok: false, msg: 'Sin grupo configurado' })

  const { name, phones = [] } = req.body || {}
  const added = []
  const errors = []

  try {
    if (name) {
      await sock.groupUpdateSubject(groupId, name)
      console.log(`[WA] Nombre del grupo actualizado: "${name}"`)
    }
    if (LOGO_PATH) {
      await sleep(500)
      const imgBuffer = fs.readFileSync(LOGO_PATH)
      await sock.updateProfilePicture(groupId, imgBuffer)
      console.log(`[WA] Ícono del grupo actualizado`)
    }

    // Sincronizar miembros faltantes si se pasaron phones
    if (phones.length) {
      // Obtener miembros actuales del grupo
      let currentJIDs = new Set()
      try {
        const meta = await sock.groupMetadata(groupId)
        for (const p of meta.participants) currentJIDs.add(p.id)
      } catch (e) {
        console.warn('[WA] No se pudo leer miembros actuales:', e.message)
      }

      for (const phone of phones) {
        const jid = toJID(phone)
        if (!jid) continue
        if (currentJIDs.has(jid)) continue  // ya está en el grupo
        if (optedOut.has(jid)) continue      // salió voluntariamente
        try {
          await sock.groupParticipantsUpdate(groupId, [jid], 'add')
          added.push(phone)
          console.log(`[WA] +${phone} agregado al grupo (sync)`)
          await sleep(500)
        } catch (e) {
          errors.push(phone)
          console.warn(`[WA] No se pudo agregar ${phone}: ${e.message}`)
        }
      }
    }

    const msg = added.length
      ? `Grupo actualizado y ${added.length} miembro(s) agregado(s)`
      : 'Grupo actualizado correctamente'
    res.json({ ok: true, msg, added, errors })
  } catch (e) {
    console.error('[WA] Error actualizando grupo:', e)
    res.status(500).json({ ok: false, msg: e.message })
  }
})

// POST /add-member  body: { phone: "+521234..." }
// Agrega un nuevo participante al grupo (se llama automáticamente al registrarse)
app.post('/add-member', async (req, res) => {
  if (!isConnected)  return res.status(503).json({ ok: false, msg: 'WhatsApp no conectado' })
  if (!groupId)      return res.status(404).json({ ok: false, msg: 'Sin grupo configurado' })

  const { phone } = req.body || {}
  const jid = toJID(phone)
  if (!jid) return res.status(400).json({ ok: false, msg: 'Número inválido' })

  // Respetar opt-out: si se salió voluntariamente, no re-agregar
  if (optedOut.has(jid)) {
    console.log(`[WA] ${jid} está en opted-out — no se re-agrega`)
    return res.json({ ok: false, skipped: true, msg: 'El usuario se salió voluntariamente del grupo' })
  }

  try {
    await sock.groupParticipantsUpdate(groupId, [jid], 'add')
    console.log(`[WA] +${phone} agregado al grupo`)
    res.json({ ok: true, msg: `${phone} agregado al grupo` })
  } catch (e) {
    console.error('[WA] Error agregando miembro:', e)
    res.status(500).json({ ok: false, msg: e.message })
  }
})

// POST /send  body: { message }
app.post('/send', async (req, res) => {
  if (!isConnected)  return res.status(503).json({ ok: false, msg: 'WhatsApp no conectado' })
  if (!groupId)      return res.status(404).json({ ok: false, msg: 'Sin grupo configurado' })

  const { message = '🔔 Prueba de notificación ✅' } = req.body

  try {
    await sock.sendMessage(groupId, { text: message })
    res.json({ ok: true, msg: 'Enviado' })
  } catch (e) {
    console.error('[WA] Error enviando:', e)
    res.status(500).json({ ok: false, msg: e.message })
  }
})

// POST /disconnect  body: { deleteGroup: false }
app.post('/disconnect', async (req, res) => {
  const { deleteGroup = false } = req.body || {}

  try {
    if (deleteGroup && groupId && isConnected) {
      try {
        await sock.sendMessage(groupId, {
          text: '🏁 La quiniela ha finalizado. ¡Gracias a todos por participar! 🎉'
        })
        await sleep(1500)
        // Expulsar a todos los participantes antes de salir
        try {
          const meta = await sock.groupMetadata(groupId)
          const myJid = sock.user?.id?.replace(/:.*@/, '@') || ''
          const others = meta.participants
            .filter(p => {
              const pid = p.id.replace(/:.*@/, '@')
              return pid !== myJid
            })
            .map(p => p.id)
          if (others.length > 0) {
            // Expulsar en lotes de 5 para evitar rate limit
            for (let i = 0; i < others.length; i += 5) {
              const batch = others.slice(i, i + 5)
              await sock.groupParticipantsUpdate(groupId, batch, 'remove').catch(() => {})
              await sleep(500)
            }
          }
        } catch (e) {
          console.warn('[WA] No se pudo expulsar participantes:', e.message)
        }
        await sleep(500)
        await sock.groupLeave(groupId)
      } catch (e) {
        console.warn('[WA] No se pudo eliminar el grupo:', e.message)
      }
    }

    groupId     = null
    pairingCode = null
    if (fs.existsSync(GROUP_ID_FILE)) fs.unlinkSync(GROUP_ID_FILE)

    if (sock) await sock.logout().catch(() => {})

    if (fs.existsSync(SESSION_PATH)) {
      fs.rmSync(SESSION_PATH, { recursive: true, force: true })
    }

    isConnected    = false
    connectedPhone = null
    sock           = null

    console.log('[WA] Desconectado y sesión eliminada')
    res.json({ ok: true, msg: 'Desconectado correctamente' })

    setTimeout(connectToWhatsApp, 2000)
  } catch (e) {
    console.error('[WA] Error al desconectar:', e)
    res.status(500).json({ ok: false, msg: e.message })
  }
})

// ─── Arranque ─────────────────────────────────────────────────────────────────

app.listen(PORT, '127.0.0.1', () => {
  console.log(`[WA] Servidor escuchando en http://127.0.0.1:${PORT}`)
})

connectToWhatsApp().catch(console.error)
