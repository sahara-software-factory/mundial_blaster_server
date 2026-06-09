const {
  makeWASocket,
  useMultiFileAuthState,
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
} = require('@whiskeysockets/baileys')
const pino = require('pino')
const fs = require('fs-extra')
const path = require('path')
const QRCode = require('qrcode')
const NodeCache = require('node-cache')
const logger = pino({ level: 'silent' })

// === UTILS ===
function resolveSpintax(text) {
  if (!text) return ''
  return text.replace(/\{\{([^}]+)\}\}/g, (match, content) => {
    if (!content.includes('|')) return match
    const variants = content.split("|").map(s => s.trim()).filter(Boolean)
    return variants.length ? variants[Math.floor(Math.random() * variants.length)] : ''
  })
}

class WAService {
  constructor(prisma, io) {
    this.prisma = prisma
    this.io = io
    this.clients = new Map()
    this.reconnectTimers = {}
    this.presenceIntervals = {}
    this.sessionsDir = '/app/sessions'
    this.msgRetryCounterCache = new NodeCache()
    fs.ensureDirSync(this.sessionsDir)
  }

  async init() {
    console.log('✅ Mundial Blaster WAService inicializado')
    try {
      const lines = await this.prisma.lineas_whatsapp.findMany({
        where: { status: 'CONECTADA' }
      })
      for (const line of lines) {
        if (line.phone) {
          await this.connect(line.phone).catch(e => console.error(e))
          await new Promise(r => setTimeout(r, 1500)) // espaciado para no saturar Meta
        }
      }
    } catch (e) {
      console.error('Error init:', e)
    }
  }

  async connect(phone) {
    try {
      let line = await this.prisma.lineas_whatsapp.findUnique({ where: { phone } })
      if (!line) {
        console.warn(`⚠️ Línea no registrada: ${phone}`)
        return
      }

      const lineId = line.id
      const sessionPath = path.join(this.sessionsDir, String(lineId))

      // Cerrar socket viejo si existe
      const existing = this.clients.get(lineId)
      if (existing) {
        try { existing.ws?.close() } catch {}
        this.clients.delete(lineId)
      }

      await fs.ensureDir(sessionPath)
      const { state, saveCreds } = await useMultiFileAuthState(sessionPath)
      const { version } = await fetchLatestBaileysVersion()

      const waClient = makeWASocket({
        version,
        logger,
        printQRInTerminal: false,
        auth: {
          creds: state.creds,
          keys: makeCacheableSignalKeyStore(state.keys, logger),
        },
        browser: ['MundialBlaster', 'Chrome', '120.0.0'],
        msgRetryCounterCache: this.msgRetryCounterCache,
        getMessage: async (key) => {
          return { conversation: 'Mundial Blaster' }
        },
        markOnlineOnConnect: false,
        syncFullHistory: false,
        connectTimeoutMs: 60000,
        keepAliveIntervalMs: 25000,
        retryRequestDelayMs: 3000,
        defaultQueryTimeoutMs: 60000,
      })

      this.clients.set(lineId, waClient)
      this.setupEvents(waClient, lineId, phone, saveCreds)
      return line
    } catch (err) {
      console.error('❌ Error connect:', err)
      throw err
    }
  }

  setupEvents(waClient, lineId, phone, saveCreds) {
    // 1. Guardar credenciales
    waClient.ev.on('creds.update', saveCreds)

    // 2. Actualizaciones de conexión
    waClient.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update

      // ─── QR ───
      if (qr) {
        try {
          const qrDataUrl = await QRCode.toDataURL(qr)
          this.io.emit('qr', { lineId, qr: qrDataUrl })
          await this.prisma.lineas_whatsapp.update({
            where: { id: lineId },
            data: { status: 'PENDING' }
          }).catch(() => {})
          console.log(`📱 QR emitido: ${lineId}`)
        } catch (e) {
          console.error('Error QR:', e)
        }
        return
      }

      // ─── OPEN ───
      if (connection === 'open') {
        console.log(`✅ Conectado: ${lineId}`)

        // Limpiar timer de reconexión si existía
        if (this.reconnectTimers[lineId]) {
          clearTimeout(this.reconnectTimers[lineId])
          delete this.reconnectTimers[lineId]
        }

        // 🔥 KEEP-ALIVE: presence cada 3 minutos para evitar que Meta corte por inactividad
        if (this.presenceIntervals[lineId]) {
          clearInterval(this.presenceIntervals[lineId])
          delete this.presenceIntervals[lineId]
        }

        this.presenceIntervals[lineId] = setInterval(async () => {
          try {
            if (waClient?.ws?.readyState === 1) {
              await waClient.sendPresenceUpdate('available')
            }
          } catch (e) {
            // Silencioso: si falla, la reconexión lo maneja
          }
        }, 180000) // 3 minutos

        this.io.emit('status', { lineId, status: 'CONECTADA' })
        await this.prisma.lineas_whatsapp.update({
          where: { id: lineId },
          data: { status: 'CONECTADA' }
        }).catch(() => {})
        return
      }

      // ─── CLOSE ───
      if (connection === 'close') {
        // Limpiar keep-alive
        if (this.presenceIntervals[lineId]) {
          clearInterval(this.presenceIntervals[lineId])
          delete this.presenceIntervals[lineId]
        }

        const statusCode = lastDisconnect?.error?.output?.statusCode
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut

        console.log(`❌ Desconectado ${lineId}. Razón: ${statusCode}`)

        // 401 = Unauthorized: sesión invalidada por WhatsApp. Requiere QR nuevo.
        if (statusCode === 401) {
          console.log(`🔒 Sesión invalidada (401). Requiere reescanear QR.`)
          await fs.remove(path.join(this.sessionsDir, String(lineId))).catch(() => {})
          this.clients.delete(lineId)
          await this.prisma.lineas_whatsapp.update({
            where: { id: lineId },
            data: { status: 'DESCONECTADA' }
          }).catch(() => {})
          this.io.emit('status', { lineId, status: 'DESCONECTADA', reason: 'SESSION_INVALID' })
          return
        }

        // Reconexión inteligente para el resto
        if (shouldReconnect) {
          if (this.reconnectTimers[lineId]) clearTimeout(this.reconnectTimers[lineId])

          let delay = 5000 // default
          if (statusCode === 503 || statusCode === 428 || statusCode === 515) {
            delay = 15000 // errores de servidor/saturación/stream
          } else if (statusCode === 408) {
            delay = 8000  // timeout
          } else if (statusCode === 440) {
            delay = 10000 // connection replaced
          }

          console.log(`⏳ Reconectando ${lineId} en ${delay / 1000}s...`)

          this.reconnectTimers[lineId] = setTimeout(async () => {
            try {
              this.clients.delete(lineId)
              await this.connect(phone)
              delete this.reconnectTimers[lineId]
            } catch (e) {
              console.error(`❌ Falló reconexión ${lineId}:`, e)
            }
          }, delay)
        } else {
          // Logout definitivo
          console.log(`⚠️ Logout permanente ${lineId}`)
          await fs.remove(path.join(this.sessionsDir, String(lineId))).catch(() => {})
          this.clients.delete(lineId)
          await this.prisma.lineas_whatsapp.update({
            where: { id: lineId },
            data: { status: 'DESCONECTADA' }
          }).catch(() => {})
          this.io.emit('status', { lineId, status: 'DESCONECTADA', reason: 'LOGOUT' })
        }
      }
    })

    waClient.ev.on('messages.upsert', async ({ messages, type }) => {
  if (type !== 'notify') return
  for (const msg of messages) {
    if (!msg.message || msg.key.fromMe) continue

    const from = msg.key.remoteJid?.replace('@s.whatsapp.net', '')
    if (!from) continue

    const messageBody = msg.message?.conversation || msg.message?.extendedTextMessage?.text || ''

    // ─── AUTO BLACKLIST: verificar palabras clave ───
    try {
      const config = await this.prisma.app_config.findUnique({ where: { key: 'blacklist_keywords' } })
      const keywords = config?.value ? JSON.parse(config.value) : ['basta', 'no molesten', 'no me interesa', 'no, gracias', 'eliminar', 'stop', 'darme de baja']
      const lowerBody = messageBody.toLowerCase()
      const matched = keywords.find(k => lowerBody.includes(k.toLowerCase()))

      if (matched) {
        await this.prisma.blacklist.upsert({
          where: { phone: from.replace(/\D/g, '') },
          update: { reason: `auto: "${matched}"` },
          create: { phone: from.replace(/\D/g, ''), reason: `auto: "${matched}"` }
        })
        console.log(`🚫 Auto-blacklist: ${from} por keyword "${matched}"`)
        // Emitir evento para que el frontend se entere
        this.io.emit('auto_blacklist', { phone: from, keyword: matched, timestamp: new Date() })
      }
    } catch (e) {
      console.error('Auto-blacklist error:', e)
    }

    // ─── REPLY TRACKING (campana reciente) ───
    const recentLog = await this.prisma.campaign_logs.findFirst({
      where: {
        contact_phone: from,
        status: 'sent',
        created_at: { gte: new Date(Date.now() - 30 * 86400000) }
      },
      orderBy: { created_at: 'desc' }
    })

    if (recentLog && !recentLog.has_reply) {
      await this.prisma.campaign_logs.update({
        where: { id: recentLog.id },
        data: { has_reply: true, replied_at: new Date() }
      })

      this.io.emit('campaign_reply', {
        campaign_id: recentLog.campaign_id,
        phone: from,
        message: messageBody,
        timestamp: new Date()
      })
    }
  }
})

    // 4. Delivery & Read receipts: TRACKING DE APERTURA
    waClient.ev.on('messages.update', async (updates) => {
      for (const update of updates) {
        const { key, update: statusUpdate } = update
        if (!key.id || !key.remoteJid) continue

        const phone = key.remoteJid.replace('@s.whatsapp.net', '')

        // Buscar el log más reciente de este número
        const log = await this.prisma.campaign_logs.findFirst({
          where: { contact_phone: phone },
          orderBy: { created_at: 'desc' }
        })

        if (!log) continue

        // ack: 1 = sent, 2 = delivered, 3 = read, 4 = played
        const ack = statusUpdate?.status || statusUpdate?.ack

        if (ack === 2 && !log.delivered_at) {
          await this.prisma.campaign_logs.update({
            where: { id: log.id },
            data: { delivered_at: new Date() }
          })
        }

        if (ack === 3 && !log.read_at) {
          await this.prisma.campaign_logs.update({
            where: { id: log.id },
            data: { read_at: new Date() }
          })
        }
      }
    })
  }

  cleanJid(jid) {
    if (!jid) return ''
    return jid.split('@')[0].split(':')[0].replace(/\D/g, '')
  }




  

  async sendMessage(lineId, contactPhone, content, options = {}) {
    const { type = 'text', imageUrl = null } = options
    let waClient = this.clients.get(lineId) || this.clients.get(String(lineId))
    if (!waClient || !waClient.user) throw new Error('Línea no conectada')

    try {
      const cleanNumber = this.cleanJid(contactPhone)
      const isGroup = cleanNumber.length > 15
      const jid = isGroup ? `${cleanNumber}@g.us` : `${cleanNumber}@s.whatsapp.net`

      let messagePayload = {}
      if (type === 'image' && imageUrl) {
        messagePayload = { image: { url: imageUrl }, caption: content || '' }
      } else {
        messagePayload = { text: content }
      }

      await waClient.sendMessage(jid, messagePayload)
      return { success: true }
    } catch (error) {
      console.error('❌ Error sendMessage:', error)
      throw error
    }
  }

        async sendMessageHuman(lineId, contactPhone, content, options = {}) {
    const waClient = this.clients.get(lineId) || this.clients.get(String(lineId))
    if (!waClient || !waClient.user) throw new Error('Línea no conectada')

    const cleanNumber = this.cleanJid(contactPhone)
    const isGroup = cleanNumber.length > 15
    const jid = isGroup ? `${cleanNumber}@g.us` : `${cleanNumber}@s.whatsapp.net`

    console.log(`[HUMAN MODE] Iniciando typing para ${contactPhone} → JID: ${jid}`)

    try {
      // 1. Composing
      await waClient.sendPresenceUpdate('composing', jid)
      console.log(`[HUMAN MODE] ✅ composing enviado`)

      // 2. Delay proporcional al texto (50ms por char, entre 2s y 8s)
      const typingDelay = Math.min(8000, Math.max(2000, content.length * 50))
      console.log(`[HUMAN MODE] ⏳ Esperando ${typingDelay}ms (${content.length} caracteres)`)
      await new Promise(r => setTimeout(r, typingDelay))

      // 3. Pausar typing
      await waClient.sendPresenceUpdate('paused', jid)
      console.log(`[HUMAN MODE] ✅ paused enviado`)

      // 4. Pausa post-typing
      await new Promise(r => setTimeout(r, 500))

      // 5. Enviar mensaje real (mismo código que sendMessage)
      const { type = 'text', imageUrl = null } = options
      let messagePayload = {}
      if (type === 'image' && imageUrl) {
        messagePayload = { image: { url: imageUrl }, caption: content || '' }
      } else {
        messagePayload = { text: content }
      }

      await waClient.sendMessage(jid, messagePayload)
      console.log(`[HUMAN MODE] ✅ Mensaje enviado`)
      return { success: true }
    } catch (err) {
      console.error(`[HUMAN MODE] ❌ Error:`, err.message)
      throw err
    }
  }

async sendCampaign(campaignId, lineInput, targets, message, options = {}) {
  const { delayMin = 8000, delayMax = 15000, imageUrl = null } = options

  // ─── Normalizar líneas ───
  let lineasActivas = []
  if (Array.isArray(lineInput)) {
    lineasActivas = lineInput.filter(l => l.status === 'CONECTADA' && this.clients.has(l.id))
  } else if (typeof lineInput === 'string') {
    // Legacy: una sola línea por ID
    const line = await this.prisma.lineas_whatsapp.findUnique({ where: { id: lineInput } })
    if (line && line.status === 'CONECTADA') lineasActivas = [line]
  } else if (lineInput && lineInput.id) {
    // Legacy: objeto único
    lineasActivas = [lineInput]
  }

  if (lineasActivas.length === 0) {
    throw new Error('No hay líneas conectadas disponibles para enviar')
  }

  // Marcar como running
  await this.prisma.campaigns.update({
    where: { id: campaignId },
    data: { status: 'running' }
  }).catch(() => {})

  const results = []
  let wasCancelled = false
  let lineaIndex = 0
  const lineasCaidas = new Set()
   let lastDelayMs = 0


   

  for (let i = 0; i < targets.length; i++) {
    const target = targets[i]

    if (options.skipBlacklist) {
        const cleanPhone = target.phone.replace(/\D/g, '')
        const blacklisted = await this.prisma.blacklist.findUnique({
          where: { phone: cleanPhone }
        })
        if (blacklisted) {
          console.log(`⛔ Saltando ${target.phone} — blacklist`)
          await this.prisma.campaign_logs.create({
            data: {
              campaign_id: campaignId,
              contact_phone: target.phone,
              status: 'skipped_blacklist',
              line_id: null,           // ← aún no asignamos línea
              owner_id: options.ownerId || null,
            }
          }).catch(() => {})
          continue // NO enviar, NO contar como fallo
        }
      }


    // ⏹️ CHEQUEAR CANCELACIÓN
    try {
      const campaignStatus = await this.prisma.campaigns.findUnique({
        where: { id: campaignId },
        select: { status: true }
      })
      if (campaignStatus?.status === 'cancelled') {
        console.log(`⏹️ Campaña ${campaignId} cancelada. Deteniendo en ${i}/${targets.length}`)
        wasCancelled = true
        break
      }
    } catch (e) {}

    // ─── ROUND-ROBIN: buscar línea viva ───
    let intentos = 0
    let lineaAsignada = null

    while (intentos < lineasActivas.length) {
      const candidata = lineasActivas[lineaIndex % lineasActivas.length]
      if (!lineasCaidas.has(candidata.id)) {
        lineaAsignada = candidata
        break
      }
      lineaIndex++
      intentos++
    }

    if (!lineaAsignada) {
      // Todas las líneas cayeron
      results.push({ phone: target.phone, status: 'failed', error: 'Todas las líneas offline', index: i })
      await this.prisma.campaign_logs.create({
        data: {
          campaign_id: campaignId,
          line_id: null,
          contact_phone: target.phone,
          status: 'failed',
          error: 'Todas las líneas offline'
        }
      }).catch(() => {})
      continue
    }

    try {
      const resolvedMessage = resolveSpintax(message)
      const personalized = resolvedMessage
        .replace(/\{\{nombre\}\}/gi, target.name || 'Cliente')
        .replace(/\{nombre\}/gi, target.name || 'Cliente')
        .replace(/\{\{telefono\}\}/gi, target.phone || '')
        .replace(/\{telefono\}/gi, target.phone || '')

      const sendOptions = {
        type: imageUrl ? 'image' : 'text',
        imageUrl
      }
      if (options.humanMode) {
        await this.sendMessageHuman(lineaAsignada.id, target.phone, personalized, sendOptions)
      } else {
        await this.sendMessage(lineaAsignada.id, target.phone, personalized, sendOptions)
      }

      results.push({ phone: target.phone, status: 'sent', lineId: lineaAsignada.id, index: i })

      await this.prisma.campaigns.update({
        where: { id: campaignId },
        data: { sent: { increment: 1 } }
      }).catch(() => {})

      await this.prisma.campaign_logs.create({
        data: {
          campaign_id: campaignId,
          line_id: lineaAsignada.id,
          contact_phone: target.phone,
          status: 'sent'
        }
      }).catch(() => {})

      this.io.emit('campaign_log', {
        campaignId: campaignId,           // camelCase (dashboard en vivo)
        campaign_id: campaignId,          // snake_case (reports/page.tsx)
        phone: target.phone,              // camelCase
        contact_phone: target.phone,      // snake_case
        status: 'sent',
        lineId: lineaAsignada.id,         // camelCase
        line_id: lineaAsignada.id,        // snake_case
        linePhone: lineaAsignada.phone,
         delayMs: lastDelayMs, 
        line_phone: lineaAsignada.phone,
        progress: `${i + 1}/${targets.length}`
      })

      console.log(`✅ ${i + 1}/${targets.length} → ${target.phone} [${lineaAsignada.phone}]`)
      lineaIndex++ // avanzar al siguiente para el próximo contacto

    } catch (err) {
      console.error(`❌ ${target.phone} [${lineaAsignada.phone}]:`, err.message)

      // FAILOVER: marcar línea como caída y reintentar ESTE contacto con otra
      lineasCaidas.add(lineaAsignada.id)

      // Log del fallo
      await this.prisma.campaigns.update({
        where: { id: campaignId },
        data: { failed: { increment: 1 } }
      }).catch(() => {})

        this.io.emit('campaign_log', {
        campaignId: campaignId,
        campaign_id: campaignId,
        phone: target.phone,
        contact_phone: target.phone,
        status: 'failed',
        lineId: lineaAsignada.id,
         delayMs: lastDelayMs, 
        line_id: lineaAsignada.id,
        linePhone: lineaAsignada.phone,
        line_phone: lineaAsignada.phone,
        error: err.message?.slice(0, 200),
        progress: `${i + 1}/${targets.length}`
      })

      await this.prisma.campaign_logs.create({
        data: {
          campaign_id: campaignId,
          line_id: lineaAsignada.id,
          contact_phone: target.phone,
          status: 'failed',
          error: err.message?.slice(0, 200)
        }
      }).catch(() => {})

      // Si hay otras líneas vivas, reintentar este mismo contacto (i-- para no avanzar)
      const quedanLineas = lineasActivas.some(l => !lineasCaidas.has(l.id))
      if (quedanLineas) {
        i-- // repetir este contacto con la siguiente línea disponible
      } else {
        results.push({ phone: target.phone, status: 'failed', error: err.message, index: i })
      }
    }

    // Delay anti-ban (solo si no es el último)
        // Delay anti-ban (solo si no es el último)
    if (i < targets.length - 1 && !wasCancelled) {
      const baseDelay = Math.floor(Math.random() * (delayMax - delayMin + 1)) + delayMin
      const humanExtra = options.humanMode ? (3000 + Math.random() * 5000) : 0
      lastDelayMs = baseDelay + humanExtra // ← GUARDAR para el próximo log
      console.log(`[DELAY] Esperando ${lastDelayMs}ms antes del siguiente mensaje`)
      await new Promise(r => setTimeout(r, lastDelayMs))
    }
  }

  // Finalizar
  const finalStatus = wasCancelled ? 'cancelled' : 'completed'
  await this.prisma.campaigns.update({
    where: { id: campaignId },
    data: { status: finalStatus, finished_at: new Date() }
  }).catch(() => {})

  this.io.emit('campaign_complete', {
    campaignId: campaignId,
    campaign_id: campaignId,
    status: finalStatus,
    sent: results.filter(r => r.status === 'sent').length,
    failed: results.filter(r => r.status === 'failed').length,
    total_sent: results.filter(r => r.status === 'sent').length,
    total_failed: results.filter(r => r.status === 'failed').length
  })

  return results
}

  async logout(lineId) {
    // Limpiar timers/intervals antes de todo
    if (this.reconnectTimers[lineId]) {
      clearTimeout(this.reconnectTimers[lineId])
      delete this.reconnectTimers[lineId]
    }
    if (this.presenceIntervals[lineId]) {
      clearInterval(this.presenceIntervals[lineId])
      delete this.presenceIntervals[lineId]
    }

    const client = this.clients.get(lineId)
    if (client) {
      try { await client.logout() } catch {}
      this.clients.delete(lineId)
    }

    await fs.remove(path.join(this.sessionsDir, String(lineId))).catch(() => {})
    await this.prisma.lineas_whatsapp.update({
      where: { id: lineId },
      data: { status: 'DESCONECTADA' }
    }).catch(() => {})

    return { success: true }
  }
}



// 🧹 Cleanup graceful al recibir SIGTERM (Railway reinicia containers)
process.on('SIGTERM', () => {
  console.log('🛑 SIGTERM recibido. Limpiando timers y sockets...')
  // Nota: no podemos acceder a la instancia desde aquí, pero los timers
  // se limpian automáticamente al morir el proceso. Esto es más que nada
  // para logs limpios.
  process.exit(0)
})

module.exports = { WAService }