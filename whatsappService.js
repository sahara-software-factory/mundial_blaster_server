const {
  makeWASocket,
  useMultiFileAuthState,
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  jidDecode,           
  jidNormalizedUser
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
    this.lidCache = new Map() 
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
          this.io.emit('status', { lineId, status: 'DESCONECTADA', reason: 'SESSION_INVALID', phone:line_phone, nombre: line_nombre })
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

    const messageBody = msg.message?.conversation || msg.message?.extendedTextMessage?.text || ''
    
    // ==========================================
    // 🧠 EL RESCATE DEL NÚMERO REAL (Patrón Sahara One)
    // ==========================================
    const raw = msg.key.remoteJid;
    const alt = msg.key.remoteJidAlt; // 🔑 LA MAGIA — WhatsApp manda ambos en el mismo paquete
    
    let fromPhone = null;
    let lidDetectado = null;

    // Paso 1: Leer ambos campos del mensaje a la vez
    if (raw?.includes('@s.whatsapp.net')) {
        fromPhone = raw.split('@')[0].replace(/\D/g, '');
    } else if (raw?.includes('@lid')) {
        lidDetectado = raw.split('@')[0];
    }
    
    if (alt?.includes('@s.whatsapp.net')) {
        fromPhone = alt.split('@')[0].replace(/\D/g, '');
    } else if (alt?.includes('@lid') && !lidDetectado) {
        lidDetectado = alt.split('@')[0];
    }

    // Paso 2: AUTO-SANACIÓN — Si el mensaje trajo ambos, curar la BD en el momento
    if (fromPhone && lidDetectado) {
      this.lidCache.set(`${lineId}:${lidDetectado}`, fromPhone);
      await this.prisma.lid_mappings.upsert({
        where: { lineId_lid: { lineId, lid: lidDetectado } },
        update: { phone: fromPhone, updatedAt: new Date() },
        create: { lineId, lid: lidDetectado, phone: fromPhone }
      }).catch(() => {});
      console.log(`🩺 Auto-sanación instantánea: ${lidDetectado} → ${fromPhone}`);
    }

    // Paso 3: Si solo tenemos LID (remoteJidAlt no vino), buscar en caché/BD
    if (!fromPhone && lidDetectado) {
      fromPhone = await this.resolvePhoneFromJid(lineId, `${lidDetectado}@lid`);
    }

    // Paso 4: ÚLTIMO RECURSO — Usar el LID limpio como ID temporal
    // El mensaje NO se descarta. Cuando llegue otro mensaje con remoteJidAlt, o enviemos uno, se reconcilia solo.
    if (!fromPhone && lidDetectado) {
      fromPhone = lidDetectado;
      console.log(`⚠️ LID sin resolver aún, usando como ID temporal: ${lidDetectado}`);
    }

    // 🛑 FILTRO FINAL: Si no tenemos absolutamente nada, abortar.
    if (!fromPhone) {
      console.log(`⚠️ JID irresoluble (incluso sin LID temporal): ${raw}`);
      continue;
    }

    // Actualizar caché preventivamente (si fromPhone vino del Paso 3)
    if (lidDetectado && fromPhone && fromPhone !== lidDetectado) {
      this.lidCache.set(`${lineId}:${lidDetectado}`, fromPhone);
    }

    // ─── AUTO BLACKLIST ───
    try {
      const config = await this.prisma.app_config.findUnique({ where: { key: 'blacklist_keywords' } })
      const keywords = config?.value ? JSON.parse(config.value) : ['basta', 'no molesten', 'no me interesa', 'no, gracias', 'eliminar', 'stop', 'darme de baja']
      const lowerBody = messageBody.toLowerCase()
      const matched = keywords.find(k => lowerBody.includes(k.toLowerCase()))

      if (matched) {
        const existing = await this.prisma.blacklist.findFirst({ where: { phone: fromPhone } })
        if (existing) {
          await this.prisma.blacklist.update({ where: { id: existing.id }, data: { reason: `auto: "${matched}"` } })
        } else {
          await this.prisma.blacklist.create({ data: { phone: fromPhone, reason: `auto: "${matched}"`, owner_id: null } })
        }

        console.log(`🚫 Auto-blacklist: ${fromPhone} por keyword "${matched}"`)
        this.io.emit('auto_blacklist', { phone: fromPhone, keyword: matched, timestamp: new Date() })
      }
    } catch (e) {
      console.error('Auto-blacklist error:', e)
    }

    // ─── REPLY TRACKING ───
    try {
      const recentLog = await this.prisma.campaign_logs.findFirst({
        where: {
          contact_phone: fromPhone,
          status: 'sent',
          created_at: { gte: new Date(Date.now() - 30 * 86400000) }
        },
        orderBy: { created_at: 'desc' }
      })

      if (recentLog && !recentLog.has_reply) {
        await this.prisma.campaign_logs.update({ where: { id: recentLog.id }, data: { has_reply: true, replied_at: new Date() } })
        this.io.emit('campaign_reply', { campaign_id: recentLog.campaign_id, phone: fromPhone, message: messageBody, timestamp: new Date() })
      }
    } catch (e) {
      console.error('Reply tracking error:', e)
    }
  }
})

  waClient.ev.on('messages.update', async (updates) => {
      for (const update of updates) {
        const { key, update: statusUpdate } = update;
        if (!key.id || !key.remoteJid) continue;

        // ==========================================
        // 🕵️‍♂️ CAZADOR DE LIDs (Vía Acuse de Recibo)
        // ==========================================
        if (key.remoteJid.includes('@lid')) {
            const lidClean = key.remoteJid.split('@')[0];
            
            // Si no lo tenemos en caché, investigamos de quién es este acuse
            if (!this.lidCache.has(`${lineId}:${lidClean}`)) {
                try {
                    // Buscamos a quién le enviamos ESTE mensaje exacto
                    const logSent = await this.prisma.campaign_logs.findFirst({
                        where: { message_id: key.id }
                    });

                    if (logSent && logSent.contact_phone) {
                        const realPhone = logSent.contact_phone;
                        
                        // ¡BINGO! Llenamos el diccionario y la BD en silencio
                        this.lidCache.set(`${lineId}:${lidClean}`, realPhone);
                        await this.prisma.lid_mappings.upsert({
                            where: { lineId_lid: { lineId, lid: lidClean } },
                            update: { phone: realPhone, updatedAt: new Date() },
                            create: { lineId, lid: lidClean, phone: realPhone }
                        }).catch(() => {});
                        
                        console.log(`🕵️‍♂️ LID mapeado por Acuse de Recibo: ${lidClean} -> ${realPhone}`);
                    }
                } catch (e) {
                    console.error("Error en Cazador de LIDs:", e);
                }
            }
        }

        // ==========================================
        // ─── LÓGICA DE LECTURA (Tu código original)
        // ==========================================
        const phone = await this.resolvePhoneFromJid(lineId, key.remoteJid);
        if (!phone) continue;

        const log = await this.prisma.campaign_logs.findFirst({
          where: { contact_phone: phone, message_id: key.id }, // Mejor buscar por message_id también
          orderBy: { created_at: 'desc' }
        });

        if (!log) continue;

        const ack = statusUpdate?.status || statusUpdate?.ack;

        if (ack === 2 && !log.delivered_at) {
          await this.prisma.campaign_logs.update({
            where: { id: log.id },
            data: { delivered_at: new Date() }
          });
        }

        if (ack === 3 && !log.read_at) {
          await this.prisma.campaign_logs.update({
            where: { id: log.id },
            data: { read_at: new Date() }
          });
        }
      }
    });


waClient.ev.on('contacts.upsert', async (contacts) => {
  for (const contact of contacts) {
    await this._storeLidMappingFromContact(lineId, contact).catch(() => {})
  }
})

waClient.ev.on('contacts.update', async (updates) => {
  for (const update of updates) {
    await this._storeLidMappingFromContact(lineId, update).catch(() => {})
  }
})
  }

    cleanJid(jid) {
    if (!jid) return ''
    return jid.split('@')[0].split(':')[0].replace(/\D/g, '')
  }

  // ← PEGAR AQUÍ, debajo de cleanJid
  decodeJid(jid) {
    if (!jid || typeof jid !== 'string') return jid
    try {
      if (jid.includes(':') || jid.includes('@')) {
        const decoded = jidDecode(jid)
        if (decoded?.user && decoded?.server) {
          return `${decoded.user}@${decoded.server}`
        }
      }
    } catch (e) {}
    return jid
  }


  // NUEVO (no había antes):
async _storeLidMappingFromContact(lineId, contact) {
  if (!contact?.id) return

  let lid = null
  let phone = null

  // Caso A: contact.id es un LID y trae phoneNumber
  // { id: '27590987374634@lid', phoneNumber: '+5491234567890' }
  if (contact.id.includes('@lid') && contact.phoneNumber) {
    lid = contact.id.split('@')[0]
    phone = contact.phoneNumber.replace(/\D/g, '')
  }

  // Caso B: contact.id es un phone JID y trae el campo lid
  // { id: '5491234567890@s.whatsapp.net', lid: '27590987374634' }
  if (contact.id.includes('@s.whatsapp.net') && contact.lid) {
    phone = contact.id.split('@')[0].replace(/\D/g, '')
    lid = typeof contact.lid === 'string'
      ? contact.lid.split('@')[0]
      : String(contact.lid)
  }

  if (!lid || !phone || phone.length < 8) return

  const cacheKey = `${lineId}:${lid}`
  if (this.lidCache.get(cacheKey) === phone) return // ya lo tenemos, no tocar BD

  this.lidCache.set(cacheKey, phone)
  await this.prisma.lid_mappings.upsert({
    where: { lineId_lid: { lineId, lid } },
    update: { phone, updatedAt: new Date() },
    create: { lineId, lid, phone }
  }).catch(() => {})
  console.log(`🔗 LID mapeado: ${lid} → ${phone} (línea ${lineId})`)
}

  async resolvePhoneFromJid(lineId, rawJid) {
    if (!rawJid) return null

    // Intento 0: caché en memoria (sin I/O, instantáneo)
    if (rawJid.includes('@lid')) {
      const lidClean = rawJid.split('@')[0]
      const cached = this.lidCache.get(`${lineId}:${lidClean}`)
      if (cached) return cached
    }

    // Intento 1: jidDecode — funciona para @s.whatsapp.net, NO para @lid
    const decoded = this.decodeJid(rawJid)
    if (decoded?.includes('@s.whatsapp.net')) {
      return this.cleanJid(decoded)
    }

    // Intento 2: jidNormalizedUser — ídem, solo formatea, no resuelve LIDs
    try {
      const normalized = jidNormalizedUser(rawJid)
      if (normalized?.includes('@s.whatsapp.net')) {
        return this.cleanJid(normalized)
      }
    } catch (e) {}

    // Intento 3: BD — y popular el caché para la próxima vez
    if (rawJid.includes('@lid')) {
      const lidClean = rawJid.split('@')[0]
      const mapping = await this.prisma.lid_mappings.findUnique({
        where: { lineId_lid: { lineId, lid: lidClean } }
      }).catch(() => null)
      if (mapping?.phone) {
        this.lidCache.set(`${lineId}:${lidClean}`, mapping.phone) // ← popular caché
        return this.cleanJid(mapping.phone)
      }
    }

    return null
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

      const sentMsg = await waClient.sendMessage(jid, messagePayload)

      if (sentMsg?.key?.remoteJid?.includes('@lid')) {
      const lid = sentMsg.key.remoteJid.split('@')[0]
      this.lidCache.set(`${lineId}:${lid}`, cleanNumber)
      await this.prisma.lid_mappings.upsert({
        where: { lineId_lid: { lineId, lid } },
        update: { phone: cleanNumber, updatedAt: new Date() },
        create: { lineId, lid, phone: cleanNumber }
      }).catch(() => {})
      console.log(`🔗 LID capturado al enviar: ${lid} → ${cleanNumber}`)
    }

    return { success: true, messageId: sentMsg?.key?.id }
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

       const sentMsg = await waClient.sendMessage(jid, messagePayload)

      if (sentMsg?.key?.remoteJid?.includes('@lid')) {
        const lid = sentMsg.key.remoteJid.split('@')[0]
        this.lidCache.set(`${lineId}:${lid}`, cleanNumber)
        await this.prisma.lid_mappings.upsert({
          where: { lineId_lid: { lineId, lid } },
          update: { phone: cleanNumber, updatedAt: new Date() },
          create: { lineId, lid, phone: cleanNumber }
        }).catch(() => {})
        console.log(`🔗 LID capturado al enviar (human): ${lid} → ${cleanNumber}`)
      }

      console.log(`[HUMAN MODE] ✅ Mensaje enviado`)
      return { success: true, messageId: sentMsg?.key?.id }
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
        const blacklisted = await this.prisma.blacklist.findFirst({
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

      const sendOptions = { type: imageUrl ? 'image' : 'text', imageUrl }
      let sendResult; // Esta variable atrapará el objeto con el messageId
      
      if (options.humanMode) {
        sendResult = await this.sendMessageHuman(lineaAsignada.id, target.phone, personalized, sendOptions)
      } else {
        sendResult = await this.sendMessage(lineaAsignada.id, target.phone, personalized, sendOptions)
      }

      // ¡AQUÍ ESTÁ LA MAGIA! Extraemos el ID directamente del resultado
      const exactMessageId = sendResult?.messageId;

      // 2. Guardamos el log en BD
      await this.prisma.campaign_logs.create({
        data: {
          campaign_id: campaignId,
          line_id: lineaAsignada.id,
          contact_phone: target.phone,
          status: 'sent',
          message_id: exactMessageId // 🔥 Guardamos el ID para el cazador de Acuses
        }
      }).catch(() => {})


            // ✅ Incrementar sent en DB
      await this.prisma.campaigns.update({
        where: { id: campaignId },
        data: { sent: { increment: 1 } }
      }).catch(e => console.error(`[DB] Error incrementando sent:`, e.message))

      // ✅ Guardar en results para el resumen final
      results.push({ phone: target.phone, status: 'sent', lineId: lineaAsignada.id, index: i })

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

  console.log(`[CAMPAIGN] ${campaignId} finalizada. Results:`, results.length, 'sent:', results.filter(r => r.status === 'sent').length, 'failed:', results.filter(r => r.status === 'failed').length)

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