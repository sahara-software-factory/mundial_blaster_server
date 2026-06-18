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

function resolveSpintax(text) {
  if (!text) return ''
  return text.replace(/\{\{([^}]+)\}\}/g, (match, content) => {
    if (!content.includes('|')) return match
    const variants = content.split("|").map(s => s.trim()).filter(Boolean)
    return variants.length ? variants[Math.floor(Math.random() * variants.length)] : ''
  })
}

function maskPhone(p) {
  if (!p || typeof p !== 'string') return '???'
  if (p.length <= 6) return p
  return p.slice(0, 3) + '****' + p.slice(-2)
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
    this.maxLidCacheSize = 5000
    fs.ensureDirSync(this.sessionsDir)
  }

  _setLidCache(key, value) {
    if (this.lidCache.size >= this.maxLidCacheSize) {
      const firstKey = this.lidCache.keys().next().value
      this.lidCache.delete(firstKey)
    }
    this.lidCache.set(key, value)
  }

  async init() {
    console.log('✅ WabiSend WAService inicializado')
    try {
      const lines = await this.prisma.lineas_whatsapp.findMany({
        where: { status: 'CONECTADA' }
      })
      for (const line of lines) {
        if (line.phone) {
          await this.connect(line.phone).catch(e => console.error(e))
          await new Promise(r => setTimeout(r, 1500))
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
        console.warn(`⚠️ Línea no registrada: ${maskPhone(phone)}`)
        return
      }
      const lineId = line.id
      const sessionPath = path.join(this.sessionsDir, String(lineId))
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
        browser: ['WabiSend', 'Chrome', '120.0.0'],
        msgRetryCounterCache: this.msgRetryCounterCache,
        getMessage: async (key) => {
          return { conversation: 'WabiSend' }
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
    waClient.ev.on('creds.update', saveCreds)

    waClient.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update
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
      if (connection === 'open') {
        console.log(`✅ Conectado: ${lineId}`)
        if (this.reconnectTimers[lineId]) {
          clearTimeout(this.reconnectTimers[lineId])
          delete this.reconnectTimers[lineId]
        }
        if (this.presenceIntervals[lineId]) {
          clearInterval(this.presenceIntervals[lineId])
          delete this.presenceIntervals[lineId]
        }
        this.presenceIntervals[lineId] = setInterval(async () => {
          try {
            if (waClient?.ws?.readyState === 1) {
              await waClient.sendPresenceUpdate('available')
            }
          } catch (e) {}
        }, 180000)
        this.io.emit('status', { lineId, status: 'CONECTADA' })
        await this.prisma.lineas_whatsapp.update({
          where: { id: lineId },
          data: { status: 'CONECTADA' }
        }).catch(() => {})
        return
      }
      if (connection === 'close') {
        if (this.presenceIntervals[lineId]) {
          clearInterval(this.presenceIntervals[lineId])
          delete this.presenceIntervals[lineId]
        }
        const statusCode = lastDisconnect?.error?.output?.statusCode
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut
        console.log(`❌ Desconectado ${lineId}. Razón: ${statusCode}`)
        if (statusCode === 401) {
          console.log(`🔒 Sesión invalidada (401). Requiere reescanear QR.`)
          await fs.remove(path.join(this.sessionsDir, String(lineId))).catch(() => {})
          this.clients.delete(lineId)
          await this.prisma.lineas_whatsapp.update({
            where: { id: lineId },
            data: { status: 'DESCONECTADA' }
          }).catch(() => {})
          this.io.emit('status', { lineId, status: 'DESCONECTADA', reason: 'SESSION_INVALID', phone: maskPhone(phone) })
          return
        }
        if (shouldReconnect) {
          if (this.reconnectTimers[lineId]) clearTimeout(this.reconnectTimers[lineId])
          let delay = 5000
          if (statusCode === 503 || statusCode === 428 || statusCode === 515) {
            delay = 15000
          } else if (statusCode === 408) {
            delay = 8000
          } else if (statusCode === 440) {
            delay = 10000
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
        if (!msg.message) continue
        const messageBody = msg.message?.conversation || msg.message?.extendedTextMessage?.text || ''
        const raw = msg.key.remoteJid
        const alt = msg.key.remoteJidAlt
        let fromPhone = null
        let lidDetectado = null

        if (raw?.includes('@s.whatsapp.net')) {
          fromPhone = raw.split('@')[0].replace(/\D/g, '')
        } else if (raw?.includes('@lid')) {
          lidDetectado = raw.split('@')[0]
        }
        if (alt?.includes('@s.whatsapp.net')) {
          fromPhone = alt.split('@')[0].replace(/\D/g, '')
        } else if (alt?.includes('@lid') && !lidDetectado) {
          lidDetectado = alt.split('@')[0]
        }

        if (fromPhone && lidDetectado) {
          this._setLidCache(`${lineId}:${lidDetectado}`, fromPhone)
          await this.prisma.lid_mappings.upsert({
            where: { lineId_lid: { lineId, lid: lidDetectado } },
            update: { phone: fromPhone, updatedAt: new Date() },
            create: { lineId, lid: lidDetectado, phone: fromPhone }
          }).catch(() => {})
        }

        if (!fromPhone && lidDetectado) {
          fromPhone = await this.resolvePhoneFromJid(lineId, `${lidDetectado}@lid`)
        }

        if (!fromPhone) {
          console.log(`⚠️ JID irresoluble: ${raw}, saltando procesamiento`)
          continue
        }

        if (lidDetectado && fromPhone && fromPhone !== lidDetectado) {
          this._setLidCache(`${lineId}:${lidDetectado}`, fromPhone)
        }

        if (msg.key.fromMe) {
          const cmd = messageBody.trim().toLowerCase()
          let reason = null
          if (cmd === '!blacklist' || cmd.startsWith('!blacklist ')) {
            reason = cmd.replace('!blacklist', '').trim() || 'manual desde chat'
          } else if (cmd === 'comprobante falso' || cmd === 'falso') {
            reason = 'comprobante falso'
          } else if (cmd === 'spam') {
            reason = 'spam'
          } else if (cmd === 'ban') {
            reason = 'ban manual'
          } else if (cmd === 'estafa') {
            reason = 'estafa'
          } else if (cmd === 'no') {
            reason = 'rechazado por operador'
          }

          if (reason && fromPhone) {
            try {
              const cleanPhone = fromPhone.replace(/\D/g, '')
              const entry = await this.prisma.blacklist.upsert({
                where: { phone: cleanPhone },
                update: { reason },
                create: { phone: cleanPhone, reason, owner_id: null }
              })
              console.log(`🛡️ Operador marcó blacklist: ${maskPhone(cleanPhone)} → ${reason}`)
              this.io.emit('operator_blacklist', {
                phone: cleanPhone,
                reason,
                timestamp: new Date(),
                entry
              })
            } catch (e) {
              console.error('Operator blacklist error:', e)
            }
          }
          continue
        }

        try {
          const config = await this.prisma.app_config.findUnique({ where: { key: 'blacklist_keywords' } })
          const keywords = config?.value ? JSON.parse(config.value) : ['basta', 'no molesten', 'no me interesa', 'no, gracias', 'eliminar', 'stop', 'darme de baja', 'no quiero que me envien mas mensajes']
          const lowerBody = messageBody.toLowerCase()
          const matched = keywords.find(k => lowerBody.includes(k.toLowerCase()))
          if (matched) {
            const cleanPhone = fromPhone.replace(/\D/g, '')
            const existing = await this.prisma.blacklist.findFirst({ where: { phone: cleanPhone } })
            if (existing) {
              await this.prisma.blacklist.update({ where: { id: existing.id }, data: { reason: `auto: "${matched}"` } })
            } else {
              await this.prisma.blacklist.create({ data: { phone: cleanPhone, reason: `auto: "${matched}"`, owner_id: null } })
            }
            console.log(`🚫 Auto-blacklist: ${maskPhone(cleanPhone)} por keyword "${matched}"`)
            this.io.emit('auto_blacklist', { phone: cleanPhone, keyword: matched, timestamp: new Date() })
          }
        } catch (e) {
          console.error('Auto-blacklist error:', e)
        }

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
            await this.prisma.campaign_logs.update({
              where: { id: recentLog.id },
              data: { has_reply: true, replied_at: new Date() }
            })
            this.io.emit('campaign_reply', {
              campaign_id: recentLog.campaign_id,
              phone: fromPhone,
              message: messageBody,
              timestamp: new Date()
            })
          }
        } catch (e) {
          console.error('Reply tracking error:', e)
        }
      }
    })

    waClient.ev.on('messages.update', async (updates) => {
      for (const update of updates) {
        const { key, update: statusUpdate } = update
        if (!key.id || !key.remoteJid) continue
        if (key.remoteJid.includes('@lid')) {
          const lidClean = key.remoteJid.split('@')[0]
          if (!this.lidCache.has(`${lineId}:${lidClean}`)) {
            try {
              const logSent = await this.prisma.campaign_logs.findFirst({
                where: { message_id: key.id }
              })
              if (logSent && logSent.contact_phone) {
                const realPhone = logSent.contact_phone
                this._setLidCache(`${lineId}:${lidClean}`, realPhone)
                await this.prisma.lid_mappings.upsert({
                  where: { lineId_lid: { lineId, lid: lidClean } },
                  update: { phone: realPhone, updatedAt: new Date() },
                  create: { lineId, lid: lidClean, phone: realPhone }
                }).catch(() => {})
                console.log(`🕵️‍♂️ LID mapeado por Acuse: ${lidClean} -> ${maskPhone(realPhone)}`)
              }
            } catch (e) {
              console.error("Error en Cazador de LIDs:", e)
            }
          }
        }
        const phone = await this.resolvePhoneFromJid(lineId, key.remoteJid)
        if (!phone) continue
        const log = await this.prisma.campaign_logs.findFirst({
          where: { contact_phone: phone, message_id: key.id },
          orderBy: { created_at: 'desc' }
        })
        if (!log) continue
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

  async _storeLidMappingFromContact(lineId, contact) {
    if (!contact?.id) return
    let lid = null
    let phone = null
    if (contact.id.includes('@lid') && contact.phoneNumber) {
      lid = contact.id.split('@')[0]
      phone = contact.phoneNumber.replace(/\D/g, '')
    }
    if (contact.id.includes('@s.whatsapp.net') && contact.lid) {
      phone = contact.id.split('@')[0].replace(/\D/g, '')
      lid = typeof contact.lid === 'string'
        ? contact.lid.split('@')[0]
        : String(contact.lid)
    }
    if (!lid || !phone || phone.length < 8) return
    const cacheKey = `${lineId}:${lid}`
    if (this.lidCache.get(cacheKey) === phone) return
    this._setLidCache(cacheKey, phone)
    await this.prisma.lid_mappings.upsert({
      where: { lineId_lid: { lineId, lid } },
      update: { phone, updatedAt: new Date() },
      create: { lineId, lid, phone }
    }).catch(() => {})
    console.log(`🔗 LID mapeado: ${lid} → ${maskPhone(phone)} (línea ${lineId})`)
  }

  async resolvePhoneFromJid(lineId, rawJid) {
    if (!rawJid) return null
    if (rawJid.includes('@lid')) {
      const lidClean = rawJid.split('@')[0]
      const cached = this.lidCache.get(`${lineId}:${lidClean}`)
      if (cached) return cached
    }
    const decoded = this.decodeJid(rawJid)
    if (decoded?.includes('@s.whatsapp.net')) {
      return this.cleanJid(decoded)
    }
    try {
      const normalized = jidNormalizedUser(rawJid)
      if (normalized?.includes('@s.whatsapp.net')) {
        return this.cleanJid(normalized)
      }
    } catch (e) {}
    if (rawJid.includes('@lid')) {
      const lidClean = rawJid.split('@')[0]
      const mapping = await this.prisma.lid_mappings.findUnique({
        where: { lineId_lid: { lineId, lid: lidClean } }
      }).catch(() => null)
      if (mapping?.phone) {
        this._setLidCache(`${lineId}:${lidClean}`, mapping.phone)
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
        this._setLidCache(`${lineId}:${lid}`, cleanNumber)
        await this.prisma.lid_mappings.upsert({
          where: { lineId_lid: { lineId, lid } },
          update: { phone: cleanNumber, updatedAt: new Date() },
          create: { lineId, lid, phone: cleanNumber }
        }).catch(() => {})
        console.log(`🔗 LID capturado al enviar: ${lid} → ${maskPhone(cleanNumber)}`)
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
    console.log(`[HUMAN MODE] Iniciando typing para ${maskPhone(contactPhone)} → JID: ${jid}`)
    try {
      await waClient.sendPresenceUpdate('composing', jid)
      const typingDelay = Math.min(8000, Math.max(2000, content.length * 50))
      console.log(`[HUMAN MODE] ⏳ Esperando ${typingDelay}ms (${content.length} caracteres)`)
      await new Promise(r => setTimeout(r, typingDelay))
      await waClient.sendPresenceUpdate('paused', jid)
      await new Promise(r => setTimeout(r, 500))
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
        this._setLidCache(`${lineId}:${lid}`, cleanNumber)
        await this.prisma.lid_mappings.upsert({
          where: { lineId_lid: { lineId, lid } },
          update: { phone: cleanNumber, updatedAt: new Date() },
          create: { lineId, lid, phone: cleanNumber }
        }).catch(() => {})
        console.log(`🔗 LID capturado al enviar (human): ${lid} → ${maskPhone(cleanNumber)}`)
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
    let lineasActivas = []
    if (Array.isArray(lineInput)) {
      lineasActivas = lineInput.filter(l => l.status === 'CONECTADA' && this.clients.has(l.id))
    } else if (typeof lineInput === 'string') {
      const line = await this.prisma.lineas_whatsapp.findUnique({ where: { id: lineInput } })
      if (line && line.status === 'CONECTADA') lineasActivas = [line]
    } else if (lineInput && lineInput.id) {
      lineasActivas = [lineInput]
    }
    if (lineasActivas.length === 0) {
      throw new Error('No hay líneas conectadas disponibles para enviar')
    }
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

      if (!target || !target.phone || typeof target.phone !== 'string') {
        console.warn(`⚠️ Target inválido en índice ${i}, saltando`)
        continue
      }

      if (options.skipBlacklist) {
        const cleanPhone = target.phone.replace(/\D/g, '')
        const blacklisted = await this.prisma.blacklist.findFirst({
          where: { phone: cleanPhone }
        })
        if (blacklisted) {
          console.log(`⛔ Saltando ${maskPhone(target.phone)} — blacklist`)
          await this.prisma.campaign_logs.create({
            data: {
              campaign_id: campaignId,
              contact_phone: target.phone,
              status: 'skipped_blacklist',
              line_id: null,
              owner_id: options.ownerId || null,
            }
          }).catch(() => {})
          continue
        }
      }

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
        const quedanLineasGlobales = lineasActivas.some(l => !lineasCaidas.has(l.id))
        if (!quedanLineasGlobales) {
          console.log(`🛑 TODAS LAS LÍNEAS CAÍDAS. Deteniendo campaña ${campaignId} en ${i}/${targets.length}`)
          await this.prisma.campaign_logs.create({
            data: {
              campaign_id: campaignId,
              line_id: null,
              contact_phone: target.phone,
              status: 'failed',
              error: 'Todas las líneas offline - Campaña detenida por desconexión total',
              owner_id: options.ownerId || null,
            }
          }).catch(() => {})
          this.io.emit('campaign_log', {
            campaignId: campaignId,
            campaign_id: campaignId,
            phone: target.phone,
            contact_phone: target.phone,
            status: 'failed',
            lineId: null,
            line_id: null,
            linePhone: null,
            line_phone: null,
            error: 'TODAS LAS LÍNEAS CAÍDAS - CAMPAÑA DETENIDA',
            progress: `${i + 1}/${targets.length}`,
            isEmergencyStop: true
          })
          wasCancelled = true
          break
        }
        results.push({ phone: target.phone, status: 'failed', error: 'Todas las líneas offline', index: i })
        await this.prisma.campaign_logs.create({
          data: {
            campaign_id: campaignId,
            line_id: null,
            contact_phone: target.phone,
            status: 'failed',
            error: 'Todas las líneas offline',
            owner_id: options.ownerId || null,
          }
        }).catch(() => {})
        continue
      }

      const waClientCheck = this.clients.get(lineaAsignada.id)
      if (!waClientCheck || !waClientCheck.user) {
        lineasCaidas.add(lineaAsignada.id)
        i--
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
        let sendResult
        if (options.humanMode) {
          sendResult = await this.sendMessageHuman(lineaAsignada.id, target.phone, personalized, sendOptions)
        } else {
          sendResult = await this.sendMessage(lineaAsignada.id, target.phone, personalized, sendOptions)
        }

        const exactMessageId = sendResult?.messageId

        await this.prisma.campaign_logs.create({
          data: {
            campaign_id: campaignId,
            line_id: lineaAsignada.id,
            contact_phone: target.phone,
            status: 'sent',
            message_id: exactMessageId,
            owner_id: options.ownerId || null,
          }
        }).catch(() => {})

        await this.prisma.campaigns.update({
          where: { id: campaignId },
          data: { sent: { increment: 1 } }
        }).catch(e => console.error(`[DB] Error incrementando sent:`, e.message))

        results.push({ phone: target.phone, status: 'sent', lineId: lineaAsignada.id, index: i })

        this.io.emit('campaign_log', {
          campaignId: campaignId,
          campaign_id: campaignId,
          phone: target.phone,
          contact_phone: target.phone,
          status: 'sent',
          lineId: lineaAsignada.id,
          line_id: lineaAsignada.id,
          linePhone: lineaAsignada.phone,
          delayMs: lastDelayMs,
          line_phone: lineaAsignada.phone,
          progress: `${i + 1}/${targets.length}`
        })

        console.log(`✅ ${i + 1}/${targets.length} → ${maskPhone(target.phone)} [${maskPhone(lineaAsignada.phone)}]`)
        lineaIndex++

      } catch (err) {
        console.error(`❌ ${maskPhone(target.phone)} [${maskPhone(lineaAsignada.phone)}]:`, err.message)
        lineasCaidas.add(lineaAsignada.id)

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
            error: err.message?.slice(0, 200),
            owner_id: options.ownerId || null,
          }
        }).catch(() => {})

        const quedanLineas = lineasActivas.some(l => !lineasCaidas.has(l.id))
        if (quedanLineas) {
          i--
        } else {
          console.log(`🛑 TODAS LAS LÍNEAS CAÍDAS (catch). Deteniendo campaña ${campaignId}`)
          results.push({ phone: target.phone, status: 'failed', error: err.message, index: i })

          await this.prisma.campaign_logs.create({
            data: {
              campaign_id: campaignId,
              line_id: lineaAsignada.id,
              contact_phone: target.phone,
              status: 'failed',
              error: 'Todas las líneas caídas - Campaña detenida',
              owner_id: options.ownerId || null,
            }
          }).catch(() => {})

          this.io.emit('campaign_log', {
            campaignId: campaignId,
            campaign_id: campaignId,
            phone: target.phone,
            contact_phone: target.phone,
            status: 'failed',
            lineId: lineaAsignada.id,
            line_id: lineaAsignada.id,
            linePhone: lineaAsignada.phone,
            line_phone: lineaAsignada.phone,
            error: 'TODAS LAS LÍNEAS CAÍDAS - CAMPAÑA DETENIDA',
            progress: `${i + 1}/${targets.length}`,
            isEmergencyStop: true
          })

          wasCancelled = true
          break
        }
      }

      if (i < targets.length - 1 && !wasCancelled) {
        const baseDelay = Math.floor(Math.random() * (delayMax - delayMin + 1)) + delayMin
        const humanExtra = options.humanMode ? (3000 + Math.random() * 5000) : 0
        lastDelayMs = baseDelay + humanExtra
        console.log(`[DELAY] Esperando ${lastDelayMs}ms antes del siguiente mensaje`)
        await new Promise(r => setTimeout(r, lastDelayMs))
      }
    }

    const finalStatus = wasCancelled ? 'cancelled' : 'completed'
    await this.prisma.campaigns.update({
      where: { id: campaignId },
      data: { status: finalStatus, finished_at: new Date() }
    }).catch(() => {})

    console.log('🔥 EMITIENDO campaign_complete:', {
      campaignId,
      sent: results.filter(r => r.status === 'sent').length,
      failed: results.filter(r => r.status === 'failed').length
    })

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

process.on('SIGTERM', () => {
  console.log('🛑 SIGTERM recibido. Limpiando timers y sockets...')
  process.exit(0)
})

module.exports = { WAService }