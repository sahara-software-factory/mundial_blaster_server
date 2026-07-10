const {
  makeWASocket,
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  jidDecode,
  jidNormalizedUser
} = require('@whiskeysockets/baileys')
const pino = require('pino')
const { usePrismaAuthState } = require('./usePrismaAuthState');
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
   constructor(prisma, io, validateLicenseFn, tierLimits) {
    this.prisma = prisma
    this.io = io
    this.validateLicense = validateLicenseFn
    this.tierLimits = tierLimits || {}
    this.clients = new Map()
    this.connectingLocks = new Set()
    this.reconnectTimers = {}
    this.presenceIntervals = {}
    this.sessionsDir = '/app/sessions'
    this.msgRetryCounterCache = new NodeCache()
    this.lidCache = new Map()
    this.lineOwners = new Map()
    this.campaignActive = new Map()
    this.sessionHealth = new Map()
    this.disconnectHistory = {} 
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
  if (process.env.CONNECT_WHATSAPP_ON_START === 'false') {
      console.log('⚠️ Autoconexión deshabilitada en entorno local.')
      return 
  }
  try {
    const lines = await this.prisma.lineas_whatsapp.findMany().catch(e => {
      console.error('⚠️ DB lineas_whatsapp no accesible:', e.message)
      return []
    })
    for (const line of lines) {
      if (line.phone && line.status === 'CONECTADA') {
        // 🔥 CRÍTICO: cargar owner para emits dirigidos post-redeploy
        this.lineOwners.set(line.id, line.owner_id || null)

        this.connect(line.phone).catch(e => console.error('Connect error:', e.message))
        await new Promise(r => setTimeout(r, 1000))
      }
    }
  } catch (e) {
    console.error('Error init:', e.message)
  }
}

async connect(phone) {
  try {
    let line = await this.prisma.lineas_whatsapp.findUnique({ where: { phone } })
    if (!line) {
      console.warn(`⚠️ Línea no registrada: ${phone}`)
      return null
    }

    const lineId = line.id

    // 🔥 EL CANDADO ANTI-CLONES: Evita que el frontend dispare esto 2 veces seguidas
    if (this.connectingLocks.has(lineId)) {
      console.log(`⏳ Conexión ya en progreso para ${lineId}, ignorando petición duplicada.`)
      return line
    }
    this.connectingLocks.add(lineId) // Ponemos el candado

    const ownerId = line.owner_id || null
    this.lineOwners.set(lineId, ownerId)
    
    // Limpiar flag CANCELLED si existe
    if (this.reconnectTimers[lineId] === 'CANCELLED') {
      delete this.reconnectTimers[lineId]
    }
    
    // Cerrar socket previo si quedó colgado (zombie)
    const existing = this.clients.get(lineId)
    if (existing) {
      try { existing.end(undefined) } catch {} // Usamos end() en lugar de close()
      this.clients.delete(lineId)
      await new Promise(r => setTimeout(r, 500))
    }

    // Usar la Base de Datos Neon (PostgreSQL)
    const { state, saveCreds } = await usePrismaAuthState(this.prisma, lineId);
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
      getMessage: async (key) => ({ conversation: 'WabiSend' }),
      markOnlineOnConnect: false,
      syncFullHistory: false,
      connectTimeoutMs: 60000,
      keepAliveIntervalMs: 25000,
      retryRequestDelayMs: 3000,
      defaultQueryTimeoutMs: 60000,
    })

    this.clients.set(lineId, waClient)
    this.setupEvents(waClient, lineId, phone, saveCreds)
    
    this.connectingLocks.delete(lineId) // Quitamos el candado al terminar
    return line

  } catch (err) {
    console.error('❌ Error connect:', err)
    // Importante: Si hay error, quitamos el candado para que no quede bloqueado
    if (line) this.connectingLocks.delete(line.id)
    return null
  }
}

setupEvents(waClient, lineId, phone, saveCreds) {
    // ─── GUARDADO DE CREDENCIALES ───
    waClient.ev.on('creds.update', saveCreds)

    // ─── MANEJO DE CONEXIÓN Y ESTADO ───
    waClient.ev.on('connection.update', async (update) => {
        const { connection, lastDisconnect, qr } = update

        // ─── 1. EVENTO QR (LIMPIO: Sin autodestrucción) ───
        if (qr) {
            try {
                const qrDataUrl = await QRCode.toDataURL(qr)
                const ownerId = this.lineOwners ? this.lineOwners.get(lineId) : null
                
                if (ownerId && this.io.emitToUser) {
                    this.io.emitToUser(ownerId, 'qr', { lineId, qr: qrDataUrl })
                } else {
                    this.io.emit('qr', { lineId, qr: qrDataUrl })
                }
                
                await this.prisma.lineas_whatsapp.update({
                    where: { id: lineId },
                    data: { status: 'PENDING' }
                }).catch(() => {})
                
                console.log(`📱 QR emitido al frontend: ${lineId}`)
            } catch (e) {
                console.error('❌ Error generando QR:', e)
            }
            return
        }

        // ─── 2. EVENTO CONECTADO ───
        if (connection === 'open') {
            console.log(`✅ Conectado: ${lineId}`)

            // Limpiar timers previos
            if (this.reconnectTimers[lineId] && typeof this.reconnectTimers[lineId] !== 'string') {
                clearTimeout(this.reconnectTimers[lineId])
                delete this.reconnectTimers[lineId]
            }
            if (this.presenceIntervals[lineId]) {
                clearInterval(this.presenceIntervals[lineId])
                delete this.presenceIntervals[lineId]
            }

            // Ping de presencia para mantener viva la conexión
            this.presenceIntervals[lineId] = setInterval(async () => {
                try {
                    if (this.campaignActive && this.campaignActive.get(lineId)) return
                    if (waClient?.ws?.readyState === 1) {
                        await waClient.sendPresenceUpdate('available')
                    }
                } catch (e) {}
            }, 180000)

            // Notificar al Frontend
            const ownerId = this.lineOwners ? this.lineOwners.get(lineId) : null
            const payload = { lineId, status: 'CONECTADA', phone: maskPhone(phone) }
            
            if (ownerId && this.io.emitToUser) this.io.emitToUser(ownerId, 'status', payload)
            else this.io.emit('status', payload)

            // Actualizar Base de Datos
            await this.prisma.lineas_whatsapp.update({
                where: { id: lineId },
                data: { status: 'CONECTADA' }
            }).catch(e => console.error(`❌ DB update CONECTADA falló:`, e.message))
            return
        }

        // ─── 3. EVENTO CERRADO / DESCONECTADO ───
        if (connection === 'close') {
    if (this.presenceIntervals[lineId]) {
        clearInterval(this.presenceIntervals[lineId])
        delete this.presenceIntervals[lineId]
    }

    const statusCode = lastDisconnect?.error?.output?.statusCode
    const isLoggedOut = statusCode === DisconnectReason.loggedOut || statusCode === 401

    // ─── 440: CONEXIÓN REEMPLAZADA (otra instancia) ───
    // Esto pasa cuando health check + reconexión automática compiten
    if (statusCode === 440) {
        console.log(`⚠️ Conexión reemplazada (440): ${lineId}. Otra instancia robó la sesión.`)
        
        // Limpiar todo para que este nodo no pelee más
        this.clients.delete(lineId)
        this.sessionHealth.delete(lineId)
        if (this.reconnectTimers[lineId] && typeof this.reconnectTimers[lineId] === 'number') {
            clearTimeout(this.reconnectTimers[lineId])
        }
        delete this.reconnectTimers[lineId]
        
        // Limpiar sesión del disco (aunque sea efímero, evita que Baileys lea basura)
        await fs.remove(path.join(this.sessionsDir, String(lineId))).catch(() => {})
        
        // Actualizar DB para que el health check no intente reconectar
        await this.prisma.lineas_whatsapp.update({
            where: { id: lineId },
            data: { status: 'DESCONECTADA' }
        }).catch(() => {})
        
        // Notificar al frontend
        const ownerId = this.lineOwners ? this.lineOwners.get(lineId) : null
        const payload = { 
            lineId, 
            status: 'DESCONECTADA', 
            reason: 'CONNECTION_REPLACED', 
            phone: maskPhone(phone),
            message: 'Otra sesión conectó con este número. Reconectá desde el panel.'
        }
        if (ownerId && this.io.emitToUser) this.io.emitToUser(ownerId, 'status', payload)
        else this.io.emit('status', payload)
        
        return
    }

    console.log(`❌ Desconectado ${lineId}. Razón: ${statusCode}`)

   // ─── 401: SESIÓN INVALIDADA (logout o expirada) ───
    if (isLoggedOut) {
        console.log(`🔒 Sesión invalidada (401): ${lineId}`)
        
        this.clients.delete(lineId)
        this.sessionHealth.delete(lineId)
        
        // 🔥 ESTA ES LA CLAVE: Borramos las credenciales corruptas de NEON
        await this.prisma.wa_sessions.deleteMany({ 
            where: { sessionId: lineId } 
        }).catch(() => {})
        
        await this.prisma.lineas_whatsapp.update({
            where: { id: lineId },
            data: { status: 'DESCONECTADA' }
        }).catch(() => {})

        const ownerId = this.lineOwners ? this.lineOwners.get(lineId) : null
        const payload = { 
            lineId, 
            status: 'DESCONECTADA', 
            reason: 'SESSION_INVALID', 
            phone: maskPhone(phone) 
        }
        if (ownerId && this.io.emitToUser) this.io.emitToUser(ownerId, 'status', payload)
        else this.io.emit('status', payload)
        
        return
    }

    // ─── CANCELLED POR USUARIO (stopLine) ───
    if (this.reconnectTimers[lineId] === 'CANCELLED') {
        console.log(`⏹️ Reconexión abortada por stopLine: ${lineId}`)
        this.clients.delete(lineId)
        this.sessionHealth.delete(lineId)
        
        await this.prisma.lineas_whatsapp.update({
            where: { id: lineId },
            data: { status: 'DESCONECTADA' }
        }).catch(() => {})
        
        const ownerId = this.lineOwners ? this.lineOwners.get(lineId) : null
        const payload = { lineId, status: 'DESCONECTADA', reason: 'CANCELLED_BY_USER', phone: maskPhone(phone) }
        if (ownerId && this.io.emitToUser) this.io.emitToUser(ownerId, 'status', payload)
        else this.io.emit('status', payload)
        return
    }

    // ─── REVOKED (Meta invalidó durante reconexión) ───
    if (this.reconnectTimers[lineId] === 'REVOKED') {
        console.log(`⏹️ Reconexión bloqueada por sesión revocada: ${lineId}`)
        this.clients.delete(lineId)
        return
    }

    // ─── RECONEXIÓN AUTOMÁTICA (fallas de red temporales) ───
    console.log(`🔄 Reconexión temporal ${lineId}...`)
    this.clients.delete(lineId)

    // Anti-duplicado: si ya hay un timer corriendo, no crear otro
    if (this.reconnectTimers[lineId] && typeof this.reconnectTimers[lineId] === 'number') {
        clearTimeout(this.reconnectTimers[lineId])
    }

    const delay = (statusCode === 503 || statusCode === 428 || statusCode === 515) ? 15000 : 5000

    this.reconnectTimers[lineId] = setTimeout(() => {
        if (this.reconnectTimers[lineId] === 'CANCELLED' || this.reconnectTimers[lineId] === 'REVOKED') {
            console.log(`⏹️ Reconexión abortada (${this.reconnectTimers[lineId]}): ${lineId}`)
            return
        }
        this.connect(phone).catch(e => console.error(`❌ Reconexión fallida ${lineId}:`, e.message))
    }, delay)
}
    })

    // ─── MENSAJES (Mantenemos tu lógica de upsert intacta) ───
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

            // ─── 1. LÓGICA DE BLACKLIST MANUAL (Operador Ninja) ───
            if (msg.key.fromMe) {
                const cmd = messageBody.trim().toLowerCase()
                let reason = null

                // A. Modo Rápido: El operador manda solo un emoji de bloqueo
                if (['🚫', '⛔', '🔇'].includes(cmd)) {
                    reason = 'bloqueo_rapido_emoji'
                }
                // B. Modo Educado: El operador usa una frase natural, el cliente la lee, y el sistema actúa
                else if (cmd.includes('te doy de baja') || cmd.includes('te elimino de la base') || cmd.includes('no te escribimos más') || cmd.includes('te borro de la lista')) {
                    reason = 'baja_amable_operador'
                }
                // C. Modo Etiquetado Silencioso: Usar un punto al final para disimular (ej: "falso.")
                else if (cmd === '.falso' || cmd === 'falso.') reason = 'comprobante falso'
                else if (cmd === '.spam' || cmd === 'spam.') reason = 'spam'
                else if (cmd === '.estafa' || cmd === 'estafa.') reason = 'estafa'

                if (reason && fromPhone) {
                    try {
                        const cleanPhone = fromPhone.replace(/\D/g, '')
                        const existingBl = await this.prisma.blacklist.findFirst({
                            where: { phone: cleanPhone, owner_id: null }
                        })
                        let entry
                        if (existingBl) {
                            entry = await this.prisma.blacklist.update({
                                where: { id: existingBl.id },
                                data: { reason }
                            })
                        } else {
                            entry = await this.prisma.blacklist.create({
                                data: { phone: cleanPhone, reason, owner_id: null }
                            })
                        }
                        console.log(`🛡️ Operador Ninja marcó blacklist: ${maskPhone(cleanPhone)} → ${reason}`)
                        
                        const ownerId = entry?.owner_id || this.lineOwners.get(lineId) || null
                        const payload = { 
                            phone: cleanPhone, 
                            reason, 
                            timestamp: new Date(),
                            entry 
                        }
                        if (ownerId && this.io.emitToUser) {
                            this.io.emitToUser(ownerId, 'operator_blacklist', payload)
                        } else {
                            this.io.emit('operator_blacklist', payload)
                        }
                    } catch (e) {
                        console.error('Operator blacklist error:', e)
                    }
                }
                continue
            }

            // ─── 2. AUTO-BLACKLIST POR KEYWORDS (La Cúpula de Hierro) ───
            try {
                const config = await this.prisma.app_config.findUnique({ where: { key: 'blacklist_keywords' } })
                
                // Diccionario pesado de contingencia
                const defaultKeywords = [
                    // Educados
                    'no me interesa', 'no gracias', 'no, gracias', 'no quiero', 'paso,', 'paso gracias',
                    // Directos
                    'basta', 'stop', 'darme de baja', 'eliminar', 'no mandes', 'no escribas', 'sacame de', 'borrame', 'desuscribir', 'bloquear', 'denunciar',
                    // Enojados / Modismos (Argentina/LatAm)
                    'romper las bolas', 'rompiendo las bolas', 'hinchar los huevos', 'hinchando los huevos', 'no jodan', 'dejen de joder', 'no me jodas', 'vayanse a cagar', 'váyanse a cagar', 'pesados', 'harto', 'cansado', 'pelotudos', 'quien te paso', 'quien les paso', 'de donde sacaste', 'metete el',
                    // Emojis de rechazo
                    '🖕', '🛑', '🚫'
                ]
                
                const keywords = config?.value ? JSON.parse(config.value) : defaultKeywords
                const lowerBody = messageBody.toLowerCase()
                
                // Buscamos si alguna keyword coincide dentro del texto del cliente
                const matched = keywords.find(k => lowerBody.includes(k.toLowerCase()))
                
                if (matched) {
                    const cleanPhone = fromPhone.replace(/\D/g, '')
                    const existing = await this.prisma.blacklist.findFirst({ where: { phone: cleanPhone } })
                    
                    if (existing) {
                        await this.prisma.blacklist.update({ where: { id: existing.id }, data: { reason: `auto: "${matched}"` } })
                    } else {
                        await this.prisma.blacklist.create({ data: { phone: cleanPhone, reason: `auto: "${matched}"`, owner_id: null } })
                    }
                    
                    console.log(`🚫 Auto-blacklist activado: ${maskPhone(cleanPhone)} por keyword "${matched}"`)
                    
                    const ownerId = this.lineOwners.get(lineId) || null
                    const payload = { phone: cleanPhone, keyword: matched, timestamp: new Date() }
                    
                    if (ownerId && this.io.emitToUser) {
                        this.io.emitToUser(ownerId, 'auto_blacklist', payload)
                    } else {
                        this.io.emit('auto_blacklist', payload)
                    }
                }
            } catch (e) {
                console.error('Auto-blacklist error:', e)
            }

            // Reporte de respuestas
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
                    const ownerId = recentLog.owner_id || null
                    const payload = { 
                        campaign_id: recentLog.campaign_id, 
                        phone: fromPhone, 
                        message: messageBody, 
                        timestamp: new Date() 
                    }
                    if (ownerId && this.io.emitToUser) {
                        this.io.emitToUser(ownerId, 'campaign_reply', payload)
                    } else {
                        this.io.emit('campaign_reply', payload)
                    }
                }
            } catch (e) {
                console.error('Reply tracking error:', e)
            }
        }
    })

    // ─── MESSAGES.UPDATE (Lectura / Entregado) ───
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

    // ─── CONTACTS ───
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
    // console.log(`🔗 LID mapeado: ${lid} → ${maskPhone(phone)} (línea ${lineId})`)
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



  async sendCampaign(campaignId, lineInput, targets, message, options = {}) {
  // 🔐 INLINE LICENSE CHECK
  const licenseConfig = await this.prisma.app_config.findUnique({ where: { key: 'license' } })
  if (!licenseConfig?.value) throw new Error('LICENSE_REQUIRED')
  const license = this.validateLicense(licenseConfig.value)
  if (!license) throw new Error('LICENSE_INVALID')

  // Verificar tier para features avanzadas
  if (options.humanMode && license.tier === 'starter') throw new Error('TIER_UPGRADE_REQUIRED')
  if (options.imageUrl && license.tier === 'starter') throw new Error('TIER_UPGRADE_REQUIRED')

  // Defaults conservadores (15-25s) para evitar bans
  const { delayMin = 15000, delayMax = 25000, imageUrl = null } = options

  let lineasActivas = []
  if (Array.isArray(lineInput)) {
    lineasActivas = lineInput.filter(l => {
      const client = this.clients.get(l.id)
      return l.status === 'CONECTADA' && client && client.user
    })
  } else if (typeof lineInput === 'string') {
    const line = await this.prisma.lineas_whatsapp.findUnique({ where: { id: lineInput } })
    const client = line ? this.clients.get(line.id) : null
    if (line && line.status === 'CONECTADA' && client && client.user) lineasActivas = [line]
  } else if (lineInput && lineInput.id) {
    const client = this.clients.get(lineInput.id)
    if (lineInput.status === 'CONECTADA' && client && client.user) lineasActivas = [lineInput]
  }

  if (lineasActivas.length === 0) {
    throw new Error('No hay líneas conectadas y autenticadas disponibles para enviar')
  }

  // ─── ACTIVAR FLAG ANTI-PRESENCE (después de filtrar líneas) ───
  for (const line of lineasActivas) {
    this.campaignActive.set(line.id, true)
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

  try {
    for (let i = 0; i < targets.length; i++) {
      const target = targets[i]

      if (!target || !target.phone || typeof target.phone !== 'string') {
        console.warn(`⚠️ Target inválido en índice ${i}, saltando`)
        continue
      }

      const shouldCheckBlacklist = options.skipBlacklist !== false
if (shouldCheckBlacklist) {
  const cleanPhone = target.phone.replace(/\D/g, '')
  const blacklisted = await this.prisma.blacklist.findFirst({
    where: { phone: cleanPhone }
  })
  if (blacklisted) {
    console.log(`⛔ Saltando ${maskPhone(target.phone)} — blacklist (reason: ${blacklisted.reason})`)
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

        const ownerId = options.ownerId || null
        const payload = {
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
        }
        if (ownerId && this.io.emitToUser) {
          this.io.emitToUser(ownerId, 'campaign_log', payload)
        } else {
          this.io.emit('campaign_log', payload)
        }

        console.log(`✅ ${i + 1}/${targets.length} → ${maskPhone(target.phone)} [${maskPhone(lineaAsignada.phone)}]`)
        lineaIndex++

      } catch (err) {
        console.error(`❌ ${maskPhone(target.phone)} [${maskPhone(lineaAsignada.phone)}]:`, err.message)
        lineasCaidas.add(lineaAsignada.id)

        await this.prisma.campaigns.update({
          where: { id: campaignId },
          data: { failed: { increment: 1 } }
        }).catch(() => {})

        const failPayload = {
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
        }
        if (options.ownerId && this.io.emitToUser) {
          this.io.emitToUser(options.ownerId, 'campaign_log', failPayload)
        } else {
          this.io.emit('campaign_log', failPayload)
        }

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

          const emergencyPayload = {
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
          }
          if (options.ownerId && this.io.emitToUser) {
            this.io.emitToUser(options.ownerId, 'campaign_log', emergencyPayload)
          } else {
            this.io.emit('campaign_log', emergencyPayload)
          }

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
  } finally {
    // ─── LIMPIAR FLAG ANTI-PRESENCE (siempre, éxito o fallo) ───
    for (const line of lineasActivas) {
      this.campaignActive.set(line.id, false)
    }
    console.log(`🏁 Campaign ${campaignId} finalizada. Presence reactivado.`)
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

  const ownerId = options.ownerId || null
  const completePayload = {
    campaignId: campaignId,
    campaign_id: campaignId,
    status: finalStatus,
    sent: results.filter(r => r.status === 'sent').length,
    failed: results.filter(r => r.status === 'failed').length,
    total_sent: results.filter(r => r.status === 'sent').length,
    total_failed: results.filter(r => r.status === 'failed').length
  }
  if (ownerId && this.io.emitToUser) {
    this.io.emitToUser(ownerId, 'campaign_complete', completePayload)
  } else {
    this.io.emit('campaign_complete', completePayload)
  }

  console.log(`[CAMPAIGN] ${campaignId} finalizada. Results:`, results.length, 'sent:', results.filter(r => r.status === 'sent').length, 'failed:', results.filter(r => r.status === 'failed').length)

  return results
}


  async sendMessageHuman(lineId, contactPhone, content, options = {}) {
    // 1. INICIALIZAR Y VALIDAR CLIENTE (Esto siempre va primero)
    const waClient = this.clients.get(lineId) || this.clients.get(String(lineId))
    if (!waClient || !waClient.user) throw new Error('Línea no conectada')

    // 2. EXTRAER OPCIONES Y ARMAR EL JID
    const { type = 'text', imageUrl = null } = options
    const cleanNumber = this.cleanJid(contactPhone)
    const isGroup = cleanNumber.length > 15
    const jid = isGroup ? `${cleanNumber}@g.us` : `${cleanNumber}@s.whatsapp.net`
    
    console.log(`[HUMAN MODE] Iniciando typing para ${contactPhone} → JID: ${jid}`)
    
    try {
      // 3. SIMULAR ESCRITURA HUMANA
      const hasRealText = content && content.trim().length > 0
      const shouldSimulateTyping = hasRealText || !imageUrl 

      if (shouldSimulateTyping) {
        await waClient.sendPresenceUpdate('composing', jid)
        // Calcula el delay de typing basado en la longitud (entre 3 y 8 segundos)
        const typingDelay = Math.min(8000, Math.max(3000, (content || '').length * 100))
        console.log(`[HUMAN MODE] ⏳ Esperando ${typingDelay}ms`)
        
        await new Promise(r => setTimeout(r, typingDelay))
        await waClient.sendPresenceUpdate('paused', jid)
        await new Promise(r => setTimeout(r, 500))
      }

      // 4. ARMAR EL PAYLOAD Y ENVIAR
      let messagePayload = {}
      if (type === 'image' && imageUrl) {
        messagePayload = { image: { url: imageUrl }, caption: content || '' }
      } else {
        messagePayload = { text: content }
      }
      
      const sentMsg = await waClient.sendMessage(jid, messagePayload)
      
      // 5. CAZADOR DE LIDs (Para tracking posterior)
      if (sentMsg?.key?.remoteJid?.includes('@lid')) {
        const lid = sentMsg.key.remoteJid.split('@')[0]
        this._setLidCache(`${lineId}:${lid}`, cleanNumber)
        await this.prisma.lid_mappings.upsert({
          where: { lineId_lid: { lineId, lid } },
          update: { phone: cleanNumber, updatedAt: new Date() },
          create: { lineId, lid, phone: cleanNumber }
        }).catch(() => {})
        console.log(`🔗 LID capturado al enviar (human): ${lid} → ${cleanNumber}`)
      }
      
      console.log(`[HUMAN MODE] ✅ Mensaje enviado exitosamente`)
      return { success: true, messageId: sentMsg?.key?.id }
      
    } catch (err) {
      console.error(`[HUMAN MODE] ❌ Error:`, err.message)
      throw err
    }
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
      try { await client.logout() } catch (e) {
        console.log(`⚠️ Logout Baileys falló (ya desconectado?): ${e.message}`)
      }
      this.clients.delete(lineId)
    }
    try {
      // Borramos directamente de Neon, sin importar si hay carpeta o no
      await this.prisma.wa_sessions.deleteMany({ 
          where: { sessionId: lineId } 
      })
      console.log(`🧹 Sesión eliminada de la base de datos para: ${lineId}`)
    } catch (e) {
      console.log(`⚠️ No se pudo eliminar la sesión de la DB: ${e.message}`)
    }
    try {
       const exists = await this.prisma.lineas_whatsapp.findUnique({ where: { id: lineId } })
      if (exists) {
        this.lineOwners.delete(lineId)
        await this.prisma.lineas_whatsapp.update({
          where: { id: lineId },
          data: { status: 'DESCONECTADA' }
        })
      }
    } catch (e) {
      console.log(`⚠️ Error actualizando status: ${e.message}`)
    }
    return { success: true }
  }


    async stopLine(lineId) {
  console.log(`🛑 Deteniendo línea ${lineId.slice(0,8)}...`)
  
  // 1. Limpiar timer real si existe
  if (this.reconnectTimers[lineId] && typeof this.reconnectTimers[lineId] === 'number') {
    clearTimeout(this.reconnectTimers[lineId])
  }
  
  // 2. Setear flag para que el handler de 'close' no reconecte
  this.reconnectTimers[lineId] = 'CANCELLED'
  
  // 3. Limpiar presence
  if (this.presenceIntervals[lineId]) {
    clearInterval(this.presenceIntervals[lineId])
    delete this.presenceIntervals[lineId]
  }
  
  // 4. Cerrar socket (NO remover listeners, dejar que 'close' se dispare naturalmente)
  const client = this.clients.get(lineId)
  if (client) {
    try { client.ws?.close() } catch {}
    // NO hacer client.ev?.removeAllListeners()
    // NO hacer this.clients.delete(lineId) aquí
  }
  
  // 5. NO actualizar DB aquí — el handler de 'close' lo hace
  // 6. NO notificar aquí — el handler de 'close' lo hace con reason: CANCELLED_BY_USER
}
}



module.exports = { WAService }