const nodeFetch = globalThis.fetch ?? require('node-fetch')
require('dotenv').config()
const express = require('express')
const http = require('http')
const { Server } = require('socket.io')
const { PrismaClient } = require('@prisma/client')
const { WAService } = require('./whatsappService')
const jwt = require('jsonwebtoken')
const bcrypt = require('bcryptjs')
const cron = require('node-cron')

const PORT = process.env.PORT || 8080
const SECRET = process.env.WHATSAPP_SECRET

let JWT_SECRET = process.env.JWT_SECRET

const prisma = new PrismaClient()
const app = express()
const server = http.createServer(app)

// CORS: permitir localhost en desarrollo + Vercel en producción
const cors = require('cors')
const allowedOrigins = [
  'http://localhost:3000',
  'http://127.0.0.1:3000',
  'https://mundial-blaster-web-three.vercel.app', 
  'https://wabisend.com']


app.use(cors({
  origin: function (origin, callback) {
    // Permitir requests sin origin (como Postman, curl, o server-to-server)
    if (!origin) return callback(null, true)
    if (allowedOrigins.includes(origin)) return callback(null, true)
    callback(new Error('Not allowed by CORS: ' + origin))
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'x-api-secret'],
}))
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
})

app.use(express.json({ limit: '50mb' }))
app.use(loadTier)
app.use(express.urlencoded({ limit: '50mb', extended: true }))

let tierCache = null
let tierCacheTime = 0
const TIER_CACHE_TTL = 60_000 // 1 minuto

async function loadTier(req, res, next) {
  try {
    const now = Date.now()
    if (tierCache && (now - tierCacheTime) < TIER_CACHE_TTL) {
      req.tier = tierCache.tier
      req.tierConfig = tierCache.config
      return next()
    }

    const tier = await getAppTier()
    tierCache = { tier, config: TIER_LIMITS[tier] || TIER_LIMITS.starter }
    tierCacheTime = now
    req.tier = tierCache.tier
    req.tierConfig = tierCache.config
    next()
  } catch (e) {
    console.error('loadTier error:', e)
    // Si falla, asumir starter para no bloquear requests
    req.tier = 'starter'
    req.tierConfig = TIER_LIMITS.starter
    next()
  }
}

// ============================================================
// 🔑 CLAVE PÚBLICA PARA LICENCIAS
// ============================================================
const LICENSE_PUBLIC_KEY = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArPfXmFNNzOR7bxsPpJ6V
BdovK3uX6+GVSPcKVZfSJypeJ4PfSWea7ZWi2GzMIaNmQE1yF5McGzbq38NDl1zA
Y9Odn9a9Yol0WRgpo2/C7mMqQlwVzt8yvgG6iWX04Kqw2/ZKESc495jec5AErzBc
kXXXxzGtfyUAzkHeg0Da3CtbPwtBC4TR1QwxT6FE08+yxbdqJzCtW+Sp8jGFmwdX
Zt2U3xmqghpABkD67W4EKAO4RAsHXKrBHCf49QEprQIf0r5csnhVzQ9ZNiM64NLI
HJIeR639aAKAyWeir0j4UUp9VAuEKnzMxz+ERtQ5PdDgVHKQnK/618Qxq7b7m3bc
JwIDAQAB
-----END PUBLIC KEY-----`

// ============================================================
// HELPERS
// ============================================================
function validateLicense(token) {
  try {
    const decoded = jwt.verify(token, LICENSE_PUBLIC_KEY, { algorithms: ['RS256'] })
    
    // Validar que el tier exista en nuestra config
    if (!decoded.tier || !TIER_LIMITS[decoded.tier]) {
      console.error('❌ Licencia con tier inválido:', decoded.tier)
      return null
    }
    
    // Validar issuer (opcional, para rechazar tokens de otra app)
    if (decoded.iss && decoded.iss !== 'wabisend-v1' && decoded.iss !== 'mundial-blaster-v1') {
      console.error('❌ Issuer inválido:', decoded.iss)
      return null
    }
    
    return decoded
  } catch (e) {
    console.error('❌ Error validando licencia:', e.message)
    return null
  }
}

function generateToken(payload) {
  const secret = JWT_SECRET
  if (!secret) throw new Error('JWT_SECRET no inicializada')
  return jwt.sign(payload, secret, { expiresIn: '8h' })
}

function verifyToken(token) {
  try {
    const secret = JWT_SECRET
    if (!secret) return null
    return jwt.verify(token, secret)
  } catch (e) {
    return null
  }
}

// let JWT_SECRET = process.env.JWT_SECRET

async function ensureJwtSecret() {
  // 1. Si viene por variable de entorno, usar esa
  if (JWT_SECRET) return JWT_SECRET

  // 2. Buscar en app_config (generado en ejecuciones anteriores)
  const config = await prisma.app_config.findUnique({ where: { key: 'jwt_secret' } })
  if (config?.value) {
    JWT_SECRET = config.value
    return JWT_SECRET
  }

  // 3. Generar una nueva y guardarla
  const crypto = require('crypto')
  const newSecret = crypto.randomBytes(64).toString('hex')
  
  await prisma.app_config.create({
    data: { key: 'jwt_secret', value: newSecret }
  })
  
  JWT_SECRET = newSecret
  console.log('🔐 JWT_SECRET auto-generada y guardada en DB')
  return JWT_SECRET
}

// Llamar al iniciar el servidor
ensureJwtSecret().catch(e => {
  console.error('❌ Error inicializando JWT_SECRET:', e)
})

function resolveSpintax(text) {
  if (!text) return text
  // Solo reemplaza {{a|b|c}} (tiene pipe). Deja {{nombre}} intacto para variables.
  return text.replace(/\{\{([^}]+)\}\}/g, (match, content) => {
    if (!content.includes('|')) return match
    const variants = content.split("|").map(s => s.trim()).filter(Boolean)
    return variants.length ? variants[Math.floor(Math.random() * variants.length)] : ''
  })
}


// ============================================================
// TIER LIMITS
// ============================================================
const TIER_LIMITS = {
  starter: {
    label: 'Starter',
    maxLines: 2,
    maxTemplates: 5,
    maxPendingCampaigns: 10,
    hasBlacklist: false,
    hasAdvancedReports: false,
    hasCron: false,
    hasExport: false,
    hasRoundRobin: false,
    hasHumanMode: false,
    hasClone: false,
    hasAdvancedSpintax: false,
    hasBlacklist: false,
    hasTemplateVars: false,
    hasAI: false,
    hasMultiUser: false,
  },
  pro: {
    label: 'Pro',
    maxLines: 5,
    maxTemplates: Infinity,
    maxPendingCampaigns: 50,
    hasBlacklist: true,
    hasAdvancedReports: true,
    hasCron: true,
    hasExport: true,
    hasRoundRobin: true,
    hasHumanMode: true,
    hasClone: true,
    hasAdvancedSpintax: true,
    hasTemplateVars: true,
    hasBlacklist: false,
    hasAI: false,
    hasMultiUser: false,
  },
  business: {
    label: 'Business',
    maxLines: Infinity,
    maxTemplates: Infinity,
    maxPendingCampaigns: Infinity,
    hasBlacklist: true,
    hasAdvancedReports: true,
    hasCron: true,
    hasExport: true,
    hasRoundRobin: true,
    hasHumanMode: true,
    hasClone: true,
    hasAdvancedSpintax: true,
    hasTemplateVars: true,
    hasAI: true,
    hasMultiUser: true,
  },
}

async function getAppTier() {
  try {
    const config = await prisma.app_config.findUnique({ where: { key: 'license' } })
    if (!config?.value) return 'starter'
    const license = validateLicense(config.value)
    return license?.tier || 'starter'
  } catch {
    return 'starter'
  }
}

async function loadTier(req, res, next) {
  const tier = await getAppTier()
  req.tier = tier
  req.tierConfig = TIER_LIMITS[tier] || TIER_LIMITS.starter
  next()
}

function requireFeature(feature) {
  return (req, res, next) => {
    if (!req.tierConfig?.[feature]) {
      return res.status(403).json({
        error: `Esta función requiere un plan superior. Tu plan actual: ${req.tier}.`,
        tier: req.tier,
      })
    }
    next()
  }
}

// ============================================================
// MIDDLEWARES
// ============================================================

// Middleware original: acepta SECRET por header o body
const authMiddleware = (req, res, next) => {
  const headerSecret = req.headers['x-api-secret']
  const bodySecret = req.body?.secret
  if (headerSecret === SECRET || bodySecret === SECRET) return next()
  return res.status(401).json({ error: 'Unauthorized' })
}

// Middleware: solo JWT válido
async function requireAuth(req, res, next) {
  const token = req.headers.authorization?.replace('Bearer ', '')
  if (!token) return res.status(401).json({ error: 'No autenticado' })

  const decoded = verifyToken(token)
  if (!decoded) return res.status(401).json({ error: 'Sesión expirada o inválida' })

  const user = await prisma.usuarios.findUnique({ where: { id: decoded.userId } })
  if (!user) return res.status(401).json({ error: 'Usuario no encontrado' })

  req.user = user
  next()
}

// Middleware: SECRET o JWT (para compatibilidad líneas/campañas)
async function authOrSecret(req, res, next) {
  const headerSecret = req.headers['x-api-secret']
  const bodySecret = req.body?.secret

  if (headerSecret === SECRET || bodySecret === SECRET) {
    return next()
  }

  const token = req.headers.authorization?.replace('Bearer ', '')
  if (!token) return res.status(401).json({ error: 'Unauthorized' })

  const decoded = verifyToken(token)
  if (!decoded) return res.status(401).json({ error: 'Sesión expirada' })

  const user = await prisma.usuarios.findUnique({ where: { id: decoded.userId } })
  if (!user) return res.status(401).json({ error: 'Usuario no encontrado' })

  req.user = user
  next()
}

// Middleware: licencia activa requerida
async function requireLicense(req, res, next) {
  try {
    const config = await prisma.app_config.findUnique({ where: { key: 'license' } })
    if (!config?.value) {
      return res.status(403).json({ error: 'Licencia no activada. Andá a /setup' })
    }
    const license = validateLicense(config.value)
    if (!license) {
      return res.status(403).json({ error: 'Licencia inválida o expirada' })
    }
    req.license = license
    next()
  } catch (e) {
    res.status(500).json({ error: 'Error validando licencia' })
  }
}

// ============================================================
// WHATSAPP SERVICE
// ============================================================
const waService = new WAService(prisma, io)
waService.init()

io.on('connection', () => console.log('🟢 Socket conectado'))

// ============================================================
// RUTAS
// ============================================================

app.get('/', (_, res) => res.json({ status: 'OK', service: 'Mundial Blaster', version: '2.0.0' }))

// ========== AUTH (TODO PROTEGIDO POR LICENCIA) ==========

// Registro (onboarding)
app.post('/api/auth/register', requireLicense, async (req, res) => {
  try {
    // 🔒 BLINDAJE: Solo un admin por instancia
    const existingCount = await prisma.usuarios.count()
    if (existingCount > 0) {
      return res.status(403).json({ 
        error: 'Ya existe un administrador en esta instancia. Usá recuperación de contraseña o contactá soporte.' 
      })
    }

    const { 
      nombre, email, password, confirmPassword, avatar,
      security_question, security_answer,
      company_name, phone, timezone, language, industry, expected_volume,
      line_phone, line_name, recovery_code, affiliate_code
    } = req.body

    if (!nombre || !email || !password || !security_question || !security_answer) {
      return res.status(400).json({ error: 'Faltan campos obligatorios' })
    }
    if (password !== confirmPassword) {
      return res.status(400).json({ error: 'Las contraseñas no coinciden' })
    }
    if (password.length < 6) {
      return res.status(400).json({ error: 'Mínimo 6 caracteres' })
    }

    const hashedPassword = await bcrypt.hash(password, 10)
    const hashedAnswer = await bcrypt.hash(security_answer.toLowerCase().trim(), 10)
    
    // Hash del código de recuperación (si viene)
    let hashedRecovery = null
    if (recovery_code) {
      hashedRecovery = await bcrypt.hash(recovery_code.toUpperCase().trim(), 10)
    }

    const newUser = await prisma.usuarios.create({
      data: {
        nombre,
        email: email.toLowerCase().trim(),
        password: hashedPassword,
        avatar: avatar || 'Felix',
        security_question,
        security_answer: hashedAnswer,
        company_name,
        phone,
        timezone: timezone || 'America/Argentina/Buenos_Aires',
        language: language || 'es',
        industry,
        expected_volume,
        onboarding_completed: true,
        recovery_code: hashedRecovery,
        affiliate_code: affiliate_code || null
      }
    })

    // 📱 CREAR LÍNEA WHATSAPP si viene en el onboarding
    if (line_phone && line_name) {
      try {
        await prisma.lineas_whatsapp.create({
          data: {
            phone: line_phone.trim(),
            nombre: line_name.trim(),
            status: 'DESCONECTADA',
            owner_id: newUser.id,
          }
        })
        console.log('📱 Línea WhatsApp creada:', line_phone)
      } catch (lineError) {
        console.error('⚠️ Error creando línea (no crítico):', lineError.message)
        // No falla el registro si la línea falla
      }
    }

    // 🔥 RECLAIM: Si hay datos huérfanos (owner_id = null), asignarlos al nuevo admin
    await prisma.$transaction([
      prisma.campaigns.updateMany({ where: { owner_id: null }, data: { owner_id: newUser.id } }),
      prisma.lineas_whatsapp.updateMany({ where: { owner_id: null }, data: { owner_id: newUser.id } }),
      prisma.contacts.updateMany({ where: { owner_id: null }, data: { owner_id: newUser.id } }),
      prisma.tags.updateMany({ where: { owner_id: null }, data: { owner_id: newUser.id } }),
      prisma.message_templates.updateMany({ where: { owner_id: null }, data: { owner_id: newUser.id } }),
      prisma.campaign_logs.updateMany({ where: { owner_id: null }, data: { owner_id: newUser.id } }),
      prisma.scheduled_campaigns.updateMany({ where: { owner_id: null }, data: { owner_id: newUser.id } }),
      prisma.campaign_attachments.updateMany({ where: { owner_id: null }, data: { owner_id: newUser.id } }),
      prisma.blacklist.updateMany({ where: { owner_id: null }, data: { owner_id: newUser.id } }),
    ])

    // Guardar admin_email en app_config para referencia
    await prisma.app_config.upsert({
      where: { key: 'admin_email' },
      update: { value: email.toLowerCase().trim() },
      create: { key: 'admin_email', value: email.toLowerCase().trim() }
    })

    const secret = JWT_SECRET || await ensureJwtSecret()
    if (!secret) {
      return res.status(500).json({ error: 'Error interno: JWT no inicializado' })
    }

    const token = jwt.sign(
      { userId: newUser.id, email: newUser.email, role: newUser.role },
      secret,
      { expiresIn: '8h' }
    )

    const { password: pwd, security_answer: sa, recovery_code: rc, ...safeUser } = newUser
    res.json({ success: true, token, user: safeUser })

  } catch (e) {
    console.error('Register error:', e)
    if (e.code === 'P2002') {
      return res.status(409).json({ error: 'Ese email ya está registrado' })
    }
    res.status(500).json({ error: 'Error creando cuenta' })
  }
})

// Login
app.post('/api/auth/login', requireLicense, async (req, res) => {
  const { email, password } = req.body
  if (!email || !password) {
    return res.status(400).json({ error: 'Email y contraseña requeridos' })
  }

  try {
    const user = await prisma.usuarios.findUnique({
      where: { email: email.toLowerCase().trim() }
    })

    if (!user) {
      return res.status(401).json({ error: 'Email o contraseña incorrectos' })
    }

    const valid = await bcrypt.compare(password, user.password)
    if (!valid) {
      return res.status(401).json({ error: 'Email o contraseña incorrectos' })
    }

    await prisma.usuarios.update({
      where: { id: user.id },
      data: { last_login: new Date() }
    })

        const token = generateToken({
      userId: user.id,
      email: user.email,
      role: user.role
    })

    const { password: _, security_answer: __, ...safeUser } = user

    res.json({
      success: true,
      token,
      user: safeUser,
      expiresIn: '8h'
    })

  } catch (e) {
    console.error('Error login:', e)
    res.status(500).json({ error: 'Error en login' })
  }
})



// Recuperar contraseña por pregunta de seguridad
app.post('/api/auth/recover', requireLicense, async (req, res) => {
  const { email, new_password, security_answer, recovery_code } = req.body

  if (!email || !new_password) {
    return res.status(400).json({ error: 'Email y nueva contraseña requeridos' })
  }
  if (new_password.length < 6) {
    return res.status(400).json({ error: 'Mínimo 6 caracteres' })
  }

  try {
    const user = await prisma.usuarios.findUnique({
      where: { email: email.toLowerCase().trim() }
    })

    if (!user) {
      return res.status(404).json({ error: 'Usuario no encontrado' })
    }

    let valid = false

    // Método 1: Pregunta de seguridad
    if (security_answer && user.security_answer) {
      valid = await bcrypt.compare(security_answer.toLowerCase().trim(), user.security_answer)
    }

    // Método 2: Código de recuperación
    if (!valid && recovery_code && user.recovery_code) {
      valid = await bcrypt.compare(recovery_code.toUpperCase().trim(), user.recovery_code)
    }

    if (!valid) {
      return res.status(401).json({ error: 'Credenciales de recuperación incorrectas' })
    }

    const hashedPassword = await bcrypt.hash(new_password, 10)
    await prisma.usuarios.update({
      where: { id: user.id },
      data: { password: hashedPassword }
    })

    res.json({ success: true, message: 'Contraseña actualizada. Iniciá sesión.' })

  } catch (e) {
    console.error('Error recover:', e)
    res.status(500).json({ error: 'Error recuperando cuenta' })
  }
})



// Get usuario actual
app.get('/api/auth/me', requireLicense, requireAuth, async (req, res) => {
  const { password, security_answer, recovery_code, reset_token, ...safeUser } = req.user
  res.json({ user: safeUser })
})

// Update perfil (nombre, email, avatar, password)
  app.patch('/api/auth/me', requireLicense, requireAuth, async (req, res) => {
  const { 
    nombre, email, avatar, 
    company_name, phone, timezone, language, industry, expected_volume,
    current_password, new_password 
  } = req.body

  try {
    const updateData = {}

    // Campos de perfil nuevos
    if (nombre !== undefined) updateData.nombre = nombre
    if (avatar !== undefined) updateData.avatar = avatar
    if (company_name !== undefined) updateData.company_name = company_name
    if (phone !== undefined) updateData.phone = phone
    if (timezone !== undefined) updateData.timezone = timezone
    if (language !== undefined) updateData.language = language
    if (industry !== undefined) updateData.industry = industry
    if (expected_volume !== undefined) updateData.expected_volume = expected_volume
    if (req.body.affiliate_code !== undefined) updateData.affiliate_code = req.body.affiliate_code

    // Cambio de email: requiere password actual
    if (email && email !== req.user.email) {
      if (!current_password) {
        return res.status(400).json({ error: 'Se requiere contraseña actual para cambiar el email' })
      }
      const valid = await bcrypt.compare(current_password, req.user.password)
      if (!valid) return res.status(401).json({ error: 'Contraseña actual incorrecta' })

      const existing = await prisma.usuarios.findUnique({ where: { email } })
      if (existing) return res.status(409).json({ error: 'Ese email ya está en uso' })

      updateData.email = email.toLowerCase().trim()
    }

    // Cambio de password
    if (new_password) {
      if (!current_password) {
        return res.status(400).json({ error: 'Se requiere contraseña actual' })
      }
      const valid = await bcrypt.compare(current_password, req.user.password)
      if (!valid) return res.status(401).json({ error: 'Contraseña actual incorrecta' })
      if (new_password.length < 6) {
        return res.status(400).json({ error: 'Mínimo 6 caracteres' })
      }
      updateData.password = await bcrypt.hash(new_password, 10)
    }

    const updated = await prisma.usuarios.update({
      where: { id: req.user.id },
      data: updateData
    })

    const { password: pwd, security_answer: sa, recovery_code: rc, reset_token: rt, ...safeUser } = updated

    res.json({ success: true, user: safeUser })

  } catch (e) {
    console.error('Error update:', e)
    res.status(500).json({ error: 'Error actualizando perfil' })
  }
})

// Logout (client-side, pero dejamos endpoint por consistencia)
app.post('/api/auth/logout', requireLicense, requireAuth, async (req, res) => {
  res.json({ success: true, message: 'Sesión cerrada' })
})

// ========== LEGACY USER (compatibilidad onboarding viejo) ==========
app.get('/api/user', authOrSecret, async (req, res) => {
  try {
    const user = await prisma.usuarios.findFirst()
    res.json({ user })
  } catch (e) {
    res.status(500).json({ error: 'Error obteniendo usuario' })
  }
})

app.post('/api/user', authOrSecret, async (req, res) => {
  const { nombre, email, avatar } = req.body
  if (!nombre || !email) {
    return res.status(400).json({ error: 'Nombre y email son requeridos' })
  }
  try {
    const existing = await prisma.usuarios.findFirst()
    if (existing) {
      const updated = await prisma.usuarios.update({
        where: { id: existing.id },
        data: { nombre, email, avatar }
      })
      return res.json({ success: true, user: updated })
    }
    const user = await prisma.usuarios.create({
      data: { nombre, email, avatar }
    })
    res.json({ success: true, user })
  } catch (e) {
    console.error('Error creando usuario:', e)
    res.status(500).json({ error: 'Error creando usuario' })
  }
})

app.get('/api/auth/check', async (req, res) => {
  try {
    const count = await prisma.usuarios.count()
    res.json({ hasUser: count > 0 })
  } catch (e) {
    res.status(500).json({ error: 'Error verificando usuarios' })
  }
})



// ========== CONTACTOS ==========

app.get('/api/contacts', authOrSecret, async (req, res) => {
  try {
    const { search, tag } = req.query
    const where = {}

    // Traer propios + huérfanos
    if (req.user?.id) {
      where.OR = [
        { owner_id: req.user.id },
        { owner_id: null }
      ]
    }

    if (search) {
      where.AND = where.AND || []
      where.AND.push({
        OR: [
          { name: { contains: search, mode: 'insensitive' } },
          { phone: { contains: search } },
          { email: { contains: search, mode: 'insensitive' } },
        ]
      })
    }
    if (tag) {
      where.tags = { has: tag }
    }

    const [contacts, blacklistRows] = await Promise.all([
      prisma.contacts.findMany({ where, orderBy: { createdAt: 'desc' } }),
      prisma.blacklist.findMany({ 
        select: { phone: true } 
      })
    ])

    // Cruzar con toda la blacklist (sin filtrar por owner)
    const blacklistedPhones = new Set(
      blacklistRows.map(b => String(b.phone).replace(/\D/g, ''))
    )

    const enriched = contacts.map(c => ({
      ...c,
      isBlacklisted: blacklistedPhones.has(String(c.phone).replace(/\D/g, ''))
    }))

    res.json({ contacts: enriched })
  } catch (e) {
    console.error('Error listando contactos:', e)
    res.status(500).json({ error: 'Error listando contactos' })
  }
})

app.post('/api/contacts', authOrSecret, async (req, res) => {
  const { name, phone, email, company, tags, notes } = req.body
  if (!name || !phone) return res.status(400).json({ error: 'Nombre y teléfono requeridos' })
  
  try {
    const contact = await prisma.contacts.create({
      data: {
        name,
        phone: phone.replace(/\D/g, ''),
        email,
        company,
        tags: tags || [],
        notes,
        source: 'manual',
        owner_id: req.user?.id || null  // ← AGREGAR ESTO
      }
    })
    res.json({ success: true, contact })
  } catch (e) {
    if (e.code === 'P2002') return res.status(409).json({ error: 'Ese teléfono ya existe' })
    console.error('Error creando contacto:', e)
    res.status(500).json({ error: 'Error creando contacto' })
  }
})

app.patch('/api/contacts/:id', authOrSecret, async (req, res) => {
  const { id } = req.params
  const { name, phone, email, company, tags, notes } = req.body
  
  try {
    const contact = await prisma.contacts.update({
      where: { id },
      data: { name, phone, email, company, tags, notes }
    })
    res.json({ success: true, contact })
  } catch (e) {
    console.error('Error actualizando contacto:', e)
    res.status(500).json({ error: 'Error actualizando contacto' })
  }
})

app.delete('/api/contacts', authOrSecret, async (req, res) => {
  const { ids } = req.body
  if (!Array.isArray(ids) || ids.length === 0) {
    return res.status(400).json({ error: 'IDs requeridos' })
  }
  
  try {
    await prisma.contacts.deleteMany({ where: { id: { in: ids } } })
    res.json({ success: true, deleted: ids.length })
  } catch (e) {
    res.status(500).json({ error: 'Error eliminando contactos' })
  }
})

// Import CSV
app.post('/api/contacts/import', authOrSecret, async (req, res) => {
  const { contacts: importData } = req.body
  if (!Array.isArray(importData)) return res.status(400).json({ error: 'Array requerido' })
  
  let created = 0
  let errors = 0
  
  for (const item of importData) {
    try {
      await prisma.contacts.create({
        data: {
          name: item.name || 'Sin nombre',
          phone: item.phone.replace(/\D/g, ''),
          email: item.email,
          company: item.company,
          tags: item.tags || [],
          source: 'csv'
        }
      })
      created++
    } catch {
      errors++
    }
  }
  
  res.json({ success: true, created, errors })
})

// ========== TAGS ==========
// Contar contactos por cada tag
app.get('/api/tags/stats', authOrSecret, async (req, res) => {
  try {
    const tags = await prisma.tags.findMany({ orderBy: { name: 'asc' } })
    const contacts = await prisma.contacts.findMany({ select: { tags: true } })
    
    const stats = tags.map(tag => ({
      ...tag,
      count: contacts.filter(c => c.tags.includes(tag.name)).length
    }))
    
    res.json({ tags: stats })
  } catch (e) {
    res.status(500).json({ error: 'Error obteniendo stats' })
  }
})

app.get('/api/tags', authOrSecret, async (req, res) => {
  try {
    const tags = await prisma.tags.findMany({ orderBy: { name: 'asc' } })
    res.json({ tags })
  } catch (e) {
    res.status(500).json({ error: 'Error listando tags' })
  }
})

app.post('/api/tags', authOrSecret, async (req, res) => {
  const { name, color, icon } = req.body
  if (!name) return res.status(400).json({ error: 'Nombre requerido' })
  
  try {
    const tag = await prisma.tags.create({
      data: { name: name.toLowerCase().trim(), color: color || '#3B82F6', icon }
    })
    res.json({ success: true, tag })
  } catch (e) {
    if (e.code === 'P2002') return res.status(409).json({ error: 'Tag ya existe' })
    res.status(500).json({ error: 'Error creando tag' })
  }
})

app.delete('/api/tags/:id', authOrSecret, async (req, res) => {
  try {
    await prisma.tags.delete({ where: { id: req.params.id } })
    res.json({ success: true })
  } catch (e) {
    res.status(500).json({ error: 'Error eliminando tag' })
  }
})

// ========== REPORTES / CAMPAÑAS HISTÓRICO ==========

app.get('/api/campaigns/report', authOrSecret, loadTier, async (req, res) => {
  try {
    const { period } = req.query
    
    let dateFilter = {}
    
    // Starter: siempre 30 días, ignora el query param
    if (req.tier === 'starter') {
      dateFilter = { created_at: { gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) } }
    } else {
      // Pro y Business: respetan el period
      if (period === '7d') {
        dateFilter = { created_at: { gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) } }
      } else if (period === '30d') {
        dateFilter = { created_at: { gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) } }
      } else if (period === '90d') {
        dateFilter = { created_at: { gte: new Date(Date.now() - 90 * 24 * 60 * 60 * 1000) } }
      } else if (period === 'all') {
        dateFilter = {}
      } else {
        dateFilter = { created_at: { gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) } }
      }
    }
    
   
    
    
    const campaigns = await prisma.campaigns.findMany({
      where: dateFilter,
      orderBy: { created_at: 'desc' }
    })
    
    // Enriquecer con datos de programación
    const campaignsWithScheduled = await Promise.all(campaigns.map(async (c) => {
      const scheduled = await prisma.scheduled_campaigns.findUnique({
        where: { campaign_id: c.id }
      }).catch(() => null)
      return { ...c, scheduled }
    }))
    
    // Stats agregadas
    const totalSent = campaigns.reduce((sum, c) => sum + (c.sent || 0), 0)
    const totalFailed = campaigns.reduce((sum, c) => sum + (c.failed || 0), 0)
    const totalMessages = totalSent + totalFailed
    const deliveryRate = totalMessages > 0 ? Math.round((totalSent / totalMessages) * 100) : 0

    // Contactos únicos alcanzados (desde logs, no duplicados)
    const allLogs = await prisma.campaign_logs.findMany({
      where: {
        campaign_id: { in: campaigns.map(c => c.id) },
        status: 'sent'
      },
      select: { contact_phone: true }
    })
    const uniqueDelivered = new Set(allLogs.map(l => l.contact_phone)).size
    
    // ========== NUEVO: MÉTRICAS AVANZADAS ==========
    const campaignIds = campaigns.map(c => c.id)
    
    // Traer todos los logs de estas campañas (con los nuevos campos)
    const logsForMetrics = await prisma.campaign_logs.findMany({
      where: { campaign_id: { in: campaignIds } }
    })
    
    const deliveredLogs = logsForMetrics.filter(l => l.delivered_at)
    const readLogs = logsForMetrics.filter(l => l.read_at)
    const repliedLogs = logsForMetrics.filter(l => l.has_reply)
    
    // Tasa de apertura: read / delivered
    const openRate = deliveredLogs.length > 0 
      ? Math.round((readLogs.length / deliveredLogs.length) * 1000) / 10 
      : 0
    
    // Respuestas recibidas
    const repliesReceived = repliedLogs.length
    
    // Tiempo promedio de entrega (ms → segundos)
    const deliveryTimes = deliveredLogs
      .map(l => new Date(l.delivered_at).getTime() - new Date(l.created_at).getTime())
      .filter(t => t > 0)
    
    const avgDeliveryTime = deliveryTimes.length > 0
      ? Math.round((deliveryTimes.reduce((a, b) => a + b, 0) / deliveryTimes.length) / 1000)
      : 0
    
    // Blacklist (si tenés tabla, reemplazar; si no, queda en 0)
    let blacklistCount = 0
    try {
      blacklistCount = await prisma.blacklist?.count?.() || 0
    } catch {
      blacklistCount = 0
    }
    
    // Campañas programadas vs pendientes
    const scheduledCount = campaigns.filter(c => c.status === 'pending' && c.scheduled).length
    const pendingCount = campaigns.filter(c => c.status === 'pending' && !c.scheduled).length
    // ================================================
    
    // Datos para gráfico por día
    const dailyData = {}
    campaigns.forEach(c => {
      const date = c.created_at.toISOString().split('T')[0]
      if (!dailyData[date]) dailyData[date] = { sent: 0, failed: 0 }
      dailyData[date].sent += c.sent || 0
      dailyData[date].failed += c.failed || 0
    })
    
    const chartData = Object.entries(dailyData)
      .map(([date, data]) => ({
        date,
        sent: data.sent,
        failed: data.failed,
        total: data.sent + data.failed
      }))
      .sort((a, b) => a.date.localeCompare(b.date))
    
    res.json({
      campaigns: campaignsWithScheduled,
      stats: {
        totalCampaigns: campaigns.length,
        totalSent,
        totalFailed,
        totalMessages,
        deliveryRate,
        activeNow: campaigns.filter(c => c.status === 'running').length,
        uniqueDelivered,
        // NUEVAS MÉTRICAS
        openRate,
        repliesReceived,
        blacklistCount,
        avgDeliveryTime,
        scheduledCount,
        pendingCount,
      },
      chartData
    })
    
  } catch (e) {
    console.error('Error reportes:', e)
    res.status(500).json({ error: 'Error generando reportes' })
  }
})

app.post('/api/campaigns/:id/cancel', authOrSecret, async (req, res) => {
  try {
    const campaign = await prisma.campaigns.findUnique({ where: { id: req.params.id } })
    if (!campaign) return res.status(404).json({ error: 'Campaña no encontrada' })
    if (campaign.status !== 'running') return res.status(400).json({ error: 'No está en ejecución' })

    await prisma.campaigns.update({
      where: { id: req.params.id },
      data: { status: 'cancelled' }
    })
    
    res.json({ success: true, status: 'cancelled' })
  } catch (e) {
    console.error('Error cancelando:', e)
    res.status(500).json({ error: 'Error cancelando campaña' })
  }
})

// Endpoint para detalle de campaña (logs individuales)
app.get('/api/campaigns/:id/logs', authOrSecret, requireLicense, async (req, res) => {

  try {
    const logs = await prisma.campaign_logs.findMany({
      where: { campaign_id: req.params.id },
      orderBy: { created_at: 'desc' }
    })
    res.json({ logs })
  } catch (e) {
    res.status(500).json({ error: 'Error obteniendo logs' })
  }
})

// Repetir campaña (Pro)
app.post('/api/campaigns/:id/clone', authOrSecret, loadTier, requireFeature('hasClone'), async (req, res) => {
  try {
    const original = await prisma.campaigns.findUnique({ where: { id: req.params.id } })
    if (!original) return res.status(404).json({ error: 'Campaña no encontrada' })

 

    const newId = `camp_${Date.now()}`
        await prisma.campaigns.create({
      data: {
        id: newId,
        name: `${original.name} (copia)`,
        line_id: original.line_id,
        message: original.message,
        image_url: original.image_url,
        total: original.total,
        sent: 0,
        failed: 0,
        status: 'pending',
        targets: original.targets,
        distribution_mode: original.distribution_mode || 'single',
        selected_lines: original.selected_lines,
        human_mode: original.human_mode || false  
      }
    })

    res.json({ 
      success: true, 
      campaign: {
        id: newId,
        name: `${original.name} (copia)`,
        line_id: original.line_id,
        message: original.message,
        image_url: original.image_url,
        targets: original.targets,
        total: original.total
      }
    })
  } catch (e) {
    console.error('Error clone:', e)
    res.status(500).json({ error: 'Error clonando campaña' })
  }
})



app.delete('/api/campaigns/:id', authOrSecret, requireLicense, async (req, res) => {
  try {
    const { id } = req.params
    const userId = req.user?.id || req.userId

    const campaign = await prisma.campaigns.findFirst({
      where: { id, user_id: userId }
    })

    if (!campaign) return res.status(404).json({ error: 'Campaña no encontrada' })
    if (campaign.status === 'running') return res.status(400).json({ error: 'No se puede eliminar una campaña en ejecución. Parala primero.' })

    // Transaction atómica: logs primero (evita FK huérfanos), luego campaña
    await prisma.$transaction([
      prisma.campaign_logs.deleteMany({ where: { campaign_id: id } }),
      prisma.campaigns.delete({ where: { id } })
    ])

    res.json({ success: true })
  } catch (e) {
    console.error('Error delete campaign:', e)
    res.status(500).json({ error: 'Error eliminando campaña' })
  }
})



app.get('/api/campaigns/:id/export', authOrSecret, loadTier, requireFeature('hasExport'), async (req, res) => {
  try {
    const { id } = req.params

    const campaign = await prisma.campaigns.findUnique({ where: { id } })
    if (!campaign) return res.status(404).json({ error: 'Campaña no encontrada' })

    // Traer TODOS los logs (sent + failed) para que el usuario vea el panorama completo
    const logs = await prisma.campaign_logs.findMany({
      where: { campaign_id: id },
      orderBy: { created_at: 'desc' }
    })

    // Buscar nombres en contactos si existen
    const phones = logs.map(l => l.contact_phone)
    const contacts = await prisma.contacts.findMany({
      where: { phone: { in: phones } },
      select: { phone: true, name: true }
    }).catch(() => [])
    const contactMap = {}
    contacts.forEach(c => contactMap[c.phone] = c.name)

    // Construir CSV con headers enriquecidos
    const rows = logs.map(log => {
      const phone = log.contact_phone?.replace(/\D/g, '') || ''
      const name = (contactMap[log.contact_phone] || '').replace(/"/g, '""')
      const msg = (campaign.message || '').replace(/"/g, '""')
      const date = log.created_at ? new Date(log.created_at).toLocaleString('es-AR') : ''
      const status = log.status
      const line = log.line_id || ''
      return `"${phone}","${name}","${msg}","${date}","${status}","${line}"`
    })

    const csv = 'telefono,nombre,mensaje,fecha_envio,estado,linea\n' + rows.join('\n')

    res.setHeader('Content-Type', 'text/csv; charset=utf-8')
    res.setHeader('Content-Disposition', `attachment; filename="campana_${id}_reporte.csv"`)
    res.send(csv)
  } catch (e) {
    console.error(e)
    res.status(500).json({ error: 'Error exportando' })
  }
})


// Obtener campaña completa para edición
app.get('/api/campaigns/:id', authOrSecret, async (req, res) => {
  try {
    const campaign = await prisma.campaigns.findUnique({
      where: { id: req.params.id }
    })
    if (!campaign) return res.status(404).json({ error: 'Campaña no encontrada' })
    res.json({ campaign })
  } catch (e) {
    console.error(e)
    res.status(500).json({ error: 'Error obteniendo campaña' })
  }
})

// Guardar edición de campaña pendiente/programada
app.patch('/api/campaigns/:id', authOrSecret, async (req, res) => {
  try {
    const { id } = req.params
    const { name, message, targets, image_url, line_ids, schedule, execute_at } = req.body

    const existing = await prisma.campaigns.findUnique({ where: { id } })
    if (!existing) return res.status(404).json({ error: 'No encontrada' })
    if (existing.status === 'running' || existing.status === 'completed') {
      return res.status(400).json({ error: 'Solo se pueden editar campañas pendientes o programadas' })
    }

    // Solo campos que existen en tu schema actual
    const updateData = {}
    if (name !== undefined) updateData.name = name
    if (message !== undefined) updateData.message = message
    if (targets !== undefined) {
      updateData.targets = targets
      updateData.total = targets.length
      updateData.sent = 0
      updateData.failed = 0
    }
    if (image_url !== undefined) updateData.image_url = image_url
    if (line_ids !== undefined) {
      updateData.selected_lines = JSON.stringify(line_ids) // ← tu campo legacy es String JSON
    }

    // Status según scheduling
    if (schedule === 'scheduled' && execute_at) {
      updateData.status = 'scheduled'
    } else if (schedule === 'pending') {
      updateData.status = 'pending'
    }

    const updated = await prisma.campaigns.update({
      where: { id },
      data: updateData
    })

    // Manejar programación via tabla scheduled_campaigns (que sí existe en tu schema)
    if (schedule === 'scheduled' && execute_at) {
      await prisma.scheduled_campaigns.upsert({
        where: { campaign_id: id },
        update: { execute_at: new Date(execute_at), status: 'pending' },
        create: { campaign_id: id, execute_at: new Date(execute_at), status: 'pending' }
      })
    } else if (schedule === 'pending') {
      // Si deja de ser scheduled, limpiar
      await prisma.scheduled_campaigns.deleteMany({ where: { campaign_id: id } }).catch(() => {})
    }

    res.json({ success: true, campaign: updated })
  } catch (e) {
    console.error('PATCH ERROR:', e)
    res.status(500).json({ error: e.message || 'Error actualizando campaña' })
  }
})


app.post('/api/campaigns/simulate', authOrSecret, async (req, res) => {
  try {
    const { targets, line_ids, mode = 'lite' } = req.body
    if (!targets?.length) return res.status(400).json({ error: 'Agregá números' })
    if (!line_ids?.length) return res.status(400).json({ error: 'Seleccioná al menos una línea' })

    // Validación de tier
    const license = await prisma.app_config.findUnique({ where: { key: 'license' } })
    const tier = license?.value ? JSON.parse(license.value)?.tier : 'starter'
    if (tier === 'starter') return res.status(403).json({ error: 'Simulacro requiere Pro o Business' })
    if (mode === 'full' && tier !== 'business') return res.status(403).json({ error: 'Simulacro Full requiere Business' })
    if (mode === 'lite' && targets.length > 1 && tier !== 'business') {
      return res.status(403).json({ error: 'Simulacro Lite: máximo 1 número en Pro' })
    }

    // Crear campaña simulated
    const campaign = await prisma.campaigns.create({
      data: {
        id: `sim_${Date.now()}`,
        name: `Simulacro ${new Date().toLocaleString('es-AR')}`,
        message: 'Modo simulacro: verificación de números',
        total: targets.length,
        sent: 0,
        failed: 0,
        status: 'simulated',
        targets,
        distribution_mode: line_ids.length > 1 ? 'round_robin' : 'single',
        selected_lines: JSON.stringify(line_ids),
        owner_id: req.user?.id || null
      }
    })

    // Crear logs ficticios de ping
    const logs = []
    for (const target of targets) {
      const lineId = line_ids[Math.floor(Math.random() * line_ids.length)]
      const latency = Math.floor(Math.random() * 80) + 20 // 20-100ms
      logs.push({
        campaign_id: campaign.id,
        contact_phone: target.phone,
        status: 'ping_ok',
        line_id: lineId,
        owner_id: req.user?.id || null,
        error: `Latencia: ${latency}ms`
      })
    }
    await prisma.campaign_logs.createMany({ data: logs })

    res.status(201).json({
      success: true,
      campaign: {
        id: campaign.id,
        name: campaign.name,
        status: 'simulated',
        total: targets.length,
        mode
      }
    })
  } catch (e) {
    console.error('Error simulacro:', e)
    res.status(500).json({ error: 'Error en simulacro' })
  }
})

// ========== TEMPLATES ==========

app.get('/api/templates', authOrSecret, async (req, res) => {
  try {
    const { search, category } = req.query
    const where = {}
    
    if (search) {
      where.OR = [
        { name: { contains: search, mode: 'insensitive' } },
        { content: { contains: search, mode: 'insensitive' } },
      ]
    }
    if (category && category !== 'all') {
      where.category = category
    }
    
    const templates = await prisma.message_templates.findMany({
      where,
      orderBy: { usageCount: 'desc' }
    })
    res.json({ templates })
  } catch (e) {
    console.error('Error templates:', e)
    res.status(500).json({ error: 'Error listando templates' })
  }
})

app.post('/api/templates', authOrSecret, loadTier, async (req, res) => {
  const { name, content, category } = req.body
  if (!name || !content) return res.status(400).json({ error: 'Nombre y contenido requeridos' })

  const currentTemplates = await prisma.message_templates.count()
  if (currentTemplates >= req.tierConfig.maxTemplates) {
    return res.status(403).json({
      error: `Límite alcanzado: tu plan ${req.tier} permite ${req.tierConfig.maxTemplates} template(s).`,
      current: currentTemplates,
      max: req.tierConfig.maxTemplates,
      tier: req.tier,
    })
  }
  
  // Extraer variables automáticamente: {{variable}}
  const variables = [...content.matchAll(/\{\{(\w+)\}\}/g)].map(m => m[1])
  const uniqueVars = [...new Set(variables)]
  
  try {
    const template = await prisma.message_templates.create({
      data: {
        name,
        content,
        category: category || 'General',
        variables: uniqueVars
      }
    })
    res.json({ success: true, template })
  } catch (e) {
    res.status(500).json({ error: 'Error creando template' })
  }
})

app.patch('/api/templates/:id', authOrSecret, async (req, res) => {
  const { id } = req.params
  const { name, content, category } = req.body
  
  const variables = content ? [...content.matchAll(/\{\{(\w+)\}\}/g)].map(m => m[1]) : undefined
  const uniqueVars = variables ? [...new Set(variables)] : undefined
  
  try {
    const updateData = {}
    if (name) updateData.name = name
    if (content) updateData.content = content
    if (category) updateData.category = category
    if (uniqueVars) updateData.variables = uniqueVars
    
    const template = await prisma.message_templates.update({
      where: { id },
      data: updateData
    })
    res.json({ success: true, template })
  } catch (e) {
    res.status(500).json({ error: 'Error actualizando template' })
  }
})

app.delete('/api/templates/:id', authOrSecret, async (req, res) => {
  try {
    await prisma.message_templates.delete({ where: { id: req.params.id } })
    res.json({ success: true })
  } catch (e) {
    res.status(500).json({ error: 'Error eliminando template' })
  }
})

// Clonar template
app.post('/api/templates/:id/clone', authOrSecret, async (req, res) => {
  try {
    const original = await prisma.message_templates.findUnique({
      where: { id: req.params.id }
    })
    if (!original) return res.status(404).json({ error: 'Template no encontrado' })
    
    const clone = await prisma.message_templates.create({
      data: {
        name: `${original.name} (copia)`,
        content: original.content,
        category: original.category,
        variables: original.variables,
        usageCount: 0
      }
    })
    res.json({ success: true, template: clone })
  } catch (e) {
    res.status(500).json({ error: 'Error clonando' })
  }
})

// Usar template (incrementa contador)
app.post('/api/templates/:id/use', authOrSecret, async (req, res) => {
  try {
    const template = await prisma.message_templates.update({
      where: { id: req.params.id },
      data: { usageCount: { increment: 1 } }
    })
    res.json({ success: true, template })
  } catch (e) {
    res.status(500).json({ error: 'Error usando template' })
  }
})

// Categorías únicas
app.get('/api/templates/categories', authOrSecret, async (req, res) => {
  try {
    const templates = await prisma.message_templates.findMany({
      select: { category: true },
      distinct: ['category']
    })
    res.json({ categories: templates.map(t => t.category) })
  } catch (e) {
    res.status(500).json({ error: 'Error' })
  }
})

// ========== LÍNEAS ==========
app.post('/api/lineas/connect', authOrSecret, requireLicense, async (req, res) => {
  const { phone } = req.body
  if (!phone) return res.status(400).json({ error: 'phone required' })
  try {
    const line = await waService.connect(phone)
    res.json({ message: 'QR generado. Escanea desde el panel.', lineId: line.id })
  } catch (err) {
    console.error(err)
    res.status(500).json({ error: 'Fallo al conectar' })
  }
})

app.get('/api/lineas', authOrSecret, requireLicense, async (req, res) => {
  try {
    const lines = await prisma.lineas_whatsapp.findMany({
      orderBy: { fecha_creacion: 'desc' }
    })
    res.json({ lines })
  } catch (err) {
    res.status(500).json({ error: 'Error listando líneas' })
  }
})

app.post('/api/lineas', authOrSecret, requireLicense, loadTier, async (req, res) => {
  const { phone, nombre } = req.body
  if (!phone) return res.status(400).json({ error: 'phone required' })

  try {
    const activeLines = await prisma.lineas_whatsapp.count()

    if (activeLines >= req.tierConfig.maxLines) {
      return res.status(403).json({
        error: `Límite alcanzado: tu plan ${req.tier} permite ${req.tierConfig.maxLines} línea(s).`,
        current: activeLines,
        max: req.tierConfig.maxLines,
        tier: req.tier,
      })
    }

    const existing = await prisma.lineas_whatsapp.findUnique({ where: { phone } })
    if (existing) return res.status(409).json({ error: 'Línea ya existe' })

    const line = await prisma.lineas_whatsapp.create({
      data: {
        phone,
        nombre: nombre || 'Nueva Línea',
        status: 'DESCONECTADA'
      }
    })
    res.json({ success: true, line })
  } catch (err) {
    console.error('Error creando línea:', err)
    res.status(500).json({ error: 'Error creando línea' })
  }
})

app.post('/api/lineas/logout', authOrSecret, requireLicense, async (req, res) => {
  const { lineId } = req.body
  if (!lineId) return res.status(400).json({ error: 'lineId required' })
  try {
    await waService.logout(lineId)
    res.json({ success: true })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// ========== CAMPAÑAS ==========
app.post('/api/campaigns/send', authOrSecret, requireLicense, loadTier, async (req, res) => {
  try {
    const body = req.body

    // ─── VALIDACIONES ───
    if (!body.message || !body.message.trim()) {
      return res.status(400).json({ error: 'El mensaje es obligatorio' })
    }
    if (!body.targets || !Array.isArray(body.targets) || body.targets.length === 0) {
      return res.status(400).json({ error: 'Debes seleccionar al menos un contacto' })
    }

    // ─── LÍNEAS ───
    const lineIds = body.line_ids || (body.line_id ? [body.line_id] : [])
    if (lineIds.length === 0) {
      return res.status(400).json({ error: 'Debes seleccionar al menos una línea' })
    }

    const lineasSeleccionadas = await prisma.lineas_whatsapp.findMany({
      where: { id: { in: lineIds }, status: 'CONECTADA' }
    })

    if (lineasSeleccionadas.length === 0) {
      return res.status(400).json({
        error: 'Las líneas seleccionadas no están conectadas. Conectalas antes de enviar.'
      })
    }

      // ─── MODO ───
    const distributionMode = body.distribution_mode || 'single'
    const isPending = body.schedule === 'pending'
    const isScheduled = body.schedule === 'scheduled' && body.execute_at
    const isRoundRobin = body.distribution_mode === 'round_robin'

    // ─── VALIDACIONES TIER ───
    if (isPending) {
      const pendingCount = await prisma.campaigns.count({ where: { status: 'pending' } })
      if (pendingCount >= req.tierConfig.maxPendingCampaigns) {
        return res.status(403).json({
          error: `Límite alcanzado: tu plan ${req.tier} permite ${req.tierConfig.maxPendingCampaigns} campaña(s) pendiente(s).`,
          current: pendingCount,
          max: req.tierConfig.maxPendingCampaigns,
          tier: req.tier,
        })
      }
    }

    if (isScheduled && !req.tierConfig.hasCron) {
      return res.status(403).json({
        error: 'Programación de campañas disponible solo en plan Pro y Business.',
        tier: req.tier,
      })
    }

    if (isRoundRobin && !req.tierConfig.hasRoundRobin) {
      return res.status(403).json({
        error: 'Rotación Round-Robin disponible solo en plan Pro y Business.',
        tier: req.tier,
      })
    }

    if (body.human_mode && !req.tierConfig.hasHumanMode) {
      return res.status(403).json({
        error: 'Modo humano disponible solo en plan Pro y Business.',
        tier: req.tier,
      })
    }

    const newCampaign = await prisma.campaigns.create({
      data: {
        id: `camp_${Date.now()}`,
        name: body.name || `Campaña ${new Date().toLocaleDateString('es-AR')}`,
        message: body.message.trim(),
        image_url: body.image_url || null,
        total: body.targets.length,
        sent: 0,
        failed: 0,
        status: 'pending', // ← SIEMPRE pending, nunca scheduled
        targets: body.targets,
        distribution_mode: isRoundRobin ? 'round_robin' : 'single',
        line_id: lineasSeleccionadas[0]?.id || '',
        selected_lines: JSON.stringify(lineasSeleccionadas.map(l => l.id)),
        human_mode: body.human_mode === true  
      }
    })

   
    if (isPending) {
      return res.status(201).json({
        success: true,
        campaign: {
          id: newCampaign.id,
          name: newCampaign.name,
          status: 'pending',
          total: body.targets.length,
          distribution_mode: isRoundRobin ? 'round_robin' : 'single',
          lines: lineasSeleccionadas.map(l => ({ id: l.id, phone: l.phone, nombre: l.nombre }))
        }
      })
    }

    // ─── PROGRAMAR PARA FECHA/HORA ───
    if (isScheduled) {
      const executeAt = new Date(body.execute_at)
      const now = new Date()

      if (isNaN(executeAt.getTime())) {
        return res.status(400).json({ error: 'Fecha de programación inválida' })
      }
      if (executeAt <= now) {
        return res.status(400).json({ 
          error: 'La fecha de programación debe ser futura',
          received: body.execute_at,
          serverTime: now.toISOString()
        })
      }

      await prisma.scheduled_campaigns.create({
        data: {
          campaign_id: newCampaign.id,
          execute_at: executeAt,
          status: 'pending'
        }
      })

      return res.status(201).json({
        success: true,
        campaign: {
          id: newCampaign.id,
          name: newCampaign.name,
          status: 'pending',
          total: body.targets.length,
          distribution_mode: isRoundRobin ? 'round_robin' : 'single',
          lines: lineasSeleccionadas.map(l => ({ id: l.id, phone: l.phone, nombre: l.nombre }))
        },
        scheduledFor: body.execute_at
      })
    }

        const sendOptions = {
      delayMin: body.delay_min || 8000,
      delayMax: body.delay_max || 15000,
      imageUrl: body.image_url || null,
      humanMode: body.human_mode === true,
      skipBlacklist: body.skipBlacklist === true,
      ownerId: req.user?.id 
    }

    waService.sendCampaign(newCampaign.id, lineasSeleccionadas, body.targets, body.message.trim(), sendOptions)
      .then(() => console.log(`✅ Campaña ${newCampaign.id} finalizada`))
      .catch(err => console.error(`❌ Campaña ${newCampaign.id} falló:`, err))

    res.status(201).json({
      success: true,
      campaign: {
        id: newCampaign.id,
        name: newCampaign.name,
        status: 'running',
        total: body.targets.length,
        distribution_mode: isRoundRobin ? 'round_robin' : 'single',
        lines: lineasSeleccionadas.map(l => ({ id: l.id, phone: l.phone, nombre: l.nombre }))
      }
    })

  } catch (e) {
    console.error('💥 Error creando campaña:', e)
    res.status(500).json({ error: 'Error creando campaña', detail: e.message })
  }
})

app.get('/api/campaigns', authOrSecret, requireLicense, async (req, res) => {
  try {
    const campaigns = await prisma.campaigns.findMany({
      orderBy: { created_at: 'desc' }
    })
    
    const enriched = await Promise.all(campaigns.map(async (c) => {
      let lineData = []
      try {
        if (c.selected_lines) {
          const ids = JSON.parse(c.selected_lines)
          lineData = await prisma.lineas_whatsapp.findMany({
            where: { id: { in: ids } },
            select: { id: true, phone: true, nombre: true, status: true }
          })
        } else if (c.line_id) {
          const line = await prisma.lineas_whatsapp.findUnique({
            where: { id: c.line_id },
            select: { id: true, phone: true, nombre: true, status: true }
          })
          if (line) lineData = [line]
        }
       } catch (e) { /* silent fallback */ }
      
      const scheduled = await prisma.scheduled_campaigns.findUnique({
        where: { campaign_id: c.id }
      }).catch(() => null)
      
      return { ...c, lines: lineData, scheduled }
    }))
    
    res.json({ campaigns: enriched })
  } catch (e) {
    res.status(500).json({ error: 'Error listando campañas' })
  }
})

// ========== LICENCIA + ANTI-CLONACIÓN ==========

// Iniciar campaña pendiente
// Iniciar campaña pendiente
app.post('/api/campaigns/:id/start', authOrSecret, async (req, res) => {
  try {
    const campaign = await prisma.campaigns.findUnique({ where: { id: req.params.id } })
    if (!campaign) return res.status(404).json({ error: 'Campaña no encontrada' })
    if (campaign.status !== 'pending') return res.status(400).json({ error: 'No está en espera' })

    const targets = campaign.targets || []
    if (!targets.length) return res.status(400).json({ error: 'Sin destinatarios' })

    // ─── Resolver líneas (round-robin compatible) ───
    let lineIds = []
    try {
      if (campaign.selected_lines) lineIds = JSON.parse(campaign.selected_lines)
    } catch (e) {}
    if (lineIds.length === 0 && campaign.line_id) lineIds = [campaign.line_id]

    const lineasSeleccionadas = await prisma.lineas_whatsapp.findMany({
      where: { id: { in: lineIds }, status: 'CONECTADA' }
    })

    if (lineasSeleccionadas.length === 0) {
      return res.status(400).json({ error: 'Las líneas no están conectadas' })
    }

    // Marcar como running
    await prisma.campaigns.update({
      where: { id: campaign.id },
      data: { status: 'running' }
    }).catch(() => {})

    // Disparar fire & forget
        // Disparar fire & forget (delay mínimo 5s para no parecer spam)
         waService.sendCampaign(campaign.id, lineasSeleccionadas, targets, campaign.message, {
      delayMin: 5000,
      delayMax: 12000,
      imageUrl: campaign.image_url,
      humanMode: campaign.human_mode === true
    }).catch(console.error)

    res.json({ success: true, campaignId: campaign.id, status: 'running' })
  } catch (e) {
    console.error('Error start:', e)
    res.status(500).json({ error: 'Error iniciando campaña' })
  }
})

app.post('/api/setup/activate', async (req, res) => {
  try {
    const { licenseKey } = req.body
    if (!licenseKey) return res.status(400).json({ error: 'licenseKey required' })

    const license = validateLicense(licenseKey)
    if (!license) return res.status(400).json({ error: 'Licencia inválida' })

    // Anti-clonación: verificar dominio
    const instanceDomain = process.env.RAILWAY_STATIC_URL ||
                           process.env.VERCEL_URL ||
                           req.headers.host

    if (license.domain && license.domain !== instanceDomain) {
      return res.status(403).json({
        error: 'Licencia no válida para este dominio',
        licensedDomain: license.domain,
        currentDomain: instanceDomain
      })
    }

    await prisma.app_config.upsert({
      where: { key: 'license' },
      update: { value: licenseKey },
      create: { key: 'license', value: licenseKey }
    })

    await prisma.app_config.upsert({
      where: { key: 'instance_domain' },
      update: { value: instanceDomain },
      create: { key: 'instance_domain', value: instanceDomain }
    })

    res.json({ success: true, tier: license.tier, features: license })

  } catch (e) {
    console.error('Setup activate error:', e)
    res.status(500).json({ error: 'Error activando licencia' })
  }
})


app.get('/api/license/status', async (req, res) => {
  try {
    const origin = req.headers.origin || req.headers.referer || ''
    const host = req.headers.host || ''
    
    // Bypass localhost: solo si REALMENTE es localhost
    const isLocalhost = (
      origin.includes('localhost') || 
      origin.includes('127.0.0.1') ||
      host.includes('localhost') ||
      host.includes('127.0.0.1')
    ) && origin !== '' // ← ELIMINADO: origin === '' activaba el bypass por proxy

    if (isLocalhost) {
      return res.json({
        active: true,
        tier: 'business',
        maxLines: Infinity,
        label: 'Dev Local',
        features: Object.keys(TIER_LIMITS.business).filter(k => TIER_LIMITS.business[k] === true),
      })
    }

    // ─── LÓGICA REAL: leer de app_config ───
    const config = await prisma.app_config.findUnique({ where: { key: 'license' } })

    if (!config?.value) {
      return res.json({ 
        active: false, 
        reason: 'NO_LICENSE',
        tier: 'starter',
        maxLines: TIER_LIMITS.starter.maxLines,
        label: 'Starter (Sin licencia)'
      })
    }

    const license = validateLicense(config.value)
    if (!license) {
      return res.json({ 
        active: false, 
        reason: 'INVALID_LICENSE',
        tier: 'starter',
        maxLines: TIER_LIMITS.starter.maxLines,
        label: 'Starter (Licencia inválida)'
      })
    }

    const tier = license.tier
    if (!TIER_LIMITS[tier]) {
      return res.json({ 
        active: false, 
        reason: 'UNKNOWN_TIER',
        tier: 'starter',
        maxLines: TIER_LIMITS.starter.maxLines,
        label: 'Starter (Tier desconocido)'
      })
    }

    const tierConfig = TIER_LIMITS[tier]

    res.json({
      active: true,
      tier: tier,
      label: license.label || tierConfig.label || tier,
      maxLines: tierConfig.maxLines,
      maxTemplates: tierConfig.maxTemplates,
      maxPendingCampaigns: tierConfig.maxPendingCampaigns,
      features: Object.entries(tierConfig)
        .filter(([_, v]) => v === true)
        .map(([k]) => k),
      email: license.email,
      iat: license.iat,
      iss: license.iss,
    })

  } catch (e) {
    console.error('Error license/status:', e)
    res.status(500).json({ error: 'Error verificando licencia' })
  }
})

// ========== ADMIN (nuclear, solo con SECRET) ==========

// Resetear dominio vinculado
app.post('/api/admin/reset-domain', authMiddleware, async (req, res) => {
  const { newDomain } = req.body
  if (!newDomain) return res.status(400).json({ error: 'newDomain requerido' })

  await prisma.app_config.upsert({
    where: { key: 'instance_domain' },
    update: { value: newDomain },
    create: { key: 'instance_domain', value: newDomain }
  })

  res.json({ success: true, domain: newDomain })
})

// Resetear usuario completo (email/password)
app.post('/api/admin/reset-user', authMiddleware, async (req, res) => {
  const { email, new_email, new_password } = req.body
  if (!email) return res.status(400).json({ error: 'Email requerido' })

  try {
    const user = await prisma.usuarios.findUnique({ where: { email: email.toLowerCase().trim() } })
    if (!user) return res.status(404).json({ error: 'Usuario no encontrado' })

    const updateData = {}
    if (new_email) updateData.email = new_email.toLowerCase().trim()
    if (new_password) updateData.password = await bcrypt.hash(new_password, 10)

    await prisma.usuarios.update({ where: { id: user.id }, data: updateData })

    res.json({ success: true, message: 'Usuario reseteado' })
  } catch (e) {
    console.error('Admin reset error:', e)
    res.status(500).json({ error: 'Error reseteando usuario' })
  }
})


// ========== BLACKLIST ==========
// Solo Pro y Business

app.get('/api/blacklist', authOrSecret, loadTier, requireFeature('hasBlacklist'), async (req, res) => {
  try {
    const list = await prisma.blacklist.findMany({ orderBy: { createdAt: 'desc' } })
    res.json({ blacklist: list })
  } catch (e) {
    res.status(500).json({ error: 'Error listando blacklist' })
  }
})


// GET /api/blacklist/keywords
app.get('/api/blacklist/keywords', authOrSecret, async (req, res) => {
  try {
    const config = await prisma.app_config.findUnique({ where: { key: 'blacklist_keywords' } })
    const keywords = config?.value ? JSON.parse(config.value) : ['basta', 'no molesten', 'no me interesa', 'no, gracias', 'eliminar', 'stop', 'No quiero que me envien mas info', 'no quiero que me envien mas mensajes']
    res.json({ keywords })
  } catch (e) {
    res.status(500).json({ error: 'Error leyendo keywords' })
  }
})

// POST /api/blacklist/keywords
app.post('/api/blacklist/keywords', authOrSecret, loadTier, requireFeature('hasBlacklist'), async (req, res) => {
  try {
    const { keywords } = req.body // array de strings
    await prisma.app_config.upsert({
      where: { key: 'blacklist_keywords' },
      update: { value: JSON.stringify(keywords) },
      create: { key: 'blacklist_keywords', value: JSON.stringify(keywords) }
    })
    res.json({ success: true, keywords })
  } catch (e) {
    res.status(500).json({ error: 'Error guardando keywords' })
  }
})

app.post('/api/blacklist', authOrSecret, loadTier, requireFeature('hasBlacklist'), async (req, res) => {
  const { phone, reason } = req.body
  if (!phone) return res.status(400).json({ error: 'Teléfono requerido' })
  
  try {
    const entry = await prisma.blacklist.upsert({
      where: { phone: phone.replace(/\D/g, '') },
      update: { reason: reason || 'manual' },
      create: { phone: phone.replace(/\D/g, ''), reason: reason || 'manual' }
    })
    res.json({ success: true, entry })
  } catch (e) {
    res.status(500).json({ error: 'Error agregando a blacklist' })
  }
})

app.delete('/api/blacklist/:phone', authOrSecret, loadTier, requireFeature('hasBlacklist'), async (req, res) => {
  try {
    await prisma.blacklist.delete({ where: { phone: req.params.phone.replace(/\D/g, '') } })
    res.json({ success: true })
  } catch (e) {
    res.status(500).json({ error: 'Error eliminando de blacklist' })
  }
})


// === AI PROMPTS ===

app.get('/api/ai/prompts', authOrSecret, async (req, res) => {
  try {
    const where = req.user?.id ? { owner_id: req.user.id } : {}
    const prompts = await prisma.ai_prompts.findMany({
      where,
      orderBy: { createdAt: 'desc' }
    })
    res.json({ prompts })
  } catch (e) {
    console.error('Error listando AI prompts:', e)
    res.status(500).json({ error: 'Error listando prompts' })
  }
})

app.post('/api/ai/prompts', authOrSecret, async (req, res) => {
  try {
    const { title, instruction, results } = req.body
    if (!title || !instruction) {
      return res.status(400).json({ error: 'Título e instrucción requeridos' })
    }

    const prompt = await prisma.ai_prompts.create({
      data: {
        title: title.trim(),
        instruction: instruction.trim(),
        results: results || [],
        owner_id: req.user?.id || null
      }
    })
    res.json({ success: true, prompt })
  } catch (e) {
    console.error('Error guardando AI prompt:', e)
    res.status(500).json({ error: 'Error guardando prompt' })
  }
})

app.delete('/api/ai/prompts/:id', authOrSecret, async (req, res) => {
  try {
    const where = { id: req.params.id }
    if (req.user?.id) {
      where.owner_id = req.user.id
    }

    await prisma.ai_prompts.delete({ where })
    res.json({ success: true })
  } catch (e) {
    console.error('Error borrando AI prompt:', e)
    res.status(500).json({ error: 'Error borrando prompt' })
  }
})


// === OPENAI CONFIG ===

app.get('/api/openai/config', authOrSecret, async (req, res) => {
  try {
    const config = await prisma.openai_config.findFirst({
      where: req.user?.id ? { owner_id: req.user.id, active: true } : { active: true },
      orderBy: { updatedAt: 'desc' }
    })
    if (!config) return res.json({ hasKey: false })
    res.json({
      hasKey: true,
      model: config.model,
      // Nunca devolver la key completa al frontend
      keyPreview: `${config.api_key.slice(0, 8)}...${config.api_key.slice(-4)}`
    })
  } catch (e) {
    console.error('Error leyendo config OpenAI:', e)
    res.status(500).json({ error: 'Error leyendo configuración' })
  }
})

app.post('/api/openai/config', authOrSecret, async (req, res) => {
  try {
    const { apiKey, model = 'gpt-4o-mini' } = req.body
    if (!apiKey?.startsWith('sk-')) {
      return res.status(400).json({ error: 'API key inválida' })
    }

    // Validar key contra OpenAI antes de guardar
    const testRes = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: 'Say OK' }],
        max_tokens: 5
      })
    })
    if (!testRes.ok) throw new Error('API key inválida')

    // Upsert: desactivar anteriores y crear nueva
    if (req.user?.id) {
      await prisma.openai_config.updateMany({
        where: { owner_id: req.user.id },
        data: { active: false }
      })
    }

    const config = await prisma.openai_config.create({
      data: {
        owner_id: req.user?.id || null,
        api_key: apiKey,
        model,
        active: true
      }
    })

    res.json({
      success: true,
      model: config.model,
      keyPreview: `${apiKey.slice(0, 8)}...${apiKey.slice(-4)}`
    })
  } catch (e) {
    console.error('Error guardando config OpenAI:', e)
    res.status(500).json({ error: e.message || 'Error verificando/guardando key' })
  }
})

app.delete('/api/openai/config', authOrSecret, async (req, res) => {
  try {
    const where = req.user?.id ? { owner_id: req.user.id } : {}
    await prisma.openai_config.updateMany({
      where,
      data: { active: false }
    })
    res.json({ success: true })
  } catch (e) {
    res.status(500).json({ error: 'Error eliminando config' })
  }
})

// === AI GENERATE (proxy seguro) ===

app.post('/api/ai/generate', authOrSecret, async (req, res) => {
  try {
    const { instruction, temperature = 0.9, maxTokens = 600 } = req.body
    if (!instruction?.trim()) return res.status(400).json({ error: 'Instrucción requerida' })

    const config = await prisma.openai_config.findFirst({
      where: req.user?.id ? { owner_id: req.user.id, active: true } : { active: true },
      orderBy: { updatedAt: 'desc' }
    })
    if (!config) return res.status(403).json({ error: 'No hay API key configurada' })

    const openaiRes = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${config.api_key}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: config.model || 'gpt-4o-mini',
        messages: [
          {
            role: 'system',
            content: `Sos un copywriter experto en WhatsApp marketing. Generá 5 variantes de mensajes de venta MUY CORTOS (máximo 2 líneas cada uno). Usá la variable {nombre} para personalización. Incluí emojis naturales. Tono: directo, urgente, conversacional. NO uses spintax {{a|b}}. Respondé SOLO con los 5 mensajes, uno por línea, sin numerar, sin explicaciones.`
          },
          { role: 'user', content: instruction }
        ],
        temperature,
        max_tokens: maxTokens
      })
    })

    const data = await openaiRes.json()
    if (!openaiRes.ok) throw new Error(data.error?.message || 'Error de OpenAI')

    const text = data.choices?.[0]?.message?.content || ''
    const lines = text.split('\n').map(l => l.trim()).filter(Boolean)

    res.json({ success: true, lines, model: config.model })
  } catch (e) {
    console.error('Error generando IA:', e)
    res.status(500).json({ error: e.message || 'Error generando mensajes' })
  }
})

// Generar código de afiliado
app.post('/api/affiliate/generate', requireAuth, async (req, res) => {
  try {
    // Verificar si ya tiene código
    if (req.user.affiliate_code) {
      return res.status(409).json({ error: 'Ya tenés un código generado', code: req.user.affiliate_code })
    }

    const generateCode = () => {
      const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
      let result = ''
      for (let i = 0; i < 6; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length))
      }
      return `WS-${result}`
    }

    let code = generateCode()
    let attempts = 0
    // Asegurar unicidad
    while (await prisma.usuarios.findUnique({ where: { affiliate_code: code } }) && attempts < 10) {
      code = generateCode()
      attempts++
    }

    const updated = await prisma.usuarios.update({
      where: { id: req.user.id },
      data: { affiliate_code: code }
    })

    res.json({ success: true, code: updated.affiliate_code })
  } catch (e) {
    res.status(500).json({ error: 'Error generando código' })
  }
})


// server.js — endpoint leads/capture
const LEAD_SHEET_WEBHOOK_URL = "https://script.google.com/macros/s/AKfycby7QcHgrT6_LJZAv_Z4kw12Hn0Hsvg9r-cxBIucSN9In69KwlL1JdDTOjYJzjAxedFWTQ/exec"

app.post('/api/leads/capture', async (req, res) => {
  try {
    const { nombre, email, company_name, phone, industry, expected_volume, timezone, fecha, user_id } = req.body
    console.log('📥 Lead recibido:', { nombre, email, company_name })

    // 1. GUARDAR EN POSTGRESQL (siempre funciona)
    const lead = await prisma.leads.create({
      data: {
        nombre,
        email,
        company_name: company_name || null,
        phone: phone || null,
        industry: industry || null,
        expected_volume: expected_volume || null,
        timezone: timezone || null,
        user_id: user_id || null,
      }
    })
    console.log('✅ Lead guardado en DB:', lead.id)

    // 2. INTENTAR GOOGLE SHEET (opcional, no crítico)
    if (LEAD_SHEET_WEBHOOK_URL) {
      try {
        const params = new URLSearchParams()
        params.append("nombre", nombre || "")
        params.append("email", email || "")
        params.append("company_name", company_name || "")
        params.append("phone", phone || "")
        params.append("industry", industry || "")
        params.append("expected_volume", expected_volume || "")
        params.append("timezone", timezone || "")
        params.append("fecha", fecha || new Date().toISOString())

        // Usar POST en vez de GET
        const sheetRes = await fetch(LEAD_SHEET_WEBHOOK_URL, {
          method: 'POST',
          body: params,
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          redirect: 'follow',
        })

        const responseText = await sheetRes.text()
        console.log('📡 Sheet status:', sheetRes.status)
        
        // Si la respuesta empieza con <, es HTML (error)
        if (responseText.trim().startsWith('<')) {
          console.warn('⚠️ Sheet devolvió HTML (no JSON). URL incorrecta o no autorizado.')
        } else {
          console.log('✅ Sheet respondió:', responseText.substring(0, 100))
        }
      } catch (sheetError) {
        console.warn('⚠️ Sheet falló (no crítico):', sheetError.message)
      }
    }

    res.json({ success: true, lead_id: lead.id, sheet_attempted: !!LEAD_SHEET_WEBHOOK_URL })

  } catch (e) {
    console.error('❌ Lead capture error:', e)
    res.status(500).json({ error: 'Error guardando lead' })
  }
})


// CRON

cron.schedule('*/5 * * * *', async () => {
  try {
        const now = new Date()
        const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000)
    const pending = await prisma.scheduled_campaigns.findMany({
      where: {
        status: 'pending',
        execute_at: { 
          lte: now,
          gte: oneHourAgo // ← Solo las últimas 1 hora, no viejas eternas
        }
      },
      orderBy: { execute_at: 'asc' }
    })

    if (pending.length === 0) return

    console.log(`⏰ ${pending.length} campaña(s) programada(s) lista(s) para ejecutar`)

    for (const sched of pending) {
      const campaign = await prisma.campaigns.findUnique({
        where: { id: sched.campaign_id }
      })
      if (!campaign || campaign.status !== 'pending') continue

      // Marcar como processing para que otro tick no la agarre
      await prisma.scheduled_campaigns.update({
        where: { id: sched.id },
        data: { status: 'processing' }
      })

      // Resolver líneas
      let lineIds = []
      try {
        if (campaign.selected_lines) lineIds = JSON.parse(campaign.selected_lines)
      } catch (e) {}
      if (lineIds.length === 0 && campaign.line_id) lineIds = [campaign.line_id]

      const lineasSeleccionadas = await prisma.lineas_whatsapp.findMany({
        where: { id: { in: lineIds }, status: 'CONECTADA' }
      })

      if (lineasSeleccionadas.length === 0) {
        await prisma.scheduled_campaigns.update({
          where: { id: sched.id },
          data: { status: 'failed' }
        })
        continue
      }

      // Marcar running y disparar
      await prisma.campaigns.update({
        where: { id: campaign.id },
        data: { status: 'running' }
      }).catch(() => {})

      waService.sendCampaign(campaign.id, lineasSeleccionadas, campaign.targets, campaign.message, {
        delayMin: 5000,
        delayMax: 12000,
        imageUrl: campaign.image_url,
        humanMode: campaign.human_mode === true
      }).then(async () => {
        await prisma.scheduled_campaigns.update({
          where: { id: sched.id },
          data: { status: 'completed' }
        }).catch(() => {})
      }).catch(async (err) => {
        console.error('❌ Scheduled campaign failed:', err)
        await prisma.scheduled_campaigns.update({
          where: { id: sched.id },
          data: { status: 'failed' }
        }).catch(() => {})
      })
    }
  } catch (e) {
    console.error('⏰ Error cron scheduled campaigns:', e)
  }
})

// ============================================================
// SERVER START
// ============================================================
server.listen(PORT, () => console.log(`🚀 WabiSend v2.0.0 en puerto ${PORT}`))

process.on('uncaughtException', (err) => console.error('🔥 Uncaught:', err))
process.on('unhandledRejection', (reason) => console.error('🔥 Unhandled:', reason))