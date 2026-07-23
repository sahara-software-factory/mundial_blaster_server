

// 🛡️ Caché Global Inmortal
const globalMemoryCache = new Map();

// 🚂 Cola de Trabajo Secuencial para Neon
const writeQueue = [];
let isProcessing = false;

const processQueue = async (prisma) => {
    if (isProcessing) return;
    isProcessing = true;
    while (writeQueue.length > 0) {
        const task = writeQueue.shift();
        try {
            await task(prisma);
        } catch (e) {
            console.error('⚠️ Error guardando llave en Neon (reintentando en el próximo ciclo):', e.message);
        }
    }
    isProcessing = false;
};

async function usePrismaAuthState(prisma, sessionId, { initAuthCreds, BufferJSON, proto }) {
    if (!globalMemoryCache.has(sessionId)) {
        globalMemoryCache.set(sessionId, new Map());
    }
    const memoryCache = globalMemoryCache.get(sessionId);

    // 1. Restaurar de Neon a RAM si la RAM está vacía (Ej: Post-Redeploy)
    if (memoryCache.size === 0) {
        try {
            const allKeys = await prisma.wa_sessions.findMany({ where: { sessionId } });
            for (const row of allKeys) {
                memoryCache.set(row.key, row.data);
            }
            console.log(`⚡ AuthState restaurado de Neon a RAM: ${allKeys.length} llaves`);
        } catch (e) {
            console.error('Error precargando llaves:', e);
        }
    } else {
        console.log(`🛡️ AuthState rescatado de la RAM global: ${memoryCache.size} llaves`);
    }

    const writeData = (data, key) => {
        const dataString = JSON.stringify(data, BufferJSON.replacer);
        
        // A) Guardado inmediato en RAM (0ms para Baileys)
        memoryCache.set(key, dataString);

        // B) Encolamos para Neon (1 por 1, sin saturar)
        writeQueue.push((db) => db.wa_sessions.upsert({
            where: { sessionId_key: { sessionId, key } },
            update: { data: dataString },
            create: { sessionId, key, data: dataString }
        }));
        processQueue(prisma);
    };

    const readData = async (key) => {
        if (memoryCache.has(key)) {
            return JSON.parse(memoryCache.get(key), BufferJSON.reviver);
        }
        return null;
    };

    const removeData = async (key) => {
        memoryCache.delete(key);
        writeQueue.push((db) => db.wa_sessions.delete({
            where: { sessionId_key: { sessionId, key } }
        }).catch(()=>{}));
        processQueue(prisma);
    };

    let creds = await readData('creds');
    if (!creds) {
        creds = initAuthCreds();
        writeData(creds, 'creds');
    }

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    for (const id of ids) {
                        let value = await readData(`${type}-${id}`);
                        if (type === 'app-state-sync-key' && value) {
                            value = proto.Message.AppStateSyncKeyData.fromObject(value);
                        }
                        data[id] = value;
                    }
                    return data;
                },
                set: async (data) => {
                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
                            const key = `${category}-${id}`;
                            if (value) writeData(value, key);
                            else removeData(key);
                        }
                    }
                }
            }
        },
        saveCreds: async () => {
    // Guard anti-zombie: nunca pisar creds registradas con una copia vieja sin registrar
    if (creds.registered === false) {
        const current = await readData('creds')
        if (current?.registered === true) return
    }
    writeData(creds, 'creds')
}
    };
}

const clearRamCache = (sessionId) => {
    if (globalMemoryCache.has(String(sessionId))) {
        globalMemoryCache.delete(String(sessionId));
        console.log(`🧹 Caché RAM global eliminada para la sesión: ${sessionId}`);
    }
};

module.exports = { usePrismaAuthState, clearRamCache };