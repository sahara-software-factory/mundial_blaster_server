const { initAuthCreds, BufferJSON, proto } = require('@whiskeysockets/baileys');

async function usePrismaAuthState(prisma, sessionId) {
    const writeData = async (data, key) => {
        try {
            // BufferJSON es CRÍTICO para que Baileys no rompa los Uint8Arrays de encriptación
            const dataString = JSON.stringify(data, BufferJSON.replacer);
            await prisma.wa_sessions.upsert({
                where: { sessionId_key: { sessionId, key } },
                update: { data: dataString },
                create: { sessionId, key, data: dataString }
            });
        } catch (error) {
            console.error(`Error escribiendo llave ${key} en BD:`, error.message);
        }
    };

    const readData = async (key) => {
        try {
            const record = await prisma.wa_sessions.findUnique({
                where: { sessionId_key: { sessionId, key } }
            });
            return record ? JSON.parse(record.data, BufferJSON.reviver) : null;
        } catch (error) {
            console.error(`Error leyendo llave ${key} de BD:`, error.message);
            return null;
        }
    };

    const removeData = async (key) => {
        try {
            await prisma.wa_sessions.delete({
                where: { sessionId_key: { sessionId, key } }
            });
        } catch (error) {
            // Ignorar si no existe
        }
    };

    let creds = await readData('creds');
    if (!creds) {
        creds = initAuthCreds();
        await writeData(creds, 'creds');
    }

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data = {};
                    await Promise.all(
                        ids.map(async id => {
                            let value = await readData(`${type}-${id}`);
                            // Parche de seguridad para llaves de sincronización de WhatsApp
                            if (type === 'app-state-sync-key' && value) {
                                value = proto.Message.AppStateSyncKeyData.fromObject(value);
                            }
                            data[id] = value;
                        })
                    );
                    return data;
                },
                set: async (data) => {
                    const tasks = [];
                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
                            const key = `${category}-${id}`;
                            if (value) {
                                tasks.push(writeData(value, key));
                            } else {
                                tasks.push(removeData(key));
                            }
                        }
                    }
                    await Promise.all(tasks);
                }
            }
        },
        saveCreds: () => writeData(creds, 'creds')
    };
}

module.exports = { usePrismaAuthState };