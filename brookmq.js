const sqlite3 = require('sqlite3').verbose();

let isInitiated = false;
let connection;
const subscriptions = {};

const config = {
    pollInterval: 1000,
    databaseFile: 'brookmq.sqlite',
    queueTableName: 'brookmq_queue',
}

const open = async (databaseFile) => {
    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database(databaseFile, (err) => {
            if (err) {
                reject(err);
            } else {
                connection = db;
                resolve(db);
            }
        });
    });
}

const createQueueTable = async () => {
    return new Promise((resolve, reject) => {
        const query = `CREATE TABLE IF NOT EXISTS ${config.queueTableName} (
                id integer primary key, 
                topic text,
                payload text,
                lockCount integer default 0,
                lockedAt real default 0)`
        connection.run(query, (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

const publish = async (topic, payload) => {
    return new Promise((resolve, reject) => {
        const query = `INSERT INTO ${config.queueTableName} (topic, payload)
                        VALUES (?, ?)`;
        connection.run(query, [topic, payload], (err) => {
            if (err) {
                reject(err);
            } else {
                resolve(true);
            }
        });
    });
}

const getNextId = async (topic) => {
    return new Promise((resolve, reject) => {
        const query =
            `SELECT 
                id 
            FROM 
                ${config.queueTableName} 
            WHERE 
                topic = ?
                AND (
                    lockedAt = 0 
                    OR julianday('now') > julianday(lockedAt, '+5 minutes')
                )
            ORDER BY id`
        connection.get(query, [topic], (err, row) => {
            if (err) {
                reject(err);
            } else {
                if (row) {
                    resolve(row.id);
                } else {
                    resolve(null);
                }
            }
        });
    });
}

const lockMessage = async (id) => {
    return new Promise((resolve, reject) => {
        const query =
            `UPDATE 
                ${config.queueTableName} 
            SET 
                lockedAt = julianday('now'),
                lockCount = lockCount + 1
            WHERE 
                id = ? 
                AND (
                    lockedAt = 0 
                    OR julianday('now') > julianday(lockedAt, '+5 minutes')
                )`;
        connection.run(query, [id], async function (err) {
            if (err) {
                reject(err);
            } else {
                const rowsAffected = this.changes;
                resolve(rowsAffected == 1);
            }
        });
    })
}

const unlockMessage = async (id) => {
    return new Promise((resolve, reject) => {
        const query =
            `UPDATE 
                ${config.queueTableName} 
            SET 
                lockedAt = 0
            WHERE 
                id = ?`;
        connection.run(query, [id], async function (err) {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    })
}

const getMessage = async (id) => {
    return new Promise((resolve, reject) => {
        const query =
            `SELECT 
                id,
                topic,
                payload,
                lockCount,
                lockedAt
            FROM 
                ${config.queueTableName}
            WHERE 
                id = ?`
        connection.get(query, [id], (err, row) => {
            if (err) {
                reject(err);
            } else {
                resolve(row);
            }
        });
    });
}

const deleteMessage = async (id) => {
    return new Promise((resolve, reject) => {
        const query =
            `DELETE FROM ${config.queueTableName}
            WHERE id = ?`
        connection.run(query, [id], (err) => {
            if (err) {
                reject(err);
            } else {
                resolve();
            }
        });
    });
}

const getNext = async (topic) => {
    const id = await getNextId(topic);
    const locked = await lockMessage(id);
    if (locked) {
        const message = getMessage(id);
        return message;
    } else {
        return null;
    }
}

const subscribe = async (topic, callback) => {
    if (!isInitiated) {
        await open(config.databaseFile);
        await createQueueTable();
        await start();
    }

    if (!subscriptions[topic]) {
        subscriptions[topic] = [];
    }

    subscriptions[topic].push(callback);
}

const tick = async () => {
    const topics = Object.keys(subscriptions);
    for (let topic of topics) {
        const message = await getNext(topic);
        if (message) {
            const handle = {
                message,
                complete: async () => {
                    await deleteMessage(message.id);
                },
                keep: async () => {
                    await lockMessage(message.id);
                },
                reject: async () => {
                    await unlockMessage(message.id);
                }
            };

            const subscribers = subscriptions[topic];
            for (let i = 0; i < subscribers.length; i++) {
                const sub = subscribers[i];
                await sub(handle);
            }
        }
    }
}

const start = async () => {
    setInterval(async () => {
        await tick();
    }, config.pollInterval);
}

exports.publish = publish;
exports.subscribe = subscribe;
exports.config = config;