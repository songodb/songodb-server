const { v4: uuidv4 } = require('uuid')

// https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#~insertOneWriteOpResult
async function insertOne(s3, prefix, doc, options) {
  doc["_id"] = doc["_id"] || uuidv4()
  let key = `${prefix}${doc["_id"]}`
  let result = await s3.putOne(key, doc)
  return {
    insertedCount: 1,
    ops: [ doc ],
    insertedId: doc["_id"]
    // connection: The connection object used for the operation.
    // result: The raw command result object returned from MongoDB (content might vary by server version).
  }
}

// https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#~insertWriteOpResult
async function insertMany(s3, prefix, docs, options) {
  docs.forEach(doc => { doc["_id"] = doc["_id"] || uuidv4() })
  let keys = docs.map(doc => `${prefix}${doc["_id"]}`)
  let result = await s3.putMultiple(keys, docs)
  return {
    insertedCount: result.length,
    ops: docs,
    insertedIds: docs.map(doc => doc["_id"])
    // connection: The connection object used for the operation.
    // result: The raw command result object returned from MongoDB (content might vary by server version).
  }
}

module.exports = exports = {
  insertOne,
  insertMany
}