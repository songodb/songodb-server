const deepequal = require('deep-equal');
const { v4: uuidv4 } = require('uuid')
const sift = require('sift')
const { update } = require('./update')
const { sort } = require('./sort')

async function findMany(s3, prefix, query, options) {
  options = options || { }
  let t0 = Date.now()
  let { MaxKeys, ContinuationToken } = options 
  if (query && query["_id"] && (typeof query["_id"] === 'string')) {
    // Shortcut: we avoid a "full table scan" when our query
    // is looking for a single record by its unique _id   
    scan = await getPrefixWithId(s3, query["_id"])
  } else {
    // we do a "full table scan" up to MaxKeys from the ContinuationToken onward
    scan = await s3.getPrefix(prefix, { MaxKeys, ContinuationToken })
  }
  const t1 = Date.now()
  let docs = scan.Contents.map(content => content.Body)
  if (query) {
    docs = docs.filter(sift(query))
  }
  if (options.sort) {
    docs = docs.sort(sort(options.sort))
  }
  if (options.skip) {
    docs = docs.slice(options.skip)
  }
  if (options.limit) {
    docs = docs.slice(0, options.limit)
  }
  const t2 = Date.now()
  let explain = createExplain(scan, docs, t2 - t0, t1 - t0)
  return { docs, explain }
}

function createExplain(scan, docs, elapsed, elapsed_s3) {
  let explain = { }
  explain.executionStats = { 
    nReturned: docs.length,
    executionTimeMillis: elapsed,
    totalKeysExamined: 0, // displays 0 to indicate that this is query is not using an index.
    totalDocsExamined: scan.KeyCount,
  }
  explain.s3 = {
    IsTrucated: scan.IsTrucated, 
    KeyCount: scan.KeyCount, 
    MaxKeys: scan.MaxKeys, 
    NextContinuationToken: scan.NextContinuationToken || null,
    TimeMillis: elapsed_s3
  }
  return explain
}

async function getPrefixWithId(s3, id) {
  let doc = await s3.getOne(`${prefix}${id}`)
  return {
    IsTruncated: false,
    KeyCount: doc && 1 || 0,
    MaxKeys,
    NextContinuationToken: null,
    Contents: doc && [ doc ] || [ ]
  }
}

// updateMany(filter, update, options, callback)
// https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#updateMany
// https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#~updateWriteOpResult
async function updateMany(s3, prefix, docs, up, options) {
  options = options || { }
  let copy = JSON.parse(JSON.stringify(docs))
  let updated = [ ]
  copy.forEach(update(up))
  for (let i=0; i<copy.length; i++) {
    if (!deepequal(copy[i], docs[i])) {
      updated.push(copy[i])
    }
  }
  // Need to write each one of these docs by id
  let result = await Promise.all(updated.map(doc => insertOne(s3, prefix, doc, options)))
  return {
    matchedCount: docs.length,
    // technically these shouldn't always be equal, only if the update 
    // changed the object should it be considered modified
    // we could manually do a diff between original and modified if we 
    // really want get this count
    modifiedCount: updated.length, 
    upsertedCount: 0,
    upsertedId: null
  }
}

// we insert the doc using the given filter
async function upsert(s3, prefix, up, options) {
  options = options || { }
  let doc = { }
  update(doc)(up) // apply the update to a blank doc
  let { insertedId } = await insertOne(s3, prefix, doc, options)
  return {
    matchedCount: 0,
    modifiedCount: 0, 
    upsertedCount: 1,
    upsertedId: { _id: insertedId }
  }
}

async function replaceOne(s3, prefix, docs, replace, options) {
  replace["_id"] = docs[0]["_id"]
  await insertOne(s3, prefix, replace)
  return {
    matchedCount: docs.length,
    modifiedCount: 1, 
    upsertedCount: 0,
    upsertedId: null
  }
}

// https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#~insertOneWriteOpResult
async function insertOne(s3, prefix, doc, options) {
  doc["_id"] = doc["_id"] || uuidv4()
  let key = `${prefix}${doc["_id"]}`
  let result = await s3.putOne(key, doc)
  return {
    insertedCount: 1,
    ops: [ doc ],
    insertedId: doc["_id"]
  }
}

module.exports = exports = { 
  findMany,
  updateMany,
  replaceOne,
  upsert,
  insertOne
}