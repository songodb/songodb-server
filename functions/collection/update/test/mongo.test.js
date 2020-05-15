require('dotenv').config()
const AWS = require('aws-sdk')
const { v4: uuidv4 } = require('uuid')
const createSongoS3 = require('@songodb/songodb-s3')
let s3 = createSongoS3(new AWS.S3(), process.env.BUCKET)
const { findMany, updateMany, replaceOne, upsert, insertOne  } = require('../lib/mongo')

describe('insertOne', () => {
  let prefix = `test/update/insertOne/`
  afterAll(async () => {
    await s3.deletePrefix(prefix)
  })
  it ('should insert a single record', async () => {
    let doc = { _id: "myid", hello: "world" }
    let result = await insertOne(s3, prefix, doc)
    expect(result).toEqual({
      insertedCount: 1,
      ops: [ doc ],
      insertedId: doc["_id"]
    })
  })
})

describe('updateMany', () => {
  let prefix = `test/update/updateMany/`
  afterEach(async () => {
    await s3.deletePrefix(prefix)
  })
  it ('should update and write multiple docs', async () => {
    //let filter = { name: "daniel" }
    let docs = [ 
      { _id: "1", i: 1, name: "daniel" },
      { _id: "2", i: 2, name: "daniel" }
    ]
    let up = { "$set": { i: 4 } }
    let result = await updateMany(s3, prefix, docs, up)
    expect(result).toEqual({
      "matchedCount": 2,
      "modifiedCount": 2,
      "upsertedCount": 0,
      "upsertedId": null
    })
    result = await findMany(s3, prefix, { }, { sort: [ [ "_id", 1 ] ] })
    expect(result.docs).toEqual([
      { "_id": "1", "i": 4, "name": "daniel" },
      { "_id": "2", "i": 4, "name": "daniel" }
    ])
  })
  it ('should only write modified docs', async () => {
    //let filter = { name: "daniel" }
    let docs = [ 
      { _id: "1", i: 1, name: "daniel" },
      { _id: "2", i: 2, name: "daniel" }
    ]
    let up = { "$set": { i: 2 } }
    let result = await updateMany(s3, prefix, docs, up)
    expect(result).toEqual({
      "matchedCount": 2,
      "modifiedCount": 1,
      "upsertedCount": 0,
      "upsertedId": null
    })
    result = await findMany(s3, prefix, { }, { sort: [ [ "_id", 1 ] ] })
    expect(result.docs).toEqual([
      { "_id": "1", "i": 2, "name": "daniel" }
    ])
  })
})

describe('upsert', () => {
  let prefix = `test/update/upsert/`
  afterAll(async () => {
    await s3.deletePrefix(prefix)
  })
  it ('should update and write multiple docs', async () => {
    let up = { "$set": { i: 4, hello: "world" } }
    let result = await upsert(s3, prefix, up)
    expect(result).toEqual({
      "matchedCount": 0,
      "modifiedCount": 0,
      "upsertedCount": 1,
      "upsertedId": {
        "_id": expect.anything()
      }
    })
  })
})

describe('replaceOne', () => {
  let prefix = `test/update/replaceOne/`
  afterAll(async () => {
    await s3.deletePrefix(prefix)
  })
  it ('should replace a doc', async () => {
    let docs = [ 
      { _id: "1", i: 1, name: "daniel" },
      { _id: "2", i: 2, name: "daniel" }
    ]
    let replace = { i: 4, hello: "world" } 
    let result = await replaceOne(s3, prefix, docs, replace)
    expect(result).toEqual({
      "matchedCount": 2,
      "modifiedCount": 1,
      "upsertedCount": 0,
      "upsertedId": null
    })
    result = await findMany(s3, prefix, { }, { sort: [ [ "_id", 1 ] ] })
    expect(result.docs).toEqual([ { "_id": "1", "i": 4, hello: "world"} ])
  })
})