require('dotenv').config()
const AWS = require('aws-sdk')
const { v4: uuidv4 } = require('uuid')
const createSongoS3 = require('@songodb/songodb-s3')
const { handler } = require('../lib/handler')
let s3 = createSongoS3(new AWS.S3(), process.env.BUCKET)

describe('handler', () => {
  let event = null
  let prefix = `test/insert/handler/`
  let objects = null
  beforeAll(async () => {
    objects = [ 
      { _id: uuidv4(), name: "obj2", i: 2 }, 
      { _id: uuidv4(), name: "obj3", i: 3 }, 
      { _id: uuidv4(), name: "obj1", i: 1 } 
    ]
    await s3.putMultiple(objects.map(obj => `${prefix}${obj["_id"]}`), objects)
  })
  afterAll(async () => {
    await s3.deletePrefix(prefix)
  })
  it ('should find using simple query', async () => {
    let event = createTestEvent({ name: "obj3" })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs).toEqual([ { _id: expect.anything(), name: "obj3", i: 3 } ])
    expect(body.explain).toMatchObject({
      executionStats: {
        nReturned: 1,
        executionTimeMillis: expect.anything(),
        totalKeysExamined: 0,
        totalDocsExamined: 3,
      },
      s3: {
        KeyCount: 3,
        MaxKeys: 100,
        NextContinuationToken: null,
        TimeMillis: expect.anything()
      }
    })
  })
  it ('should find all if null query given', async () => {
    let event = createTestEvent()
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs.length).toBe(3)
    expect(body.docs.map(doc => doc.name).sort()).toEqual([ "obj1", "obj2", "obj3" ])
  })
  it ('should find all if empty query given', async () => {
    let event = createTestEvent({ })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs.length).toBe(3)
    expect(body.docs.map(doc => doc.name).sort()).toEqual([ "obj1", "obj2", "obj3" ])
  })
  it ('should sort by a single field', async () => {
    let event = createTestEvent(null, { sort: [ [ "obj", 1 ] ] })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs.length).toBe(3)
    expect(body.docs.map(doc => doc.name)).toEqual([ "obj1", "obj2", "obj3" ])
  })
  it ('should skip and limit', async () => {
    let event = createTestEvent(null, { skip: 1, limit: 1, sort: [ [ "name", 1 ] ] })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs.length).toBe(1)
    expect(body.docs[0].name).toEqual("obj2")
  })
  it ('should get a single record and skip scan', async () => {
    let event = createTestEvent({ _id: objects[1]["_id"] })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs.length).toBe(1)
    expect(body.docs[0]).toEqual(objects[1])
    expect(body.explain).toMatchObject({
      executionStats: {
        nReturned: 1,
        executionTimeMillis: expect.anything(),
        totalKeysExamined: 0,
        totalDocsExamined: 1,
      },
      s3: {
        KeyCount: 1,
        MaxKeys: 100,
        NextContinuationToken: null,
        TimeMillis: expect.anything()
      }
    })
  })
})

function createTestEvent(query, options) {
  return {
    "pathParameters": {
      "instance": "test",
      "db": "insert",
      "collection": "handler"
    },
    "stageVariables": {
      "BUCKET": process.env.BUCKET
    },
    "queryStringParameters": {
      "query": query && JSON.stringify(query) || null,
      "options": options && JSON.stringify(options) || null
    } 
  }
}
