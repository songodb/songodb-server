require('dotenv').config()
const AWS = require('aws-sdk')
const { v4: uuidv4 } = require('uuid')
const createSongoS3 = require('@songodb/songodb-s3')
const { handler } = require('../lib/handler')
let s3 = createSongoS3(new AWS.S3(), process.env.BUCKET)

describe('handler', () => {
  let event = null
  let prefix = `test/update/handler/`
  let objects = null
  beforeEach(async () => {
    objects = [ 
      { _id: "2", name: "obj2", i: 2, foo: "bar" }, 
      { _id: "3", name: "obj3", i: 3, foo: "bar" }, 
      { _id: "1", name: "obj1", i: 1, foo: "foo" } 
    ]
    await s3.putMultiple(objects.map(obj => `${prefix}${obj["_id"]}`), objects)
  })
  afterEach(async () => {
    try { 
      await s3.deletePrefix(prefix)
    } catch (err) {
      console.error(err.message)
    }
  })
  it ('should filter and update multiple records', async () => {
    let event = createTestEvent({ 
      filter: { "foo": "bar" },
      update: { "$max": { i: 3 } }
    })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({
      matchedCount: 2,
      modifiedCount: 1,
      upsertedCount: 0,
      upsertedId: null,
      explain: {
        executionStats: {
          nReturned: 1,
          executionTimeMillis: expect.anything(),
          totalKeysExamined: 0,
          totalDocsExamined: 3
        },
        s3: {
          KeyCount: 3,
          MaxKeys: 100,
          NextContinuationToken: null,
          TimeMillis: expect.anything()
        }
      }
    })
  })
  it ('should upsert a document', async () => {
    let event = createTestEvent({ 
      filter: { "name": "obj4" },
      update: { "$set": { i: 4 } },
      options: { upsert: true }
    })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({
      matchedCount: 0,
      modifiedCount: 0,
      upsertedCount: 1,
      upsertedId: { "_id": expect.anything() },
      explain: {
        executionStats: {
          nReturned: 1,
          executionTimeMillis: expect.anything(),
          totalKeysExamined: 0,
          totalDocsExamined: 3
        },
        s3: {
          KeyCount: 3,
          MaxKeys: 100,
          NextContinuationToken: null,
          TimeMillis: expect.anything()
        }
      }
    })
  })
  it ('should replace a single document', async () => {
    let event = createTestEvent({ 
      filter: { "name": "obj3" },
      doc: { name: "obj3", i: 3, foo: "foo" }
    })
    event.requestContext.http.method = 'PUT'
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({
      matchedCount: 1,
      modifiedCount: 1,
      upsertedCount: 0,
      upsertedId: null,
      explain: {
        executionStats: {
          nReturned: 1,
          executionTimeMillis: expect.anything(),
          totalKeysExamined: 0,
          totalDocsExamined: 3
        },
        s3: {
          KeyCount: 3,
          MaxKeys: 100,
          NextContinuationToken: null,
          TimeMillis: expect.anything()
        }
      }
    })
  })
})

function createTestEvent(body, options) {
  return {
    "requestContext": {
      "http": {
        "method": "PATCH"
      }
    },
    "pathParameters": {
      "instance": "test",
      "db": "update",
      "collection": "handler"
    },
    "stageVariables": {
      "BUCKET": process.env.BUCKET
    },
    "body": JSON.stringify(body)
  }
}
