require('dotenv').config()
const AWS = require('aws-sdk')
const { v4: uuidv4 } = require('uuid')
const createSongoS3 = require('@songodb/songodb-s3')
const { handler } = require('../lib/handler')
let s3 = createSongoS3(new AWS.S3(), process.env.BUCKET)

describe('handler', () => {
  let event = null
  let prefix = `test/delete/handler/`
  let objects = null
  beforeEach(async () => {
    objects = [ 
      { _id: "2", name: "obj2", i: 2 }, 
      { _id: "3", name: "obj3", i: 3 }, 
      { _id: "1", name: "obj1", i: 1 } 
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
  it ('should delete a single record', async () => {
    let event = createTestEvent({ _id: "obj3" })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.deletedCount).toEqual(1)
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
  it ('should delete multiple records using a filter', async () => {
    let event = createTestEvent({ "i": { "$gte": 2 } })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.deletedCount).toEqual(2)
    expect(body.explain).toMatchObject({
      executionStats: {
        nReturned: 2,
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
  it ('should delete the entire collection', async () => {
    let event = createTestEvent({ })
    let response = await handler(event)
    console.log(response)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.deletedCount).toEqual(3)
    expect(body.explain).toMatchObject({
      executionStats: {
        nReturned: 3,
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
})

function createTestEvent(filter, options) {
  return {
    "pathParameters": {
      "instance": "test",
      "db": "delete",
      "collection": "handler"
    },
    "stageVariables": {
      "BUCKET": process.env.BUCKET
    },
    "queryStringParameters": {
      "filter": filter && JSON.stringify(filter) || null,
      "options": options && JSON.stringify(options) || null
    } 
  }
}
