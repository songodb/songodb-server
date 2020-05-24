require('dotenv').config()
const AWS = require('aws-sdk')
const createSongoDB = require('@songodb/songodb-mongo-s3')
const { handler } = require('../lib/handler')

describe('handler', () => {
  let event = null
  let { collection } = createSongoDB(new AWS.S3(), process.env.BUCKET, { 
    instanceId: 'collection',
    dbName: 'insert',
    collectionName: 'insert'
  })
  beforeAll(async () => {
    
  })
  afterAll(async () => {
    await collection.drop() 
  })
  it ('should insert a single doc', async () => {
    let event = createTestEvent({ doc: { hello: "world" } })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({
      insertedCount: 1,
      ops: [ {
        _id: expect.anything(),
        hello: "world"
      } ],
      insertedId: expect.anything()
    })
  })
  it ('should insert multiple docs', async () => {
    let event = createTestEvent({ docs: [ { hello: "world" }, { foo: "bar" } ] })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({
      insertedCount: 2,
      ops: [ 
        {
          _id: expect.anything(),
          hello: "world"
        }, 
        {
          _id: expect.anything(),
          foo: "bar"
        }, 
      ],
      insertedIds: [ expect.anything(), expect.anything() ]
    })
  })
})

function createTestEvent(body) {
  return {
    "pathParameters": {
      "instance": "collection",
      "db": "insert",
      "collection": "insert"
    },
    "stageVariables": {
      "BUCKET": process.env.BUCKET
    },
    body: JSON.stringify(body)
  }
}