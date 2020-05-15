require('dotenv').config()
const AWS = require('aws-sdk')
const createSongoS3 = require('@songodb/songodb-s3')
const { handler } = require('../lib/handler')
let s3 = createSongoS3(new AWS.S3(), process.env.BUCKET)

describe('handler', () => {
  let event = null
  beforeAll(async () => {
  })
  afterAll(async () => {
    await s3.deletePrefix(`somekey/somedb/somecollection/`)
  })
  it ('should insert a single doc', async () => {
    let event = createTestEvent({ doc: { hello: "world" } })
    let response = await handler(event)
    console.log(JSON.stringify(response, null, 2))
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
    console.log(JSON.stringify(response, null, 2))
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
      "instance": "somekey",
      "db": "somedb",
      "collection": "somecollection"
    },
    "stageVariables": {
      "BUCKET": process.env.BUCKET
    },
    body: JSON.stringify(body)
  }
}