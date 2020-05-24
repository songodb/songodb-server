require('dotenv').config()
const AWS = require('aws-sdk')
const createSongoDB = require('@songodb/songodb-mongo-s3')
const { handler } = require('../lib/handler')

describe('handler', () => {
  let event = null
  let { instance, db } = createSongoDB(new AWS.S3(), process.env.BUCKET, { instanceId: 'collection', dbName: 'listCollections' })
  let objects = null
  beforeAll(async () => {
    await db.collection('col1').insertOne({ hello: "world" })
    await db.collection('col2').insertOne({ foo: "bar" })
  })
  afterAll(async () => {
    await db.dropDatabase()
  })
  it ('should get all the collections for a db', async () => {
    let event = createTestEvent({ })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({
      docs: [
        { name: "col1" },
        { name: "col2" }
      ]
    })
  })
  it ("should only return the names via the namesOnly param", async () => {
    let event = createTestEvent({ }, { nameOnly: true })
    let response = await handler(event)
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({ docs: [ "col1", "col2" ] })
  })
})

function createTestEvent(filter, options) {
  return {
    "pathParameters": {
      "instance": "collection",
      "db": "listCollections",
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
