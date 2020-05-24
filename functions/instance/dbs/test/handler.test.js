require('dotenv').config()
const AWS = require('aws-sdk')
const createSongoDB = require('@songodb/songodb-mongo-s3')
const { handler } = require('../lib/handler')

describe('handler', () => {
  let event = null
  let { instance } = createSongoDB(new AWS.S3(), process.env.BUCKET, { instanceId: 'listDatabases' })
  beforeAll(async () => {
    await instance.db('db1').collection('col1').insertOne({ hello: "world" })
    await instance.db('db2').collection('col2').insertOne({ foo: "bar" })
  })
  afterAll(async () => {
    await instance.db('db1').dropDatabase()
    await instance.db('db2').dropDatabase()
  })
  it ('should get all the dbs', async () => {
    let event = createTestEvent({ })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({
      docs: [
        { name: "db1" },
        { name: "db2" }
      ],
      explain: {
        "executionStats": {
          "nReturned": 2,
          "executionTimeMillis": expect.anything(),
          "totalKeysExamined": 0,
          "totalDocsExamined": 2
        },
        "s3": {
          "KeyCount": 2,
          "MaxKeys": 100,
          "NextContinuationToken": null,
          "TimeMillis": expect.anything()
        }
      }
    })
  })
  it ("should only return the names via the namesOnly param", async () => {
    let event = createTestEvent({ }, { nameOnly: true })
    let response = await handler(event)
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({ docs: [ "db1", "db2" ] })
  })
})

function createTestEvent(filter, options) {
  return {
    "pathParameters": {
      "instance": 'listDatabases',
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
