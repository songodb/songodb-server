require('dotenv').config()
const AWS = require('aws-sdk')
const createSongoDB = require('@songodb/songodb-mongo-s3')
const { handler } = require('../lib/handler')

describe('handler', () => {
  let event = null
  let { instance } = createSongoDB(new AWS.S3(), process.env.BUCKET, { instanceId: 'dropDatabase' })
  beforeEach(async () => {
    await instance.db('db1').collection('col1').insertOne({ hello: "world" })
  })
  afterEach(async () => {
    await instance.db('db1').dropDatabase()
  })
  it ('should drop the database', async () => {
    let event = createTestEvent()
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({
      deletedCount: 1,
      dropped: true
    })
  })
})

function createTestEvent(options) {
  return {
    "pathParameters": {
      "instance": "dropDatabase",
      "db": "db1"
    },
    "stageVariables": {
      "BUCKET": process.env.BUCKET
    },
    "queryStringParameters": {
      "options": options && JSON.stringify(options) || null
    } 
  }
}
