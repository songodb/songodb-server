require('dotenv').config()
const AWS = require('aws-sdk')
const createSongoDB = require('@songodb/songodb-mongo-s3')
const { handler } = require('../lib/handler')

describe('handler', () => {
  let event = null
  let { collection } = createSongoDB(new AWS.S3(), process.env.BUCKET, { 
    instanceId: 'collection',
    dbName: 'update',
    collectionName: 'update'
  })
  beforeEach(async () => {
    await collection.insertMany([ 
      { _id: "1", first: "Jane", last: "Doe" }, 
      { _id: "2", first: "John", last: "Doe" },
      { _id: "3", first: "Joe", last: "Doe" } 
    ])
  })
  afterEach(async () => {
    await collection.drop() 
  })
  it ('should filter and update multiple records', async () => {
    let event = createTestEvent({ 
      filter: { last: "Doe" },
      update: { "$set": { last: "Smith" } }
    })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({
      matchedCount: 3,
      modifiedCount: 3,
      upsertedCount: 0,
      upsertedId: null,
    })
  })
  it ('should upsert a document', async () => {
    let event = createTestEvent({ 
      filter: { "first": "Jill" },
      update: { "$set": { first: "Jill" } },
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
    })
  })
  it ('should replace a single document', async () => {
    let event = createTestEvent({ 
      filter: { first: "Joe" },
      doc: { first: "Joseph", last: "Doe" }
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
    })
  })
  it ('should return no records if collection does not exist', async () => {
    let event = createTestEvent({ 
      filter: { first: "Jane" },
      update: { "$set": { last: "Smith" } }
    })
    event.pathParameters.collection = "doesnotexist"
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({
      "matchedCount": 0,
      "modifiedCount": 0,
      "upsertedCount": 0,
      "upsertedId": null,
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
      "instance": "collection",
      "db": "update",
      "collection": "update"
    },
    "stageVariables": {
      "BUCKET": process.env.BUCKET
    },
    "body": JSON.stringify(body)
  }
}
