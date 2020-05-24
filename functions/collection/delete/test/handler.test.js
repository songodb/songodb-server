require('dotenv').config()
const AWS = require('aws-sdk')
const createSongoDB = require('@songodb/songodb-mongo-s3')
const { handler } = require('../lib/handler')

describe('handler', () => {
  let event = null
  let { collection } = createSongoDB(new AWS.S3(), process.env.BUCKET, { 
    instanceId: 'collection',
    dbName: 'delete',
    collectionName: 'delete'
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
  it ('should delete a single record', async () => {
    let event = createTestEvent({ _id: "1" })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.deletedCount).toEqual(1)
  })
  it ('should delete multiple records using a filter', async () => {
    let event = createTestEvent({ "first": { "$in": [ "Jane", "John" ] } })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.deletedCount).toEqual(2)
  })
  it ('should delete all content in a collection collection', async () => {
    let event = createTestEvent({ })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.deletedCount).toEqual(3)
  })
  it ('should drop a collection', async () => {
    let event = createTestEvent()
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.deletedCount).toEqual(3)
    expect(body.dropped).toBe(true)
  })
  it ('should not delete any records if collection does not exist', async () => {
    let event = createTestEvent({ })
    event.pathParameters.instance = 'doesnotexist'
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.deletedCount).toEqual(0)
  })
})

function createTestEvent(filter, options) {
  return {
    "pathParameters": {
      "instance": "collection",
      "db": "delete",
      "collection": "delete"
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
