require('dotenv').config()
const AWS = require('aws-sdk')
const createSongoDB = require('@songodb/songodb-mongo-s3')
const { handler } = require('../lib/handler')

describe('handler', () => {
  let event = null
  let { collection } = createSongoDB(new AWS.S3(), process.env.BUCKET, { 
    instanceId: 'collection',
    dbName: 'find',
    collectionName: 'find'
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
  it ('should find using simple query', async () => {
    let event = createTestEvent({ first: "John" })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs).toEqual([ { _id: "2", first: "John", last: "Doe" } ])
  })
  it ('should find all if null query given', async () => {
    let event = createTestEvent()
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs.length).toBe(3)
    expect(body.docs.map(doc => doc.first).sort()).toEqual([ "Jane", "Joe", "John" ])
  })
  it ('should find all if empty query given', async () => {
    let event = createTestEvent({ })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs.length).toBe(3)
    expect(body.docs.map(doc => doc.first).sort()).toEqual([ "Jane", "Joe", "John" ])
  })
  it ('should sort by a single field', async () => {
    let event = createTestEvent(null, { sort: [ [ "first", 1 ] ] })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs.length).toBe(3)
    expect(body.docs.map(doc => doc.first)).toEqual([ "Jane", "Joe", "John"  ])
  })
  it ('should skip and limit', async () => {
    let event = createTestEvent(null, { skip: 1, limit: 1, sort: [ [ "first", 1 ] ] })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs.length).toBe(1)
    expect(body.docs[0].first).toEqual("Joe")
  })
  it ('should get a single record and skip scan', async () => {
    let event = createTestEvent({ _id: "2" })
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body.docs.length).toBe(1)
    expect(body.docs[0]).toEqual({ _id: "2", first: "John", last: "Doe" })
  })
  it ('should return no records if collection does not exist', async () => {
    let event = createTestEvent()
    event.pathParameters.collection = 'doesnotexist'
    let response = await handler(event)
    expect(response).toMatchObject({
      statusCode: 200
    })
    let body = JSON.parse(response.body)
    expect(body).toMatchObject({ docs: [] })
  })
})

function createTestEvent(query, options) {
  return {
    "pathParameters": {
      "instance": "collection",
      "db": "find",
      "collection": "find"
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
