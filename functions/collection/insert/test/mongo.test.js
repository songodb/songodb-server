require('dotenv').config()
const AWS = require('aws-sdk')
const s3 = require('@songodb/songodb-s3')(new AWS.S3(), process.env.BUCKET)
const { insertOne, insertMany } = require('../lib/mongo')

describe('insertOne', () => {
  let prefix = 'insertOne/'
  afterAll(async () => {
    await s3.deletePrefix(prefix)
  })
  it ('should insert a single doc', async () => {
    let doc = { _id: "myid", hello: "world" }
    let result = await insertOne(s3, prefix, doc)
    expect(result).toMatchObject({
      "insertedCount": 1,
      "ops": [
        {
          "_id": "myid",
          "hello": "world"
        }
      ],
      "insertedId": "myid"
    })
  })
  it ('should set an _id if not provided', async () => {
    let doc = { hello: "world" }
    let result = await insertOne(s3, prefix, doc)
    expect(result).toMatchObject({
      "insertedCount": 1,
      "ops": [
        {
          "_id": expect.anything(),
          "hello": "world"
        }
      ],
      "insertedId": expect.anything()
    })
  })
})

describe('insertMany', () => {
  let prefix = 'insertMany/'
  afterAll(async () => {
    await s3.deletePrefix(prefix)
  })
  it ('should insert multiple docs', async () => {
    let docs = [ { _id: "myid", hello: "world" }, { _id: "yourid", foo: "bar" } ]
    let result = await insertMany(s3, prefix, docs)
    expect(result).toMatchObject({
      "insertedCount": 2,
      "ops": [
        {
          "_id": "myid",
          "hello": "world"
        },
        {
          "_id": "yourid",
          "foo": "bar"
        }
      ],
      "insertedIds": [ "myid", "yourid" ]
    })
  })
  it ('should set an _id if not provided', async () => {
    let docs = [ { hello: "world" }, { foo: "bar" } ]
    let result = await insertMany(s3, prefix, docs)
    expect(result).toMatchObject({
      "insertedCount": 2,
      "ops": [
        {
          "_id": expect.anything(),
          "hello": "world"
        },
        {
          "_id": expect.anything(),
          "foo": "bar"
        }
      ],
      "insertedIds": [ expect.anything(), expect.anything() ]
    })
  })
})