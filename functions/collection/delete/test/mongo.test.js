require('dotenv').config()
const AWS = require('aws-sdk')
const { v4: uuidv4 } = require('uuid')
const createSongoS3 = require('@songodb/songodb-s3')
let s3 = createSongoS3(new AWS.S3(), process.env.BUCKET)
const { deleteOne, deleteMany, deleteCollection } = require('../lib/mongo')

describe('deleteOne', () => {
  let prefix = `test/delete/deleteOne/`
  let objects = null
  beforeEach(async () => {
    objects = [ 
      { _id: "2", name: "obj2", i: 2 }, 
      { _id: "3", name: "obj3", i: 3 }, 
      { _id: "1", name: "obj1", i: 1 } 
    ]
    await s3.putMultiple(objects.map(obj => `${prefix}${obj["_id"]}`), objects)
  })
  afterAll(async () => {
    await s3.deletePrefix(prefix)
  })
  it ('should delete a single record', async () => {
    let result = await deleteOne(s3, prefix, objects[0]["_id"])
    expect(result).toMatchObject({
      IsTruncated: false,
      KeyCount: 1,
      MaxKeys: null,
      NextContinuationToken: null,
      Deleted: [ { Key: 'test/delete/deleteOne/2' } ],
      Errors: []
    })
  })
  it ('should delete multiple records using a filter', async () => {
    let result = await deleteMany(s3, prefix, { "i": { "$gte": 2 } })
    expect(result).toMatchObject({
      IsTruncated: false,
      KeyCount: 3,
      MaxKeys: 100,
      NextContinuationToken: null,
      Deleted: [
        { Key: 'test/delete/deleteOne/2' },
        { Key: 'test/delete/deleteOne/3' }
      ],
      Errors: []
    })
  })
  it ('should delete the entire collection', async () => {
    let result = await deleteCollection(s3, prefix)
    expect(result).toMatchObject({
      IsTruncated: false,
      KeyCount: 3,
      MaxKeys: 100,
      NextContinuationToken: null,
      Deleted: [
        { Key: 'test/delete/deleteOne/1' },
        { Key: 'test/delete/deleteOne/2' },
        { Key: 'test/delete/deleteOne/3' }
      ]
    })
  })
})