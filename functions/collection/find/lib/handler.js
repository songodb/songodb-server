const AWS = require('aws-sdk')
const sift = require('sift')
const createSongoS3 = require('@songodb/songodb-s3')
const { sort } = require('./sort')

//const { insertOne, insertMany } = require('./mongo')

// find: https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#find
// findOne: https://mongodb.github.io/node-mongodb-native/3.6/api/Collection.html#findOne
// sift: https://github.com/crcn/sift.js#readme

// ?query=...&options=...
// initial set of options to implement:
// limit	number	0	optional
// Sets the limit of documents returned in the query.

// sort	array | object		optional
// Set to sort the documents coming back from the query. Array of indexes, [['a', 1]] etc.

// projection	object		optional
// The fields to return in the query. Object of fields to include or exclude (not both), {'a':1}

// skip	number	0	optional
// Set to skip N documents ahead in your query (useful for pagination).

// get and parse query
// if query has _id field against a plain string
// -> shortcut and just do a get on that id key
// else 
// -> read all docs out of the collection 
//    let client specify scan_limit and continuation_token if provided
//    to reduce how much the underlying S3 scan will serach over

const DEFAULT_MAX_KEYS = 100

async function handler(event) {
  const t0 = Date.now()
  normalize(event)
  let s3 = createSongoS3(new AWS.S3(), event.stageVariables.BUCKET)
  const instance = event.pathParameters.instance
  const db = event.pathParameters.db
  const collection = event.pathParameters.collection
  let query = event.queryStringParameters.query && JSON.parse(event.queryStringParameters.query) || null
  let options = event.queryStringParameters.options && JSON.parse(event.queryStringParameters.options) || { }
  let MaxKeys = options.MaxKeys || DEFAULT_MAX_KEYS
  let ContinuationToken = options.ContinuationToken

  let prefix = `${instance}/${db}/${collection}/`
  let scan = null 

  const t1 = Date.now()
  try {
    if (query && query["_id"] && (typeof query["_id"] === 'string')) {
      let doc = await s3.getOne(`${prefix}${query["_id"]}`)
      scan = {
        IsTruncated: false,
        KeyCount: doc && 1 || 0,
        MaxKeys,
        NextContinuationToken: null,
        Contents: doc && [ doc ] || [ ]
      }
    } else {
      scan = await s3.getPrefix(prefix, { MaxKeys, ContinuationToken })
    }
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        errorMessage: `Internal Server Error: ${err.message}`
      })
    }
  }
  const t2 = Date.now()
  let docs = scan.Contents.map(content => content.Body)
  if (query) {
    docs = docs.filter(sift(query))
  }
  if (options.sort) {
    docs = docs.sort(sort(options.sort))
  }
  if (options.skip) {
    docs = docs.slice(options.skip)
  }
  if (options.limit) {
    docs = docs.slice(0, options.limit)
  }
  const t3 = Date.now()
  let explain = createExplain(scan, docs, t3-t0, t2-t1)
  return {
    statusCode: 200,
    body: JSON.stringify({ docs, explain })
  }
}

// https://docs.mongodb.com/manual/tutorial/analyze-query-plan/
function createExplain(scan, docs, t, tscan) {
  let explain = { }
  explain.executionStats = { 
    nReturned: docs.length,
    executionTimeMillis: t,
    totalKeysExamined: 0, // displays 0 to indicate that this is query is not using an index.
    totalDocsExamined: scan.KeyCount,
  }
  explain.s3 = {
    IsTrucated: scan.IsTrucated, 
    KeyCount: scan.KeyCount, 
    MaxKeys: scan.MaxKeys, 
    NextContinuationToken: scan.NextContinuationToken || null,
    TimeMillis: tscan
  }
  return explain
}

function normalize(event) {
  event.pathParameters = event.pathParameters || { }
  event.queryStringParameters = event.queryStringParameters || { }
  event.stageVariables = event.stageVariables || { }
  return event
}

module.exports = exports = {
  handler
}
