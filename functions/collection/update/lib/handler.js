const AWS = require('aws-sdk')
const createSongoS3 = require('@songodb/songodb-s3')
const { findMany, updateMany, replaceOne, upsert } = require('./mongo')

const DEFAULT_MAX_KEYS = 100

async function handler(event) {
  const t0 = Date.now()
  normalize(event)
  let s3 = createSongoS3(new AWS.S3(), event.stageVariables.BUCKET)
  const method = event.requestContext.http.method
  const op = (method == 'PUT' && 'REPLACE') || (method == 'PATCH' && 'UPDATE') || null
  const instance = event.pathParameters.instance
  const db = event.pathParameters.db
  const collection = event.pathParameters.collection
  let filter = event.body.filter || null
  let options = event.body.options || { }
  let MaxKeys = options.MaxKeys || DEFAULT_MAX_KEYS
  let ContinuationToken = options.ContinuationToken || null
  // updateOne, updateMany
  let update = event.body.update || null 
  let upsertOption = options.upsert || false
  // replaceOne
  let doc = event.body.doc || null 
  
  let prefix = `${instance}/${db}/${collection}/`
  let scan = null
  const t1 = Date.now()
  try {
    scan = await findMany(s3, prefix, filter, { MaxKeys, ContinuationToken })
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        errorMessage: `Internal Server Error: findMany ${err.message}`
      })
    }
  }
  let result = {
    matchedCount: 0,
    modifiedCount: 0, 
    upsertedCount: 0,
    upsertedId: null
  }
  switch(op) {
    case 'REPLACE': 
      if (scan.docs.length == 0 && upsertOption) {
        result = await upsert(s3, prefix, { "$set": doc })
      } else if (scan.docs.length > 0) {
        result = await replaceOne(s3, prefix, scan.docs, doc, { upsert: upsertOption })
      }
      break
    case 'UPDATE':
      if (scan.docs.length == 0 && upsertOption) {
        result = await upsert(s3, prefix, update)
      } else if (scan.docs.length > 0) {
        result =  await updateMany(s3, prefix, scan.docs, update, options)
      }
      break
    default:
      return {
        statusCode: 400,
        body: JSON.stringify({ errorMessage: `Bad Request: Unsupported HTTP method ${method} { PUT, PATCH } only`})
      }
  }
  const t2 = Date.now()
  result.explain = createExplain(scan, result, t2-t0, t2-t1)
  return {
    statusCode: 200,
    body: JSON.stringify(result)
  }
}

// https://docs.mongodb.com/manual/tutorial/analyze-query-plan/
function createExplain(scan, result, elapsed, elapsed_s3) {
  let explain = { }
  explain.executionStats = { 
    nReturned: result.modifiedCount || result.upsertedCount,
    executionTimeMillis: elapsed,
    totalKeysExamined: 0, // displays 0 to indicate that this is query is not using an index.
    totalDocsExamined: scan.explain.s3.KeyCount,
  }
  explain.s3 = {
    IsTrucated: scan.explain.s3.IsTrucated, 
    KeyCount: scan.explain.s3.KeyCount, 
    MaxKeys: scan.explain.s3.MaxKeys, 
    NextContinuationToken: scan.explain.s3.NextContinuationToken || null,
    TimeMillis: elapsed_s3
  }
  return explain
}

function normalize(event) {
  event.pathParameters = event.pathParameters || { }
  event.queryStringParameters = event.queryStringParameters || { }
  event.stageVariables = event.stageVariables || { }
  event.body = event.body && JSON.parse(event.body) || { }
  return event
}

module.exports = exports = {
  handler
}
