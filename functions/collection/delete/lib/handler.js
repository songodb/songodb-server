const AWS = require('aws-sdk')
const createSongoS3 = require('@songodb/songodb-s3')
const { deleteOne, deleteCollection, deleteMany } = require('./mongo')

const DEFAULT_MAX_KEYS = 100

async function handler(event) {
  const t0 = Date.now()
  normalize(event)
  let s3 = createSongoS3(new AWS.S3(), event.stageVariables.BUCKET)
  const instance = event.pathParameters.instance
  const db = event.pathParameters.db
  const collection = event.pathParameters.collection
  let filter = event.queryStringParameters.filter && JSON.parse(event.queryStringParameters.filter) || null
  let options = event.queryStringParameters.options && JSON.parse(event.queryStringParameters.options) || { }
  let MaxKeys = options.MaxKeys || DEFAULT_MAX_KEYS
  let ContinuationToken = options.ContinuationToken

  let prefix = `${instance}/${db}/${collection}/`
  let result = null

  const t1 = Date.now()
  try {
    if (!filter || Object.keys(filter).length == 0) {
      result = await deleteCollection(s3, prefix, { MaxKeys, ContinuationToken })
    } else if (filter["_id"] && Object.keys(filter).length == 1) {
      result = await deleteOne(s3, prefix, filter["_id"], { MaxKeys, ContinuationToken })
    } else {
      result = await deleteMany(s3, prefix, filter, { MaxKeys, ContinuationToken })
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
  let explain = createExplain(result, t2-t0, t2-t1)
  return {
    statusCode: 200,
    body: JSON.stringify({ 
      deletedCount: result.Deleted.length,
      explain
    })
  }
}

// https://docs.mongodb.com/manual/tutorial/analyze-query-plan/
function createExplain(result, t, ts3) {
  let explain = { }
  explain.executionStats = { 
    nReturned: result.Deleted.length,
    executionTimeMillis: t,
    totalKeysExamined: 0, // displays 0 to indicate that this is query is not using an index.
    totalDocsExamined: result.KeyCount,
  }
  explain.s3 = {
    IsTrucated: result.IsTrucated, 
    KeyCount: result.KeyCount, 
    MaxKeys: result.MaxKeys, 
    NextContinuationToken: result.NextContinuationToken || null,
    TimeMillis: ts3
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
