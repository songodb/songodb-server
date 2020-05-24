const AWS = require('aws-sdk')
const createSongoDB = require('@songodb/songodb-mongo-s3')

const DEFAULT_MAX_KEYS = 1000

async function handler(event) {
  let params = null 
  try {
    params = validateAndGetParams(event)
  } catch (err) {
    console.error(err)
    return {
      statusCode: 400,
      body: JSON.stringify({ errorMessage: err.message })
    }
  }
  let {
    instanceId,
    dbName,
    options
  } = params
  let { db } = createSongoDB(new AWS.S3(), event.stageVariables.BUCKET, { instanceId, dbName })
  let result = null
  try {
    result = await db.dropDatabase({ 
      MaxKeys: options.MaxKeys || DEFAULT_MAX_KEYS, 
      ContinuationToken: options.ContinuationToken || null
    })
  } catch (err) {
    console.error(err)
    let statusCode = 500
    if (err.code == "InvalidNamespace") {
      statusCode = 400
    }
    return {
      statusCode,
      body: JSON.stringify({ errorMessage: err.message })
    }
  }
  return {
    statusCode: 200,
    body: JSON.stringify(result)
  }
}

function validateAndGetParams(event) {
  normalize(event)
  let params = defaultParams()
  params.instanceId = event.pathParameters.instance
  params.dbName = event.pathParameters.db
  params.options = parseJSONParam(event.queryStringParameters.options) || { }
  return params
}

function normalize(event) {
  event.pathParameters = event.pathParameters || { }
  event.queryStringParameters = event.queryStringParameters || { }
  event.stageVariables = event.stageVariables || { }
  return event
}

function parseJSONParam(str) {
  try {
    return str && JSON.parse(str) || null
  } catch (err) {
    throw new Error(`Invalid JSON: ${str}`)
  }
}

function defaultParams() {
  return {
    filter: null,
    options: { 
      MaxKeys: DEFAULT_MAX_KEYS,
      ContinuationToken: null
    }
  }
}


module.exports = exports = {
  handler
}
