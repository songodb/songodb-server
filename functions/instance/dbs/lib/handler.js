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
    filter,
    options
  } = params
  let { instance } = createSongoDB(new AWS.S3(), event.stageVariables.BUCKET, { instanceId })
  let result = null
  try {
    result = await instance.listDatabases(filter, { 
      nameOnly: options.nameOnly, 
      MaxKeys: options.MaxKeys, 
      ContinuationToken: options.ContinuationToken })
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
  params.filter = parseJSONParam(event.queryStringParameters.filter) 
  params.options = parseJSONParam(event.queryStringParameters.options) || { }
  return params
}

function defaultParams() {
  return {
    filter: null,
    options: { 
      nameOnly: false,
      MaxKeys: DEFAULT_MAX_KEYS,
      ContinuationToken: null
    }
  }
}

function parseJSONParam(str) {
  try {
    return str && JSON.parse(str) || null
  } catch (err) {
    throw new Error(`Invalid JSON: ${str}`)
  }
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
