const AWS = require('aws-sdk')
const createSongoDB = require('@songodb/songodb-mongo-s3')

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
    collectionName,
    doc, 
    docs, 
    options
  } = params
  let { collection } = createSongoDB(new AWS.S3(), event.stageVariables.BUCKET, { instanceId, dbName, collectionName })
  let result = null
  try {
    if (doc) { 
      result = await collection.insertOne(doc, options)
    } else {
      result = await collection.insertMany(docs, options)
    }
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
  params.collectionName = event.pathParameters.collection
  let { doc, docs, options } = parseJSONParam(event.body) || { }
  params.doc = doc
  params.docs = docs
  params.options = options
  return params
}

function defaultParams() {
  return {
    filter: null,
    options: { 
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
