const AWS = require('aws-sdk')
const createSongoS3 = require('@songodb/songodb-s3')
const { insertOne, insertMany } = require('./mongo')

async function handler(event) {
  normalize(event)
  let s3 = createSongoS3(new AWS.S3(), event.stageVariables.BUCKET)
  const instance = event.pathParameters.instance
  const db = event.pathParameters.db
  const collection = event.pathParameters.collection
  let prefix = `${instance}/${db}/${collection}/`
  let { docs, doc, options } = JSON.parse(event.body)
  let isOne = doc || docs && !Array.isArray(docs)
  let body = null
  try {
    if (isOne) {
      body = await insertOne(s3, prefix, (doc || docs), options)
    } else {
      body = await insertMany(s3, prefix, docs, options)
    }
    return {
      statusCode: 200,
      body: JSON.stringify(body)
    }
  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        errorMessage: `Internal Server Error: ${err.message}`
      })
    }
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
