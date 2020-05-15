
const sift = require('sift')

async function deleteOne(s3, prefix, id, options) {
  options = options || { }
  let key = `${prefix}${id}`
  let { Deleted, Errors } = await s3.deleteOne(key)
  return {
    IsTruncated: false,
    KeyCount: Deleted && 1 || 0,
    MaxKeys: options.MaxKeys || null,
    NextContinuationToken: null,
    Deleted: [ Deleted ],
    Errors
  }
}

async function deleteMany(s3, prefix, filter, options) {
  options = options || { }
  let scan = await s3.getPrefix(prefix, options)
  let docs = scan.Contents.map(content => content.Body)
  if (filter) {
    docs = docs.filter(sift(filter))
  }
  let keys = docs.map(doc => `${prefix}${doc["_id"]}`)
  let { Deleted, Errors } = await s3.deleteMultiple(keys)
  return {
    IsTruncated: scan.IsTruncated,
    KeyCount: scan.KeyCount || 0,
    MaxKeys: scan.MaxKeys,
    NextContinuationToken: scan.NextContinuationToken || null,
    Deleted,
    Errors,
  }
}

async function deleteCollection(s3, prefix, options) {
  options = options || { }
  let scan = await s3.deletePrefix(prefix, options)
  return {
    IsTruncated: scan.IsTruncated,
    KeyCount: scan.KeyCount || 0,
    MaxKeys: scan.MaxKeys,
    NextContinuationToken: scan.NextContinuationToken || null,
    Deleted: scan.Deleted,
    Errors: scan.Errors,
  }
}

module.exports = exports = { 
  deleteOne,
  deleteMany,
  deleteCollection
}