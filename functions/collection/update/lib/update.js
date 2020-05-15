const jessy = require('jessy')
const nessy = require('nessy')
const finicky = require('finicky')

function update(up) {
  return doc => {
    let ops = Object.keys(up).forEach(op => {
      apply(doc, [ ], op, up[op])
    })
  }
}

// Update Operators 
// https://docs.mongodb.com/manual/reference/operator/update/#id1

function apply(doc, path, op, val) {
  switch (op) {
    case "$currentDate":
      return currentDate(doc, path, val)
    case "$inc":
      return inc(doc, path, val)
    case "$min":
      return min(doc, path, val)
    case "$max":
      return max(doc, path, val)
    case "$mul":
      return mul(doc, path, val)
    case "$rename":
      return rename(doc, path, val)
    case "$set":
      return set(doc, path, val)
    case "$setOnInsert":
      // not sure how to handle this at this level
      // because we don't know whether we are doing an insert or not
      // return setOnInsert(doc, path, val)
    case "$unset":
      return unset(doc, path, val)
    default:
      throw new Error(`Unknown modifier: ${op}`)
  }
}

// https://docs.mongodb.com/manual/reference/operator/update/currentDate/#up._S_currentDate
function currentDate(doc, path, val) {
  Object.keys(val).forEach(key => {
    let date = new Date()
    if (typeof val[key] === true) {
      // noop
    } else if (val[key] && val[key]["$type"] == "timestamp") {
      // noop
    } else if (val[key] && val[key]["$type"] == "timestamp") {
      date.setHours(0,0,0,0)
    }
    nessy(path.concat([key]).join('.'), date, '.', doc)
  })
}

// https://docs.mongodb.com/manual/reference/operator/update/inc/#up._S_inc
function inc(doc, path, val) {
  Object.keys(val).forEach(key => {
    let amount = val[key]
    let current = jessy(path.concat([key]).join('.'), doc) || 0
    if (typeof current != 'number') throw new Error("Cannot apply $inc to a value of non-numeric type")
    nessy(path.concat([key]).join('.'), current + amount, '.', doc)
  })
}

function min(doc, path, val) {
  Object.keys(val).forEach(key => {
    let minval = val[key]
    let current = jessy(path.concat([key]).join('.'), doc)
    if (minval < current) {
      nessy(path.concat([key]).join('.'), minval, '.', doc)
    }
  })
}

function max(doc, path, val) {
  Object.keys(val).forEach(key => {
    let maxval = val[key]
    let current = jessy(path.concat([key]).join('.'), doc)
    if (maxval > current) {
      nessy(path.concat([key]).join('.'), maxval, '.', doc)
    }
  })
}

// https://docs.mongodb.com/manual/reference/operator/update/mul/#up._S_mul
function mul(doc, path, val) {
  Object.keys(val).forEach(key => {
    let amount = val[key]
    let current = jessy(path.concat([key]).join('.'), doc) || 0
    if (typeof current != 'number') throw new Error("Cannot apply $mul to a value of non-numeric type")
    nessy(path.concat([key]).join('.'), current * amount, '.', doc)
  })
}

// https://docs.mongodb.com/manual/reference/operator/update/rename/#up._S_rename
function rename(doc, path, val) {
  Object.keys(val).forEach(key => {
    let newname = val[key]
    let current = jessy(path.concat([key]).join('.'), doc) 
    if (current === undefined) return // rename non-existent field seems to no-op in mongo
    // set the new key value
    nessy(path.concat([newname]).join('.'), current, '.', doc)
    // delete the old key
    deleteKey(path.concat([key]).join('.'), doc)
  })
}

// https://docs.mongodb.com/manual/reference/operator/update/set/#up._S_set
// Currently does not support nested array notation e.g. "ratings.0.rating"
function set(doc, path, val) {
  Object.keys(val).forEach(key => {
    nessy(path.concat([key]).join('.'), val[key], '.', doc)
  })
}

function unset(doc, path, val) {
  Object.keys(val).forEach(key => {
    deleteKey(path.concat([key]).join('.'), doc)
  })
}

function deleteKey(keypath, doc) {
  if (keypath.indexOf(".") >= 0) {
    finicky(keypath, doc)
  } else {
    delete doc[keypath]
  }
}

module.exports = exports = { 
  update,
  apply,
  currentDate,
  inc, min, max, mul,
  rename, set, unset
}