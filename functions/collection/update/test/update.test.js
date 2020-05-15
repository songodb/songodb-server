require('dotenv').config()

const { 
  update,
  currentDate,
  inc, min, max, mul,
  rename, set, unset
} = require('../lib/update')

describe('update', () => {
  it ('should do multiple update operations', async () => {
    let doc = { a: "hello", b: { foo: "bar" }, c: 1 }
    let up = { 
      "$currentDate": { date: true },
      "$set": { "b.foo": "foo" }, 
      "$rename": { a: "aa" }, 
      "$inc": { c: 1 } 
    }
    update(up)(doc)
    expect(doc).toEqual({ 
      date: expect.anything(),
      aa: "hello", 
      b: { foo: "foo" },
      c: 2
    })
  })
})

describe('$currentDate', () => {
  let doc = { d: null }
  let up = { "$currentDate": { d: true } }
  it ('should set a field to current timestamp', async () => {
    currentDate(doc, [ ], up["$currentDate"])
    expect(doc).toEqual({
      d: expect.anything()
    })
  })
})

describe('$inc', () => {
  it ('should increment an existing field', async () => {
    let doc = { i: 1 }
    let up = { "$inc": { i: 1 } }
    inc(doc, [ ], up["$inc"])
    expect(doc).toEqual({
      i: 2
    })
  })
  it ('should set value if field is non-existent', async () => {
    let doc = { a: "b" }
    let up = { "$inc": { i: 1 } }
    inc(doc, [ ], up["$inc"])
    expect(doc).toEqual({
      a: "b",
      i: 1
    })
  })
  it ('should throw if inc non-numeric type', async () => {
    let doc = { i: "1" }
    let up = { "$inc": { i: 1 } }
    let error = null
    try {
      inc(doc, [ ], up["$inc"])
    } catch (err) {
      error = err
    }
    expect(error).toBeTruthy()
    expect(error.message).toEqual("Cannot apply $inc to a value of non-numeric type")
  })
})

describe('$min', () => {
  it ('should replace if min', async () => {
    let doc = { i: 2 }
    let up = { "$min": { i: 1 } }
    min(doc, [ ], up["$min"])
    expect(doc).toEqual({
      i: 1
    })
  })
  it ('should no-op if not min', async () => {
    let doc = { i: 1 }
    let up = { "$min": { i: 2 } }
    min(doc, [ ], up["$min"])
    expect(doc).toEqual({
      i: 1
    })
  })
})

describe('$max', () => {
  it ('should replace if max', async () => {
    let doc = { i: 1 }
    let up = { "$max": { i: 2 } }
    max(doc, [ ], up["$max"])
    expect(doc).toEqual({
      i: 2
    })
  })
  it ('should no-op if not max', async () => {
    let doc = { i: 3 }
    let up = { "$max": { i: 2 } }
    max(doc, [ ], up["$max"])
    expect(doc).toEqual({
      i: 3
    })
  })
})

describe('$mul', () => {
  it ('should multiply an existing field', async () => {
    let doc = { i: 2 }
    let up = { "$mul": { i: 3 } }
    mul(doc, [ ], up["$mul"])
    expect(doc).toEqual({
      i: 6
    })
  })
  it ('should set value to 0 if field is non-existent', async () => {
    let doc = { a: "b" }
    let up = { "$mul": { i: 2 } }
    mul(doc, [ ], up["$mul"])
    expect(doc).toEqual({
      a: "b",
      i: 0
    })
  })
  it ('should throw if inc non-numeric type', async () => {
    let doc = { i: "1" }
    let up = { "$mul": { i: 2 } }
    let error = null
    try {
      mul(doc, [ ], up["$mul"])
    } catch (err) {
      error = err
    }
    expect(error).toBeTruthy()
    expect(error.message).toEqual("Cannot apply $mul to a value of non-numeric type")
  })
})

describe('$rename', () => {
  it ('should rename and existing field', async () => {
    let doc = { "hello": 2 }
    let up = { "$rename": { "hello": "world" } }
    rename(doc, [ ], up["$rename"])
    expect(doc).toEqual({
      "world": 2
    })
  })
  it ('should no-op if field is non-existent', async () => {
    let doc = { "hello": 2 }
    let up = { "$rename": { "foo": "bar" } }
    rename(doc, [ ], up["$rename"])
    expect(doc).toEqual({
      hello: 2
    })
  })
})

describe('$set', () => {
  it ('should set top level fields', async () => {
    let doc = { "hello": "world", "foo": "bar" }
    let up = { "$set": { "hello": "bar", "foo": "world" } }
    set(doc, [ ], up["$set"])
    expect(doc).toEqual({
      "hello": "bar", 
      "foo": "world"
    })
  })
  it ('should set field if it is non-existent', async () => {
    let doc = { "hello": 2 }
    let up = { "$set": { "foo": "bar" } }
    set(doc, [ ], up["$set"])
    expect(doc).toEqual({
      hello: 2,
      foo: "bar"
    })
  })
  it ('should set a nested field', async () => {
    let doc = { "hello": { "foo": "bar", "hello": "world" } }
    let up = { "$set": { "hello.hello": "hello" } }
    set(doc, [ ], up["$set"])
    expect(doc).toEqual({
      hello: {
        foo: "bar",
        hello: "hello"
      }
    })
  })
  it ('should set a nonexistent nested field', async () => {
    let doc = { "hello": { "foo": "bar" } }
    let up = { "$set": { "hello.foo": "hello", "hello.new": "world", "hello.very.new": "foo" } }
    set(doc, [ ], up["$set"])
    expect(doc).toEqual({
      hello: {
        foo: "hello",
        new: "world",
        very: {
          new: "foo"
        }
      }
    })
  })
})

describe('$unset', () => {
  it ('should delete a top level field', async () => {
    let doc = { "hello": "world", "foo": "bar" }
    let up = { "$unset": { "hello": true } }
    unset(doc, [ ], up["$unset"])
    expect(doc).toEqual({
      "foo": "bar"
    })
  })
  it ('should delete a nested field', async () => {
    let doc = { "hello": { "foo": "bar", "a": "z" } }
    let up = { "$unset": { "hello.a": true } }
    unset(doc, [ ], up["$unset"])
    expect(doc).toEqual({
      hello: { "foo": "bar" }
    })
  })
  it ('should no-op delete a non-existent field', async () => {
    let doc = { "hello": "world" }
    let up = { "$unset": { "foo": true } }
    unset(doc, [ ], up["$unset"])
    expect(doc).toEqual({
      hello: "world"
    })
  })
})

