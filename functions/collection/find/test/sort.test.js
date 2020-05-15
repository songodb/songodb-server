require('dotenv').config()

const { sort } = require('../lib/sort')

describe('sort', () => {
  it ('should sort number fields ascending using one index', async () => {
    let objects = [ { a: 2 }, { a: 3 }, { a: 1 } ]
    let indexes = [ [ "a", 1 ] ]
    let sorted = objects.sort(sort(indexes))
    expect(sorted).toEqual([ { a: 1 }, { a: 2 }, { a: 3 } ])
  })
  it ('should sort number fields descending using one index', async () => {
    let objects = [ { a: 2 }, { a: 3 }, { a: 1 } ]
    let indexes = [ [ "a", -1 ] ]
    let sorted = objects.sort(sort(indexes))
    expect(sorted).toEqual([ { a: 3 }, { a: 2 }, { a: 1 } ])
  })
  it ('should sort null as first value when ascending', async () => {
    let objects = [ { a: 2 }, { a: null }, { a: 1 } ]
    let indexes = [ [ "a", 1 ] ]
    let sorted = objects.sort(sort(indexes))
    expect(sorted).toEqual([ { a: null }, { a: 1 }, { a: 2 } ])
  })
  it ('should treat undefined as null', async () => {
    let objects = [ { a: 2 }, { }, { a: 1 } ]
    let indexes = [ [ "a", 1 ] ]
    let sorted = objects.sort(sort(indexes))
    expect(sorted).toEqual([ { }, { a: 1 }, { a: 2 } ])
  })
  it ('should sort null as last value when descending', async () => {
    let objects = [ { a: 2 }, { a: null }, { a: 1 } ]
    let indexes = [ [ "a", -1 ] ]
    let sorted = objects.sort(sort(indexes))
    expect(sorted).toEqual([ { a: 2 }, { a: 1 }, { a: null } ])
  })
  it ('should sort string fields ascending using multiple indexes', async () => {
    let objects = [ { a: 2, b: "world" }, { a: 2, b: "hello" }, { a: 1, b: "zap" } ]
    let indexes = [ [ "a", 1 ], [ "b", 1 ] ]
    let sorted = objects.sort(sort(indexes))
    expect(sorted).toEqual([  { a: 1, b: "zap" },  { a: 2, b: "hello" }, { a: 2, b: "world" } ])
  })
  it ('should sort string fields mixed ascending/descending using multiple indexes', async () => {
    let objects = [ { a: 2, b: "world" }, { a: 2, b: "hello" }, { a: 1, b: "zap" } ]
    let indexes = [ [ "a", 1 ], [ "b", -1 ] ]
    let sorted = objects.sort(sort(indexes))
    expect(sorted).toEqual([  { a: 1, b: "zap" },  { a: 2, b: "world" }, { a: 2, b: "hello" } ])
  })
  it ('should sort nested fields', async () => {
    let objects = [ { a: { b: 2 } }, { a: { b: 3 } }, { a: { b: 1 } } ]
    let indexes = [ [ "a.b", 1 ] ]
    let sorted = objects.sort(sort(indexes))
    expect(sorted).toEqual([ { a: { b: 1 } }, { a: { b: 2 } }, { a: { b: 3 } } ])
  })
  it ('should sort missing nested fields', async () => {
    let objects = [ { a: { b: 2 } }, { a: { b: 3 } }, { a: { c: "hello" } } ]
    let indexes = [ [ "a.b", 1 ] ]
    let sorted = objects.sort(sort(indexes))
    expect(sorted).toEqual([ { a: { c: "hello" } }, { a: { b: 2 } }, { a: { b: 3 } } ])
  })
})