require('dotenv').config()
const sift = require('sift')

describe('Sift', () => {
  it ('should run the example from the readme', async () => {
    let fn = sift({ $in: ["hello", "world"] })
    let result1 = ["hello", "sifted", "array!"].filter(fn)
    expect(result1).toEqual(['hello'])
  })
})