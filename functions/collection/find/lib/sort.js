// create something like sift, but it takes the MongDB sort format [['a', 1], ['b', -1 ]] and 
// creates a javascript sort function that can be passed into .sort(...)
const jessy = require('jessy')

function sort(indexes) {
  return (a, b) => {
    for (let i=0; i<indexes.length; i++) {
      let field = indexes[i][0]
      let order = indexes[i][1]
      let a_val = jessy(field, a) || null // undefined -> null
      let b_val = jessy(field, b) || null
      if (a_val < b_val) return (-1 * order) 
      if (a_val > b_val) return order
    }
    return 0
  }
}

module.exports = exports = { sort }
