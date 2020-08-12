const { loadBinding } = require('@node-rs/helper')

try {
  // __dirname means load native addon from current dir
  // 'index' means native addon name is `index`
  // the value of this two arguments was decided by `build` script in `package.json`
  module.exports = loadBinding(__dirname, 'index')
} catch (e) {
  try {
    module.exports = require(`@swc-node/core-${platform()}`)
  } catch (e) {
    throw new TypeError('Not compatible with your platform. Error message: ' + e.message)
  }
}
