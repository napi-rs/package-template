const { platform } = require('os')

const { loadBinding } = require('@node-rs/helper')

try {
  // __dirname means load native addon from current dir
  // 'package-template' means native addon name is `package-template`
  // the first arguments was decided by `build` script in `package.json`
  // the second arguments was decided by `napi.name` field in `package.json`
  module.exports = loadBinding(__dirname, 'package-template')
} catch (e) {
  try {
    module.exports = require(`@napi-rs/package-template-${platform()}`)
  } catch (e) {
    throw new TypeError('Not compatible with your platform. Error message: ' + e.message)
  }
}
