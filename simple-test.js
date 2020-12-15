const { exec } = require('child_process')
const { stderr } = require('process')

exec(`node -e "console.log(require('./index.js'))"`, (err, stdout, stderr) => {
  if (err) {
    console.error(err)
    process.exit(1)
  }
  console.info(stdout)
  console.info(stderr)
})
