const { exec } = require('child_process')

exec(`node -e "console.log(require('./index.js'))"`, (err, stdout, stderr) => {
  console.info(stdout)
  console.info(stderr)
  if (err) {
    process.exit(1)
  }
})
