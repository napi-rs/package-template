const { plus100 } = require('./index')
const count = plus100(0);
console.assert(count === 100, 'Simple test failed')
console.log(count);
console.info('Simple test passed')
