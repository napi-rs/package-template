import test from 'node:test'
import assert from 'node:assert/strict'

import { plus100 } from '../index.js'

test('sync function from native code', () => {
  const fixture = 42
  assert.equal(plus100(fixture), fixture + 100)
})
