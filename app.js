const express = require('express')
const cors = require('cors')

const { continuouslyRetryFunction, createCrashTable, createInventoryTable, toggleCrashTable } = require('./index')

let page = 0
let websiteInventory = 100
let warehouseInventory = 100
let customerBank = 1000
// let crashMode = 0 // 0 = off, 1 = on

const app = express()
app.use(cors())
app.use(express.json())

app.post('/page', (req, res) => {
  const result = page
  res.json({ result })
})

app.post('/data', (req, res) => {
  const result = {
    websiteInventory,
    warehouseInventory,
    customerBank,
  }
  res.json({ result })
})

app.post('/incrementPage', (req, res) => {
  page = page + 1
  const result = page
  res.json({ result })
})

app.post('/decrementPage', (req, res) => {
  page = page - 1
  const result = page
  res.json({ result })
})

app.post('/addToCart', (req, res) => {
  if (websiteInventory > 0) {
    websiteInventory = websiteInventory - 1
    res.status(200)
  } else {
    res.status(400)
  }
  const result = websiteInventory
  res.json({ result })
})

app.post('/confirmPayment', async (req, res) => {
  warehouseInventory = warehouseInventory - 1
  customerBank = customerBank - 10

  console.log('1')
  const LAMBDA_FUNCTION_ARN = 'arn:aws:lambda:us-east-1:000000000000:function:demo_purchase_function'

  let res2 = await continuouslyRetryFunction(LAMBDA_FUNCTION_ARN)

  console.log(res2)
  console.log('2')
  const result = {
    warehouseInventory,
    customerBank,
  }
  res.json({ result })
})

app.post('/createCrashTable', async (req, res) => {
  await createCrashTable()

  res.json({ ok: 'ok' })
})

app.post('/createInventoryTable', async (req, res) => {
  await createInventoryTable()
  res.json({ ok: 'ok' })
})

app.post('/toggleCrash', async (req, res) => {
  let res2 = await toggleCrashTable()
  console.log("res: ", res2);
  res.json({ crashed: res2 })
})

app.listen(3000, () => console.log('Server running on http://localhost:3000'))
