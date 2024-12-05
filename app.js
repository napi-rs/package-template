const express = require('express');
const cors = require('cors');

const { plus100, hi, continuouslyRetryFunction } = require('./index');
let page = 0;
let websiteInventory = 100;
let warehouseInventory = 100;
let customerBank = 1000;

const app = express();
app.use(cors()); 
app.use(express.json());

app.post('/page', (req, res) => {
  const result = page;
  res.json({ result });
});

app.post('/data', (req, res) => {
  const result = {
    websiteInventory,
    warehouseInventory,
    customerBank,
  };
  res.json({ result });
})

app.post('/incrementPage', (req, res) => {
  page = page + 1;
  const result = page;
  res.json({ result });
});

app.post('/decrementPage', (req, res) => {
  page = page - 1;
  const result = page;
  res.json({ result });
});

app.post('/addToCart', (req, res) => {
  if (websiteInventory > 0) {
    websiteInventory = websiteInventory - 1;
    res.status(200);
  } else {
    res.status(400); 
  }
  const result = websiteInventory;
  res.json({ result });
});

app.post('/confirmPayment', async (req, res) => {
  warehouseInventory = warehouseInventory - 1;
  customerBank = customerBank - 10;
  console.log("1");
  let res1 = hi(1);
  console.log(res1);
  let res2 = await continuouslyRetryFunction("arn:aws:lambda:us-east-1:443370680529:function:confirm_purchase:31")
  console.log(res2);
  console.log("2");
  const result = {
    warehouseInventory,
    customerBank
  };
  res.json({ result });
});

app.listen(3000, () => console.log("Server running on http://localhost:3000"));
