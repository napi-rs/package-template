const express = require('express');
const cors = require('cors');

const {plus100} = require('./index');

const app = express();
app.use(cors()); 
app.use(express.json());

app.get('/plus100', (req, res) => {
  const input = req.query.input;
  const result = plus100(input);
  res.json({ result });
});

app.listen(3000, () => console.log("Server running on http://localhost:3000"));
