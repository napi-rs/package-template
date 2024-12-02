let websiteInventory;
let warehouseInventory;
let customerBank;
const websiteInventoryText = document.getElementById('websiteInventory');
const warehouseInventoryText = document.getElementById('warehouseInventory');
const customerBankText = document.getElementById('customerBank');
const progressBar = document.getElementById('progressBar')
const progressText = document.getElementById('progressText')
const button = document.getElementById('mainButton')
let page;

async function getPage() {
  const res = await fetch('http://localhost:3000/page', {
    method: "POST"
  });
  const data = await res.json();
  return data.result;
}

async function getData() {
  const res = await fetch('http://localhost:3000/data', {
    method: "POST"
  });
  const data = await res.json();
  websiteInventory = data.result.websiteInventory;
  warehouseInventory = data.result.warehouseInventory;
  customerBank = data.result.customerBank;
}

function updateData() {
  if (page === 0) {
    button.innerHTML = 'Add to Cart'
    button.style.background = '#2E509B'
    
  } else {
    button.innerHTML = 'Confirm Payment'
    button.style.background = '#128132'
  }
  websiteInventoryText.innerText = websiteInventory
  warehouseInventoryText.innerText = warehouseInventory
  customerBankText.innerText = `$${customerBank}`
}

window.onload = async function() {
  page = await getPage();
  await getData();
  updateData();
};

async function addToCart() {
  const incrementRes = await fetch("http://localhost:3000/incrementPage", {
    method: "POST"
  });
  const data = await incrementRes.json();
  page = data.result;

  const addToCartUrl = `http://localhost:3000/addToCart`;
  const addToCartRes = await fetch(addToCartUrl, {
      method: "POST"
  });
  if (addToCartRes.status == 400) {
    alert('Error adding to cart!');
  } else {
    const cartData = await addToCartRes.json();
    console.log(cartData);
    websiteInventory = cartData.result;
    updateData();
  }
}

async function confirmPayment() {
  const confirmPaymentUrl = `http://localhost:3000/confirmPayment`;
  const confirmPaymentRes = await fetch(confirmPaymentUrl, {
    method: "POST"
  });
  const data = await confirmPaymentRes.json();
  warehouseInventory = data.result.warehouseInventory;
  customerBank = data.result.customerBank;

  const decrementPageUrl = 'http://localhost:3000/decrementPage';
  const decrementPageRes = await fetch(decrementPageUrl, {
    method: "POST"
  });
  const pageData = await decrementPageRes.json();
  page = pageData.result;
  updateData();
}

button.addEventListener('click', async () => {
  if (page == 0) {
    await addToCart();
  } else {
    await confirmPayment();
  }
})

document.getElementById('crashApp').addEventListener('click', () => {
  alert('Application crashed! (Simulated)')
})