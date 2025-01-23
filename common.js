let websiteInventory
let warehouseInventory
let customerBank
const websiteInventoryText = document.getElementById('websiteInventory')
const warehouseInventoryText = document.getElementById('warehouseInventory')
const customerBankText = document.getElementById('customerBank')
const progressBar = document.getElementById('progressBar')
const progressText = document.getElementById('progressText')
const button = document.getElementById('mainButton')
const url = 'http://localhost:3000'
let page

async function getPage() {
  const res = await fetch(`${url}/page`, {
    method: 'POST',
  })
  const data = await res.json()
  return data.result
}

async function getData() {
  const res = await fetch(`${url}/data`, {
    method: 'POST',
  })
  const data = await res.json()
  websiteInventory = data.result.websiteInventory
  warehouseInventory = data.result.warehouseInventory
  customerBank = data.result.customerBank
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

window.onload = async function () {
  page = await getPage()
  await getData()
  updateData()
}

async function addToCart() {
  const incrementRes = await fetch(`${url}/incrementPage`, {
    method: 'POST',
  })
  const data = await incrementRes.json()
  page = data.result

  const addToCartUrl = `${url}/addToCart`
  const addToCartRes = await fetch(addToCartUrl, {
    method: 'POST',
  })
  if (addToCartRes.status == 400) {
    alert('Error adding to cart!')
  } else {
    const cartData = await addToCartRes.json()
    console.log(cartData)
    websiteInventory = cartData.result
    updateData()
  }
}

async function confirmPayment() {
  const confirmPaymentUrl = `${url}/confirmPayment`
  const confirmPaymentRes = await fetch(confirmPaymentUrl, {
    method: 'POST',
  })
  const data = await confirmPaymentRes.json()
  warehouseInventory = data.result.warehouseInventory
  customerBank = data.result.customerBank

  const decrementPageUrl = `${url}/decrementPage`
  const decrementPageRes = await fetch(decrementPageUrl, {
    method: 'POST',
  })
  const pageData = await decrementPageRes.json()
  page = pageData.result
  updateData()
}

button.addEventListener('click', async () => {
  if (page == 0) {
    await addToCart()
  } else {
    await confirmPayment()
  }
})

// document.getElementById('crashApp').addEventListener('click', () => {
//   alert('Application crashed! (Simulated)')
// })

document.getElementById('crashApp').addEventListener('click', async () => {
  //const AWS_ENDPOINT_URL = 'http://localhost:4566' // We're using LocalStack
  const toggleCrashUrl = `${url}/toggleCrash`
  try {
    const response = await fetch(toggleCrashUrl, {
      method: 'POST',
    })

    if (!response.ok) {
      throw new Error('Failed to toggle crash state')
    }

    const data = await response.json()
    if (data.result === 1) {
      alert('Crash mode enabled!')
    } else {
      alert('Crash mode disabled!')
    }
  } catch (error) {
    console.error('Error toggling crash state:', error)
    alert('Error occurred while toggling crash state')
  }
})

document.getElementById('makeTables').addEventListener('click', async () => {
  const makeCrashTable = `${url}/createCrashTable`
  const makeInventoryTable = `${url}/createInventoryTable`
  try {
    const crashResponse = await fetch(makeCrashTable, {
      method: 'POST',
    })
    if (!crashResponse.ok) {
      throw new Error('Failed to create crash table')
    }
    console.log('Crash table created successfully')

    const inventoryResponse = await fetch(makeInventoryTable, {
      method: 'POST',
    })
    if (!inventoryResponse.ok) {
      throw new Error('Failed to create inventory table')
    }
    console.log('Inventory table created successfully')

    alert('Tables created successfully!')
  } catch (error) {
    console.error('Error creating tables:', error)
    alert('Error occurred while creating tables')
  }
})
