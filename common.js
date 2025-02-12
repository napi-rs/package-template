let websiteInventory
let warehouseInventory
let customerBank
const websiteInventoryText = document.getElementById('websiteInventory')
const warehouseInventoryText = document.getElementById('warehouseInventory')
const customerBankText = document.getElementById('customerBank')
const progressBar = document.getElementById('progressBar')
const progressText = document.getElementById('progressText')
const button = document.getElementById('mainButton')
const crashButton = document.getElementById('crashApp')
const url = 'http://localhost:3000'
let confirmLoading = false;

async function getData() {
  const res = await fetch(`${url}/data`, {
    method: 'POST',
  })
  const data = await res.json()
  websiteInventory = data.result.websiteInventory
  warehouseInventory = data.result.warehouseInventory
  customerBank = data.result.customerBank
}

function updateCrashButton(state) {
  if (state === 1) {
    crashButton.innerHTML = 'Reboot Application'
    crashButton.style.background = '#128132'
  } else {
    crashButton.innerHTML = 'Crash Application'
    crashButton.style.background = '#F74C00'
  }
}

function updateLoadingButton() {
  if (confirmLoading == false) {
    button.innerHTML = 'Purchase Ferris!'
    button.style.background = '#128132'
  } else {
    button.innerHTML = 'Loading...'
    button.style.background = '#c9c9c9'
    button.style.cursor = 'default'
  }
}
function updateData() {
  websiteInventoryText.innerText = websiteInventory
  warehouseInventoryText.innerText = warehouseInventory
  customerBankText.innerText = `$${customerBank}`
}

window.onload = async function () {
  await getData()
  updateData()
}

async function confirmPayment() {
  confirmLoading = true
  updateLoadingButton()
  const confirmPaymentUrl = `${url}/confirmPayment`
  const confirmPaymentRes = await fetch(confirmPaymentUrl, {
    method: 'POST',
  })
  const data = await confirmPaymentRes.json()
  warehouseInventory = data.warehouseInventory
  customerBank = data.customerBank
  updateData()
  confirmLoading = false
  updateLoadingButton()
}

button.addEventListener('click', async () => {
    await confirmPayment()
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
    if (data.crashed === '1') {
      updateCrashButton(1)
      alert('Crash mode enabled!')
    } else {
      updateCrashButton(0)
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
  const makeBankTable = `${url}/createBankTable`
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

    const bankResponse = await fetch(makeBankTable, {
      method: 'POST',
    })
    if (!bankResponse.ok) {
      throw new Error('Failed to create bank table')
    }
    console.log('Bank table created successfully')

    alert('Tables created successfully!')
  } catch (error) {
    console.error('Error creating tables:', error)
    alert('Error occurred while creating tables')
  }
})
