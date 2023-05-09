const { existsSync, readFileSync } = require('fs')
const { join } = require('path')

const { platform, arch } = process

function isMusl() {
  // For Node 10
  if (!process.report || typeof process.report.getReport !== 'function') {
    try {
      return readFileSync('/usr/bin/ldd', 'utf8').includes('musl')
    } catch (e) {
      return true
    }
  } else {
    const { glibcVersionRuntime } = process.report.getReport().header
    return !glibcVersionRuntime
  }
}

const adapters = {
  android: {
    display: 'Android',
    supported: new Set(['arm64', 'arm']),
  },
  win32: {
    display: 'Windows',
    supported: new Set(['x64', 'ia32', 'arm64']),
    libc() {
      return 'msvc'
    },
  },
  darwin: {
    display: 'macOS',
    supported: new Set(['x64', 'arm64']),
  },
  freebsd: {
    display: 'FreeBSD',
    supported: new Set(['x64']),
  },
  linux: {
    display: 'Linux',
    supported: new Set(['x64', 'arm64', 'arm']),
    libc(architecture) {
      if (architecture === 'arm')
        return 'gnueabihf'
      return isMusl() ? 'musl' : 'gnu'
    },
  },
}

function loadNativeBinding() {
  const adapter = adapters[platform]
  if (!adapter) {
    throw new Error(`Unsupported OS: ${platform}, architecture: ${arch}`)
  }
  if (!adapter.supported.has(arch)) {
    throw new Error(`Unsupported architecture on ${adapter.display}: ${arch}`)
  }

  const suffix = adapter.libc ? `-${adapter.libc(arch)}.node` : '.node'
  const localFile = `package-template.${platform}-${arch}${suffix}`
  const localFileExisted = existsSync(join(__dirname, localFile))

  let nativeBinding = null
  let loadError = null

  try {
    if (localFileExisted) {
      nativeBinding = require(`./${localFile}`)
    } else {
      nativeBinding = require(`@napi-rs/package-template-${platform}-${arch}`)
    }
  } catch (e) {
    loadError = e
  } finally {
    return [nativeBinding, loadError]
  }
}

let [nativeBinding, loadError] = loadNativeBinding()

if (!nativeBinding) {
  if (loadError) {
    throw loadError
  }
  throw new Error(`Failed to load native binding`)
}

const { plus100 } = nativeBinding

module.exports.plus100 = plus100
