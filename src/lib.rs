#![deny(clippy::all)]

use napi_derive::napi;

#[cfg(all(
  any(windows, unix),
  target_arch = "x86_64",
  not(target_env = "musl"),
  not(debug_assertions)
))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[napi]
pub fn plus_100(input: u32) -> u32 {
  input + 100
}
