#![warn(unused_extern_crates)]

use dotenv::dotenv;

#[tokio::main]
async fn main() {
    dotenv().ok();
    unsafe { std::env::set_var("RUST_BACKTRACE", "1") };
}
