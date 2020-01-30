# rsmq-rs

Async RSMQ port to rust. RSMQ is a simple redis queue system that works in any redis v2.6+. It contains the same methods as the original one in https://github.com/smrchy/rsmq

This crate uses async in the implementation. If you want to use it in your sync code you can use tokio / async_std "block_on" method. Async was used in order to simplify the code and allow 1-to-1 port oft he JS code.

![Crates.io](https://img.shields.io/crates/v/rsmq-rs) ![Crates.io](https://img.shields.io/crates/l/rsmq-rs)

## Installation

Add in your Cargo.toml
```
rsmq-rs = "1"
```

