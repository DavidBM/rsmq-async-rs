# RSMQ in async Rust 

RSMQ port to async rust. RSMQ is a simple redis queue system that works in any redis v2.6+. It contains the same methods as the original one in https://github.com/smrchy/rsmq

This crate uses async in the implementation. If you want to use it in your sync code you can use tokio "block_on" method. Async was used in order to simplify the code and allow 1-to-1 port oft he JS code.


![Crates.io](https://img.shields.io/crates/v/rsmq_async) ![Crates.io](https://img.shields.io/crates/l/rsmq_async)

## Installation

Check [https://crates.io/crates/rsmq_async](https://crates.io/crates/rsmq_async)

## Async executor

For now the futures of this library are dependent of the Tokio reactor. This is because the redis-rs dependency. Once redis-rs makes Tokio optinal the same will happen to this library.
