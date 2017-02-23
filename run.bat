SET RUST_BACKTRACE=1
SET RUST_LOG=raft_rust=debug
cargo run 1000 0 localhost:8080 localhost:8081 localhost:8082
