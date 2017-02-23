#!/bin/sh

RUST_BACKTRACE=1 RUST_LOG=raft_rust=debug cargo run 1000 0 127.0.0.1:8080 127.0.0.1:8081 127.0.0.1:8082

