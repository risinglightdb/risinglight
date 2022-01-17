docs:
	cargo doc --no-deps --document-private-items --all-features --open

check:
	cargo fmt --all -- --check
	cargo clippy --all-features --all-targets
	cargo build --all-features --all-targets
	cargo test --all-features
