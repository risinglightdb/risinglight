TPCH_DBGEN_PATH := target/tpch-dbgen

docs_check:
	cargo doc --no-deps --document-private-items --all-features # TODO: docs check won't fail if there is warning, should be fixed later

docs:
	cargo doc --no-deps --document-private-items --all-features --open

fmt_check:
	cargo fmt --all -- --check

fmt:
	cargo fmt --all

clippy_check:
	cargo clippy --all-features --all-targets

clippy:
	cargo clippy --all-features --all-targets --fix

build:
	cargo build --all-features --all-targets

test:
	cargo test --all-features

check: fmt_check clippy_check build test docs_check

clean:
	cargo clean

$(TPCH_DBGEN_PATH):
	mkdir target || true
	git clone https://github.com/electrum/tpch-dbgen.git $(TPCH_DBGEN_PATH)

tpch: $(TPCH_DBGEN_PATH)
	make -C $(TPCH_DBGEN_PATH)
	cd $(TPCH_DBGEN_PATH) && ./dbgen -f && mkdir -p tbl && mv *.tbl tbl

.PHONY: docs check fmt fmt_check clippy clippy_check build test docs_check clean tpch
