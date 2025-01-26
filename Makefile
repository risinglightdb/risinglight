TPCH_DBGEN_PATH := tpch-dbgen

docs_check:
	cargo doc --no-deps --document-private-items --all-features # TODO: docs check won't fail if there is warning, should be fixed later

docs:
	cargo doc --no-deps --document-private-items --all-features --open

fmt_check:
	cargo fmt --all -- --check

fmt:
	cargo fmt --all

clippy_check:
	cargo clippy --workspace --all-features --all-targets

clippy:
	cargo clippy --workspace --all-features --all-targets --fix

build:
	cargo build --all-features --all-targets

test:
	cargo nextest run --workspace --all-features

check: fmt_check clippy_check build test docs_check

apply_planner_test:
	UPDATE_PLANNER=1 cargo test --test sqlplannertest

clean:
	cargo clean
	rm -rf $(TPCH_DBGEN_PATH)

$(TPCH_DBGEN_PATH):
	mkdir -p target
	git clone https://github.com/electrum/tpch-dbgen.git $(TPCH_DBGEN_PATH)

tpch: $(TPCH_DBGEN_PATH)
	make -C $(TPCH_DBGEN_PATH)
	cd $(TPCH_DBGEN_PATH) && ./dbgen -f && mkdir -p tbl && mv *.tbl tbl && chmod -R 755 tbl

tpch-10gb: $(TPCH_DBGEN_PATH)
	make -C $(TPCH_DBGEN_PATH)
	cd $(TPCH_DBGEN_PATH) && ./dbgen -f -s 10 && mkdir -p tbl && mv *.tbl tbl && chmod -R 755 tbl

.PHONY: docs check fmt fmt_check clippy clippy_check build test docs_check clean tpch
