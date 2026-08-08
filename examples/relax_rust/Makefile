# The SDK is a std-only crate with no crates.io dependencies, so an offline
# release build needs no network; the produced binary target/release/relax is
# the runner file a job references.
relax: src/main.rs Cargo.toml ../../src/httk/workflow/native/rust/src/lib.rs
	cargo build --release --offline
	cp target/release/relax relax

clean:
	cargo clean
	rm -f relax
