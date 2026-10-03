# ash-domain — developer Makefile
#
# The *core* crate links no `ash-*` code: the shipped extensions and the HLC
# clock are opt-in cargo features (`fsm`, `audit`, `lock`, `hlc`, or
# `extensions` for the three extensions at once), off by default. A consumer who
# does not enable them pulls none of those crates.
#
# All ash-* dependencies come from crates.io, so a fresh checkout builds with
# plain cargo; these targets are shortcuts.

CARGO ?= cargo

.DEFAULT_GOAL := build

.PHONY: help build check test test-all run fmt clippy clippy-all clean

help: ## Show this help.
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# --- cargo -------------------------------------------------------------------

build: ## Build the workspace.
	$(CARGO) build

check: ## Type-check without producing binaries.
	$(CARGO) check

test: ## Run the test suite (unit, integration, and doc tests).
	$(CARGO) test

test-all: ## Run the test suite with every feature enabled.
	$(CARGO) test --all-features

run: ## Run the sandbox_side_effect example (needs the `sandbox` feature).
	$(CARGO) run --example sandbox_side_effect --features sandbox

fmt: ## Format the code.
	$(CARGO) fmt

clippy: ## Lint with clippy, warnings as errors.
	$(CARGO) clippy --all-targets -- -D warnings

clippy-all: ## Lint every feature with clippy, warnings as errors.
	$(CARGO) clippy --all-targets --all-features -- -D warnings

clean: ## Remove cargo build artifacts.
	$(CARGO) clean
