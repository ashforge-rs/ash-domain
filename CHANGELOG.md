# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] — 2026-11-14

### Changed

- `ash-domain-macros` 0.1.1 builds on `syn` 3.
- Dependency updates: `futures` 0.3.34, `proc-macro2` 1.0.107, `thiserror` 2.0.21,
  `tokio` 1.54.1.

### Security

- Locked `tokio` 1.54.1 (RUSTSEC-2026-0386) and `event-listener` 5.4.2
  (RUSTSEC-2026-0221); replaced the yanked `chacha20` 0.10.1 with 0.10.2.

## [0.1.0] — 2026-10-03

First release.
