# Contributing to ash-domain

Thanks for your interest in contributing!

## Development

```sh
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

Please make sure all three pass before opening a pull request.

## Pull requests

- Keep changes focused; one logical change per PR.
- Add tests for new behaviour and bug fixes.
- Update `CHANGELOG.md` under **Unreleased** for user-visible changes.

## Security issues

Do not report security problems in public issues. See [SECURITY.md](SECURITY.md).

## License

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as MIT OR Apache-2.0, without any additional terms or conditions.
