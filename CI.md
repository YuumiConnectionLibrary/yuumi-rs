# Continuous integration

Workflow: `.github/workflows/ci.yml`.

- Triggers: pushes and pull requests for `main` and `dev`, plus manual dispatch.
- Platforms: `windows-latest`, `ubuntu-latest`, and `macos-latest`.
- Toolchains: latest stable Rust, Go, and Python 3.
- Gates: rustfmt, Clippy with warnings denied, crate package verification,
  25 engine cases, and real Go interop.
- Spec pin: `45729f1075ec5afcd9fd811db944385d6672eec3`; changing it requires an explicit
  reviewed workflow edit.
- Cache: disabled until run timings demonstrate a useful target.
- Timeout: 10 minutes for spec validation and 30 minutes per platform cell.
- Artifacts: per-case report, logs, cleanup diagnostics, toolchain versions,
  and repository commits, retained for 14 days.

Local equivalent from the directory containing all repositories:

```powershell
cargo fmt --manifest-path yuumi-rs/Cargo.toml --all -- --check
cargo clippy --manifest-path yuumi-rs/Cargo.toml --locked --all-targets -- -D warnings
cargo package --manifest-path yuumi-rs/Cargo.toml --locked --allow-dirty
python yuumi-spec/conformance/harness.py --repositories-root . --spec-sha 45729f1075ec5afcd9fd811db944385d6672eec3 --platform windows --sdk rust --stage all --output-dir artifacts/rust
```

`--allow-dirty` permits CI metadata and documentation changes in the checkout;
Cargo still validates the exact package file set before producing the archive.
