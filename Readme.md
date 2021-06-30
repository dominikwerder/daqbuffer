# Daqbuffer

DAQ retrieval http API, contains:

* Retrieval http API to run on the nodes of our facilities (backends).
* Proxy to run on data-api.psi.ch for distribution of requests to facilities.


# Build

Tested on RHEL 7 and 8, CentOS 8.

If not yet done, see [Setup Toolchain](#setup-toolchain) first.

Then run in this directory:

```bash
cargo build --release
```

Binary is at: `./target/release/daqbuffer`


# Setup Toolchain

Install Rust toolchain.
Quoting from <https://www.rust-lang.org/tools/install> the official installation method:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

This specifically requires a verified TLS connection and then executes the installer.

Installation will by default be done only for your user. No superuser privileges required.

You should have the commands `cargo` and `rustup` now available in your terminal.


# HTTP API docs

The documentation of the currently running service is served by the service itself:

<https://data-api.psi.ch/api/4/documentation/>

The docs are found in this repository here: <httpret/static/documentation/>


# License

GNU General Public License version 3 or later.
