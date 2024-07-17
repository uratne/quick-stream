# Quick Stream
[![Build](https://github.com/uratne/quick-stream/actions/workflows/build.yml/badge.svg)](https://github.com/uratne/quick-stream/actions/workflows/build.yml) [![UnitTests](https://github.com/uratne/quick-stream/actions/workflows/unit-tests.yml/badge.svg)](https://github.com/uratne/quick-stream/actions/workflows/unit-tests.yml)![Rust Badge](https://img.shields.io/badge/Rust-1.79.0-000?logo=rust&logoColor=fff&style=flat)

* Quick Stream is a Rust-based solution designed to efficiently handle data upsert operations with a focus on performance and scalability. Utilizing asynchronous programming and a dynamic sender-receiver model, Quick Stream aims to streamline the process of synchronizing large datasets with minimal overhead.

## Files Overview

- **multi_table_upsert_support.rs**: Contains support functions and utilities for performing multi-table upsert operations.
- **multi_table_upsert.rs**: Implements the main functionality for multi-table upserts.
- **multi_table_delete_support.rs**: Provides support functions for multi-table delete operations.
- **builder_support.rs**: Contains utilities and support functions for building database queries.
- **multi_table_delete.rs**: Implements the main functionality for multi-table delete operations.
- **support.rs**: General support functions used across various modules.
- **upsert.rs**: Contains the core upsert logic.
- **shutdown_service.rs**: Implements functionality to safely shut down services.
- **lib.rs**: Main library file that integrates all modules.
- **delete.rs**: Contains the core delete logic.
- **builder.rs**: Implements query builder functionality.

## Crate

This library is available as a crate on [crates.io](https://crates.io/crates/quick_stream). You can include it in your `Cargo.toml`:

## Getting Started

### Prerequisites

- Rust 1.79.0
- Cargo package manager

### Development And Testing

1. Clone the repository:

```sh
git clone https://github.com/uratne/quick-stream
cd quick_sync
```

2. Test the project

```sh
cargo test --lib
```

## Features
1. unix-signals ( will enable graceful shutdown job on unix systems )
2. windows-signals ( will enable graceful shutdown job on windows systems )
    ***Same Cancellation Token Should be used to all of the processors of type QuickStream on windows***

## Roadmap
See the [ROADMAP](ROADMAP.md)

## Improvements
1. More Unit Tests Are Always welcome
2. Some simple examples have been given as a guide
3. For multi tables I have used a composite structure to handle all the tables, It would be nicer if It was done by defining another trait. For my use case this is enough. So I stop here for now. Maybe in future I'll fix this. If someone is willing to help out with this, your help is much appriciated.


## Acknowledgement
* [Shalinda Ranasinghe](https://github.com/shalinda)
