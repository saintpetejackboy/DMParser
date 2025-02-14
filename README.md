# DMParser 📜✨

[![Crates.io](https://img.shields.io/crates/v/DMParser?style=for-the-badge)](https://crates.io/crates/DMParser)
[![docs.rs](https://img.shields.io/docsrs/DMParser?style=for-the-badge)](https://docs.rs/DMParser)
[![Build Status](https://github.com/saintpetejackboy/DMParser/actions/workflows/rust.yml/badge.svg)](https://github.com/saintpetejackboy/DMParser/actions)
[![Rust Version](https://img.shields.io/badge/rustc-1.71+-blue?style=for-the-badge)](https://blog.rust-lang.org/)
[![Dependency Status](https://deps.rs/repo/github/saintpetejackboy/DMParser/status.svg)](https://deps.rs/repo/github/saintpetejackboy/DMParser)
![Lines of Code](https://img.shields.io/tokei/lines/github/saintpetejackboy/DMParser?style=flat-square)

[![Code Style](https://img.shields.io/badge/rustfmt-checked-blue?style=for-the-badge)](https://github.com/rust-lang/rustfmt)
[![Unsafe Code](https://img.shields.io/badge/unsafe-0%25-green?style=for-the-badge)](https://doc.rust-lang.org/book/ch19-01-unsafe-rust.html)
[![Issues](https://img.shields.io/github/issues/saintpetejackboy/DMParser?style=for-the-badge)](https://github.com/saintpetejackboy/DMParser/issues)
[![Pull Requests](https://img.shields.io/github/issues-pr/saintpetejackboy/DMParser?style=for-the-badge)](https://github.com/saintpetejackboy/DMParser/pulls)


A high-performance Rust tool for processing and ingesting Deal Machine CSV files. DMParser uses modern asynchronous operations, bulk database inserts, and robust error handling to efficiently process large datasets.

### Updates
Version 0.1.2 - 2/14/2025
	+ Changed binary name to match Repository
	+ Fixed phone misalignment from parsing when rows would fail
	+ Added support for the "state" part of the property address
	+ Reduced redundancy by rejecting contacts without unique contact phone numbers

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Database Setup](#database-setup)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

---

## Overview

DMParser is designed to take CSV files (exported from Deal Machine) and efficiently parse, validate, and insert records into a MySQL/MariaDB database. It handles bulk inserts, duplicate checking, and file-locking to prevent concurrent executions—all in a clean, asynchronous Rust application.

---

## Features

- **High Performance:**  
  Asynchronous CSV parsing and bulk database operations for rapid processing.

- **Robust Error Handling:**  
  Comprehensive logging and error management ensure reliable processing even with malformed data.

- **Automatic Campaign Management:**  
  Automatically creates or reuses campaign records based on the CSV’s metadata.

- **Flexible Configuration:**  
  Easily adjust settings such as upload directories, batch sizes, and execution timeouts via environment variables.

- **Duplicate Prevention:**  
  In-memory and database checks prevent duplicate entries and ensure data integrity.

- **Modular & Extensible:**  
  Well-structured code designed for easy maintenance and future expansion.

---

## Requirements

- **Rust:** Latest stable version (tested with Rust 1.70+)
- **Database:** MySQL or MariaDB with valid connection credentials
- **Other Dependencies:** See [Cargo.toml](Cargo.toml) for a complete list (includes `dotenvy`, `sqlx`, `tokio`, `csv`, etc.)

---

## Setup & Installation

### 1. Clone the Repository

```bash
git clone https://github.com/saintpetejackboy/DMParser.git
cd DMParser
```

### 2. Set Up Environment Variables

Create a `.env` file in the project root. You can refer to the provided [`.env.example`](.env.example):

```dotenv
# .env

# MySQL database connection string
DATABASE_URL=mysql://username:password@localhost:3306/dbname

# Directories for processing
UPLOAD_DIR=./uploads
PROCESSED_DIR=./processed

# Lock file used to prevent concurrent processing
LOCK_FILE=./process.lock

# Batch insert size for the CSV processing
BATCH_SIZE=1000

# Maximum execution seconds per file before timeout
MAX_EXECUTION_SECONDS=3600
```

> **Note:** Replace the placeholder values with your actual configuration details.

### 3. Build the Project

Compile the project in release mode:

```bash
cargo build --release
```

---

## Configuration

DMParser reads its configuration from environment variables. Ensure that your `.env` file includes the following keys:

- **DATABASE_URL:** Your MySQL/MariaDB connection string.
- **UPLOAD_DIR:** Directory containing CSV files to process.
- **PROCESSED_DIR:** Directory where processed files are moved.
- **LOCK_FILE:** Path for the lock file (prevents concurrent runs).
- **BATCH_SIZE:** Number of records to insert per batch.
- **MAX_EXECUTION_SECONDS:** Maximum allowed seconds for processing a single file.

---

## Usage

After building the project, run the application:

```bash
cargo run --release
```

DMParser automatically scans the `UPLOAD_DIR` for CSV files that match the expected filename pattern. Processed files will be moved to the `PROCESSED_DIR` once complete.

> **Tip:** Use `--help` for additional command-line options:
> ```bash
> cargo run --release -- --help
> ```

---

## Database Setup

Before running DMParser, ensure your database has the necessary tables. See the [SQL setup script](sql/create_tables.sql) in the `sql` directory for instructions on creating the required tables (campaigns, emoji, address, and phonequeue).


---

## Contributing

Contributions are welcome!

---

## License

This project is unlicensed. 

---

## Contact

For any questions, issues, or feature requests, please [open an issue](https://github.com/saintpetejackboy/DMParser/issues) or contact the maintainers.

---

Happy parsing! 🚀
