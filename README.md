# Database Exporter

A simple Rust command-line tool to export database tables to CSV files with automatic timestamping.

## Features

- Connects to MySQL databases
- Exports single or multiple tables to CSV format in one run
- Automatically handles various data types (integers, floats, booleans, text, dates)
- Timestamps output files for easy versioning
- Lightweight and fast
- Supports multiple table exports with a single command

## Prerequisites

- Rust (edition 2024)
- MySQL database
- Cargo (Rust's package manager)

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/db_exporter.git
   cd db_exporter
   ```

2. Build the project:
   ```bash
   cargo build --release
   ```

The binary will be available at `target/release/db_exporter`

## Configuration

1. Copy the example environment file:

   ```bash
   cp .env.example .env
   ```

2. Edit the `.env` file with your database connection details:

   ```
   DATABASE_URL=mysql://username:password@localhost:3306/your_database
   TABLE_NAME=table1,table2,table3
   CSV_OUTPUT=output
   ```

   - `DATABASE_URL`: MySQL connection string in the format `mysql://username:password@host:port/database`
   - `TABLE_NAME`: Comma-separated list of table names to export (e.g., `users,products,orders`)
   - `CSV_OUTPUT`: Base name for the output files (will be appended with table name and timestamp)

## Usage

```bash
# Run the exporter
cargo run --release

# Or run the built binary directly
./target/release/db_exporter
```

The tool will create CSV files with the specified base name, table name, and a timestamp (e.g., `output_table1_2023-01-01_12-00-00.csv`).

### Multiple Table Export

To export multiple tables in a single run, simply list them in the `TABLE_NAME` environment variable, separated by commas:

```
TABLE_NAME=customers,orders,products
```

This will create separate CSV files for each table with their respective names in the output files.

## Supported Data Types

- Integer types (INT, INTEGER, BIGINT, SMALLINT)
- Floating point numbers (FLOAT, DOUBLE, DECIMAL)
- Boolean values (BOOL, BOOLEAN)
- Text (TEXT, VARCHAR, CHAR, LONGTEXT)
- Date/Time (DATETIME, TIMESTAMP)

## Error Handling

- The tool will exit with an error if:
  - The database connection fails
  - Any of the specified tables don't exist
  - There are permission issues
  - The output files cannot be created

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
