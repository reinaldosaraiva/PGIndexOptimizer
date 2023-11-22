# PostgreSQL Index Maintenance Script

## Description
This script automates the maintenance of PostgreSQL database indexes. It connects to a PostgreSQL database server, iterates through the databases, and performs index maintenance tasks. These tasks include checking for corrupted indexes, recreating indexes concurrently, and reporting on reclaimed storage space.

## Features
- Connects to PostgreSQL server
- Iterates through databases based on provided patterns
- Checks and reports invalid or corrupted indexes
- Recreates indexes concurrently
- Calculates and reports reclaimed storage space

## Requirements
- Python 3.x
- PostgreSQL server access
- Python libraries: psycopg2, typer

## Installation
Install required Python libraries using:
```bash
pip install psycopg2 typer
```

## Configuration
Set the necessary database connection details either as environment variables or command-line arguments:

Hostname (--host)
Port (--port)
Database pattern (--dbpattern)
Offset (--offset)
Limit (--limit)
Database user (--user)
Database password (--password)

## Usage
Run the script using the following command, replacing the placeholders with your database details:

```bash
python index_maintenance.py --host <host> --port <port> --dbpattern '%' --offset 0 --limit 10 --user <username> --password <password>
```

## Contributing
Contributions are welcome. Please fork the project, make your changes, and submit a pull request for review.

## License
Beerware