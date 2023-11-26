import typer
import psycopg2
import sys
from typing import List, Optional

app = typer.Typer()

def connect_to_db(host: str, port: int, dbname: str, user: str, password: str):
    try:
        return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    except psycopg2.Error as e:
        typer.echo(f"Error connecting to PostgreSQL database: {e}")
        raise typer.Exit(code=1)

def get_size_of_index(cursor, index_name: str) -> int:
    """
    Function to get the size of an index.
    """
    cursor.execute(f"SELECT pg_relation_size('{index_name}');")
    size = cursor.fetchone()[0]
    return size

def get_large_indexes_names(conn, dbname: str, size_threshold: int) -> List[str]:
    """
    Function to get large indexes from the database.
    """
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT indexrelid::regclass::text
            FROM pg_stat_all_indexes
            JOIN pg_class ON pg_class.oid = indexrelid
            WHERE schemaname = %s
              AND pg_relation_size(indexrelid) > %s
        """, (dbname, size_threshold))
        return [index[0] for index in cursor.fetchall()]

def recreate_index(conn, index_name: str, dbname: str):
    """
    Function to recreate an index.
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(f"REINDEX INDEX CONCURRENTLY {index_name};")
        except psycopg2.Error as e:
            typer.echo(f"Error recreating index {index_name} on database {dbname}: {e}")
            return False
        return True

def get_databases(conn, pattern: str, offset: int, limit: int) -> List[str]:
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT datname
            FROM pg_database
            WHERE datname LIKE %s
              AND datname NOT IN ('postgres', 'template0', 'template1', 'rdsadmin')
            ORDER BY pg_database_size(datname) DESC
            OFFSET %s LIMIT %s
        """, (pattern, offset, limit))
        return [db[0] for db in cursor.fetchall()]

@app.command()
def index_maintenance(
    host: str = typer.Option(..., help="Postgres hostname"),
    port: int = typer.Option(..., help="Postgres port"),
    dbpattern: str = typer.Option(..., help="Postgres db pattern"),
    offset: int = typer.Option(0, help="Report offset starting from 0"),
    limit: int = typer.Option(10, help="Report limit"),
    user: str = typer.Option(..., help="Database user name"),
    password: str = typer.Option(..., help="Database user password", hide_input=True),
    size_threshold: int = typer.Option(1000000000, help="Size threshold for large indexes"),
    recreate_index_name: Optional[str] = typer.Option(None, help="Name of the index to be recreated")
):
    conn = connect_to_db(host, port, 'postgres', user, password)
    databases = get_databases(conn, dbpattern, offset, limit)

    for dbname in databases:
        typer.echo(f"Processing database: {dbname}")

        large_indexes = get_large_indexes_names(conn, dbname, size_threshold)
        typer.echo(f"Large indexes in {dbname}: {large_indexes}")

        if recreate_index_name:
            # Recreate a specific index if specified
            typer.echo(f"Recreating specific index: {recreate_index_name}")
            success = recreate_index(conn, recreate_index_name, dbname)
            if success:
                typer.echo(f"Index {recreate_index_name} recreated successfully.")
            else:
                typer.echo(f"Failed to recreate index {recreate_index_name}.")
        else:
            # Recreate all large indexes
            for index_name in large_indexes:
                typer.echo(f"Recreating index: {index_name}")
                success = recreate_index(conn, index_name, dbname)
                if success:
                    typer.echo(f"Index {index_name} recreated successfully.")
                else:
                    typer.echo(f"Failed to recreate index {index_name}.")

    conn.close()

if __name__ == "__main__":
    app()
