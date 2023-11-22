import typer
import psycopg2
import sys
from typing import List

app = typer.Typer()

def connect_to_db(host: str, port: int, dbname: str, user: str, password: str):
    try:
        return psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    except psycopg2.Error as e:
        typer.echo(f"Error connecting to PostgreSQL database: {e}")
        raise typer.Exit(code=1)

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

def check_invalid_indexes(conn, dbname: str):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT indexrelid::regclass::text, indrelid::regclass::text
            FROM pg_index 
            JOIN pg_class ON pg_class.oid = pg_index.indexrelid 
            WHERE indisvalid = false AND pg_class.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
        """, (dbname,))
        return cursor.fetchall()

def recreate_index(conn, index_name: str):
    with conn.cursor() as cursor:
        cursor.execute(f"REINDEX INDEX CONCURRENTLY {index_name};")

@app.command()
def index_maintenance(
    host: str = typer.Option(..., help="Postgres hostname"),
    port: int = typer.Option(..., help="Postgres port"),
    dbpattern: str = typer.Option(..., help="Postgres db pattern"),
    offset: int = typer.Option(0, help="Report offset starting from 0"),
    limit: int = typer.Option(10, help="Report limit"),
    user: str = typer.Option(..., help="Database user name"),
    password: str = typer.Option(..., help="Database user password", hide_input=True)
):
    conn = connect_to_db(host, port, 'postgres', user, password)
    databases = get_databases(conn, dbpattern, offset, limit)

    for dbname in databases:
        typer.echo(f"Processing database: {dbname}")

        invalid_indexes = check_invalid_indexes(conn, dbname)
        if invalid_indexes:
            typer.echo(f"Invalid indexes found in {dbname}: {invalid_indexes}")
            sys.exit(2)

    conn.close()

if __name__ == "__main__":
    app()
