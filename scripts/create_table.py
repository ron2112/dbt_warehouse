import psycopg
import os

def execute_sql_script(script_name):
    # Using a placeholder for your Neon URL
    DATABASE_URL = "postgresql://postgres:abc123@localhost:5432/dbt_warehouse"

    script_path = os.path.join("..", "ddl_queries", script_name)

    try:
        # In psycopg 3, the connection context manager handles transactions
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                print(f"Reading script: {script_path}")
                with open(script_path, "r") as file:
                    sql_script = file.read()
                    print(f"Sql script to run : {sql_script}\n\n")

                print("Executing SQL...")
                cur.execute(sql_script)
                # No need for conn.commit() here; it's implicit in psycopg3 'with'
                
        print("Successfully executed script.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    execute_sql_script("create_tables.sql")