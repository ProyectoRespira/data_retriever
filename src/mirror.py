from src.database import create_postgres_session, create_mysql_session, get_last_mirrored_id, update_last_mirrored_id, get_tables

def mirror_table(table_name):
    # Get the last mirrored ID for this table
    last_mirrored_id = get_last_mirrored_id(table_name)

    # Create sessions for PostgreSQL and MySQL
    postgres_session = create_postgres_session()
    mysql_session = create_mysql_session()

    try:
        # Get data from MySQL with IDs greater than the last mirrored ID
        mysql_data = mysql_session.execute(f"SELECT * FROM {table_name} WHERE id > :last_id", {'last_id': last_mirrored_id}).fetchall()

        # Insert data into PostgreSQL
        for row in mysql_data:
            postgres_session.execute(f"INSERT INTO {table_name} VALUES :values", {'values': row})

        # Update the last mirrored ID for this table
        update_last_mirrored_id(table_name, max(row.id for row in mysql_data))

        postgres_session.commit()
        print(f"Data mirrored for table {table_name} successfully!")
    except Exception as e:
        print(f"Error mirroring table {table_name}:", e)
        postgres_session.rollback()
    finally:
        postgres_session.close()
        mysql_session.close()

def start_mirror_process():
    # Call mirror_table function for each table you want to mirror
    tables = get_tables()
    print('getting tables...')
    for table in tables:
        mirror_table(table)

if __name__ == "__main__":
    start_mirror_process()
    
