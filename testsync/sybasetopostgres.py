import sybpydb
import psycopg2
import psycopg2.extras
import logging

class SybasePostgresSync:
    def __init__(self, sybase_config, postgres_config, table_name, key_column, update_column):
        self.sybase_config = sybase_config
        self.postgres_config = postgres_config
        self.table_name = table_name
        self.key_column = key_column
        self.update_column = update_column
        
        self.sybase_conn = None
        self.postgres_conn = None
        self.last_sync_time = None
        self._setup_logging()
    
    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    def connect_sybase(self):
        try:
            self.sybase_conn = sybpydb.connect(**self.sybase_config)
            logging.info("Connected to Sybase ASE")
        except Exception as e:
            logging.error(f"Sybase connection failed: {e}")
            raise
    
    def connect_postgres(self):
        try:
            self.postgres_conn = psycopg2.connect(**self.postgres_config)
            logging.info("Connected to PostgreSQL")
        except Exception as e:
            logging.error(f"PostgreSQL connection failed: {e}")
            raise
    
    def fetch_last_sync_time(self):
        query = f"SELECT MAX({self.update_column}) FROM {self.table_name}"
        try:
            cursor = self.postgres_conn.cursor()
            cursor.execute(query)
            self.last_sync_time = cursor.fetchone()[0]
            cursor.close()
        except Exception as e:
            logging.error(f"Error fetching last sync time: {e}")
    
    def fetch_sybase_data(self):
        query = f"SELECT * FROM {self.table_name}"
        if self.last_sync_time:
            query += f" WHERE {self.update_column} > '{self.last_sync_time}'"
        try:
            cursor = self.sybase_conn.cursor()
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            data = cursor.fetchall()
            cursor.close()
            return columns, data
        except Exception as e:
            logging.error(f"Error fetching data from Sybase: {e}")
            return None, None
    
    def fetch_deleted_rows(self):
        query = f"SELECT {self.key_column} FROM deleted_rows_table WHERE table_name = '{self.table_name}'"
        try:
            cursor = self.sybase_conn.cursor()
            cursor.execute(query)
            deleted_keys = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return deleted_keys
        except Exception as e:
            logging.error(f"Error fetching deleted rows from Sybase: {e}")
            return []
    
    def apply_deletes_to_postgres(self, deleted_keys):
        if not deleted_keys:
            return
        
        query = f"DELETE FROM {self.table_name} WHERE {self.key_column} = ANY(%s)"
        try:
            cursor = self.postgres_conn.cursor()
            cursor.execute(query, (deleted_keys,))
            self.postgres_conn.commit()
            cursor.close()
            logging.info("Deleted rows synchronized successfully.")
        except Exception as e:
            self.postgres_conn.rollback()
            logging.error(f"Error applying deletes to PostgreSQL: {e}")
    
    def sync_to_postgres(self):
        if not self.sybase_conn or not self.postgres_conn:
            logging.error("Database connections are not established.")
            return
        
        self.fetch_last_sync_time()
        deleted_keys = self.fetch_deleted_rows()
        self.apply_deletes_to_postgres(deleted_keys)
        
        columns, data = self.fetch_sybase_data()
        if not data:
            logging.info("No new or updated data to sync.")
            return
        
        placeholders = ', '.join(['%s'] * len(columns))
        update_stmt = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != self.key_column])
        insert_query = f"""
            INSERT INTO {self.table_name} ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT ({self.key_column}) DO UPDATE
            SET {update_stmt}
        """
        
        try:
            cursor = self.postgres_conn.cursor()
            psycopg2.extras.execute_batch(cursor, insert_query, data)
            self.postgres_conn.commit()
            cursor.close()
            logging.info("Inserted/Updated rows synchronized successfully.")
        except Exception as e:
            self.postgres_conn.rollback()
            logging.error(f"Error syncing data to PostgreSQL: {e}")
    
    def close_connections(self):
        if self.sybase_conn:
            self.sybase_conn.close()
            logging.info("Sybase connection closed.")
        if self.postgres_conn:
            self.postgres_conn.close()
            logging.info("PostgreSQL connection closed.")
    
    def sync(self):
        try:
            self.connect_sybase()
            self.connect_postgres()
            self.sync_to_postgres()
        finally:
            self.close_connections()

# Example usage:
if __name__ == "__main__":
    sybase_config = {
        'server': 'SYBASE_SERVER',
        'database': 'SYBASE_DB',
        'user': 'USERNAME',
        'password': 'PASSWORD'
    }
    
    postgres_config = {
        'host': 'POSTGRES_HOST',
        'database': 'POSTGRES_DB',
        'user': 'USERNAME',
        'password': 'PASSWORD'
    }
    
    sync = SybasePostgresSync(sybase_config, postgres_config, 'your_table', 'primary_key_column', 'update_date')
    sync.sync()

