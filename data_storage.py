import os
from typing import Dict, Any
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import json
import sqlite3
import psycopg2
from mysql.connector import connect as mysql_connect, pooling
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Configuration Class for Modular Configuration
class Config:
    def __init__(self):
        load_dotenv(".env")
        self.MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
        self.HTTP_TIMEOUT = int(os.getenv('HTTP_TIMEOUT', '10'))
        self.ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')
        self.MAX_CONCURRENT_DOWNLOADS = int(os.getenv('MAX_CONCURRENT_DOWNLOADS', '2'))
        self.PDF_PAGE_SIZE = os.getenv('PDF_PAGE_SIZE', 'letter').lower()
        if self.PDF_PAGE_SIZE == 'a4':
            self.PDF_PAGE_SIZE = (595.276, 841.89)  # A4 size in points
        else:  # default to letter
            self.PDF_PAGE_SIZE = (self.PDF_PAGE_SIZE[0], self.PDF_PAGE_SIZE[1])  # letter size in points
        self.PDF_CREATION_TIMEOUT = int(os.getenv('PDF_CREATION_TIMEOUT', '60'))  # seconds

    def ensure_env_variables(self):
        env_file = ".env"
        if not os.path.exists(env_file):
            with open(env_file, 'w') as f:
                f.write(f"MAX_RETRIES=3\n")
                f.write(f"HTTP_TIMEOUT=10\n")
                f.write(f"MAX_CONCURRENT_DOWNLOADS=2\n")
                f.write(f"PDF_PAGE_SIZE=letter\n")
                f.write(f"PDF_CREATION_TIMEOUT=60\n")
            load_dotenv(env_file)

        for var, value in [
            ('MAX_RETRIES', '3'),
            ('HTTP_TIMEOUT', '10'),
            ('MAX_CONCURRENT_DOWNLOADS', '2'),
            ('PDF_PAGE_SIZE', 'letter'),
            ('PDF_CREATION_TIMEOUT', '60'),
            ('ENCRYPTION_KEY', Fernet.generate_key().decode())
        ]:
            if not os.environ.get(var):
                with open(env_file, 'a') as f:
                    f.write(f"{var}={value}\n")
                load_dotenv(env_file)


config = Config()
config.ensure_env_variables()


class DataStorage:
    """
    Manages storage, retrieval, and manipulation of application data across various database types (SQLite, MySQL, PostgreSQL) with advanced features like connection pooling, migrations,
    archiving, purging, audit logging, and more.
    """

    def __init__(self):
        """
        Initialize the DataStorage with connection pooling, database initialization, and indexes.
        """
        self.db_type = os.environ.get('DATABASE_TYPE', 'sqlite')
        self.connection_pool = self._setup_connection_pool()
        self._initialize_database()
        self._create_indexes()

    def _setup_connection_pool(self):
        """
        Set up a connection pool based on the database type.

        Returns:
            A connection pool or single connection object.
        """
        db_name = os.environ.get('DB_NAME', 'mangadex_data')
        db_user = os.environ.get('DB_USER', 'admin')
        db_password = os.environ.get('DB_PASSWORD', 'default_password')
        db_host = os.environ.get('DB_HOST', 'localhost')
        db_port = os.environ.get('DB_PORT', '5432')  # Default PostgreSQL port

        if self.db_type == 'sqlite':
            return sqlite3.connect(f"{db_name}.db")
        elif self.db_type == 'mysql':
            pool = mysql_connect(
                pool_name="mypool",
                pool_size=5,
                user=db_user,
                password=db_password,
                host=db_host,
                database=db_name,
                port=int(db_port)
            )
            return pool
        elif self.db_type == 'postgres':
            return psycopg2.pool.SimpleConnectionPool(1, 5,
                                                      dbname=db_name,
                                                      user=db_user,
                                                      password=db_password,
                                                      host=db_host,
                                                      port=db_port)
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def _get_connection(self):
        """
        Get a connection from the pool.

        Returns:
            A database connection.
        """
        if self.db_type == 'sqlite':
            return self.connection_pool
        elif self.db_type == 'mysql':
            return self.connection_pool.get_connection()
        elif self.db_type == 'postgres':
            return self.connection_pool.getconn()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def _return_connection(self, connection):
        """
        Return a connection
    """
        if self.db_type in ['mysql', 'postgres']:
            if self.db_type == 'mysql':
                connection.close()
            else:
                self.connection_pool.putconn(connection)

    def _execute_query(self, query: str, params: Any = (), many: bool = False):
        """
        Execute a SQL query with error handling and connection management.

        Args:
            query (str): SQL query to execute.
            params (Any): Parameters for the query.
            many (bool): If True, execute multiple inserts or updates.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            logger.info(f"Executing query: {query} with params: {params}")
            if many:
                cursor.executemany(query, params)
            else:
                cursor.execute(query, params)
            conn.commit()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            self._return_connection(conn)


    def _initialize_database(self):
        """
        Initialize the database schema with tables for data, versioning, and archiving.
        """
        self._execute_query('''CREATE TABLE IF NOT EXISTS db_version (version TEXT PRIMARY KEY)''')
        self._execute_query(
            '''CREATE TABLE IF NOT EXISTS audit_log (id INTEGER PRIMARY KEY, action TEXT, details TEXT, timestamp INTEGER)''')
        self._execute_query(
            '''CREATE TABLE IF NOT EXISTS archived_manga (manga_id TEXT PRIMARY KEY, title TEXT, description TEXT, archived_at INTEGER)''')

        # Apply existing migrations if any
        self.apply_migrations()

        # Set initial version if not set
        if not self.get_db_version():
            self.set_db_version("0.0.1")

        # Other table creations as previously defined
        if self.db_type == 'sqlite':
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga (manga_id TEXT PRIMARY KEY, title TEXT, description TEXT, last_chapter TEXT, is_deleted BOOLEAN DEFAULT FALSE)''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS authors (author_id TEXT PRIMARY KEY, name TEXT)''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS artists (artist_id TEXT PRIMARY KEY, name TEXT)''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga_authors (manga_id TEXT, author_id TEXT, FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (author_id) REFERENCES authors(author_id), PRIMARY KEY (manga_id, author_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga_artists (manga_id TEXT, artist_id TEXT, FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (artist_id) REFERENCES artists(artist_id), PRIMARY KEY (manga_id, artist_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS chapters (chapter_id TEXT PRIMARY KEY, manga_id TEXT, chapter_number REAL, volume TEXT, title TEXT, hash TEXT, is_deleted BOOLEAN DEFAULT FALSE, FOREIGN KEY (manga_id) REFERENCES manga(manga_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS files (app_uid TEXT, manga_id TEXT, file_path TEXT, PRIMARY KEY (app_uid, manga_id), FOREIGN KEY (manga_id) REFERENCES manga(manga_id))''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS credentials (username TEXT PRIMARY KEY, password TEXT)''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS tags (tag_id TEXT PRIMARY KEY, name TEXT)''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga_tags (manga_id TEXT, tag_id TEXT, FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (tag_id) REFERENCES tags(tag_id), PRIMARY KEY (manga_id, tag_id))''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS user_config (key TEXT PRIMARY KEY, value TEXT)''')
        # Add similar table creation queries for MySQL and PostgreSQL if needed


    def _create_indexes(self):
        """
        Create indexes on frequently queried columns for performance optimization.
        """
        if self.db_type == 'sqlite' or self.db_type == 'mysql':
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_manga_id ON manga(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_chapter_manga_id ON chapters(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_files_manga_id ON files(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_manga_tags_manga_id ON manga_tags(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_manga_tags_tag_id ON manga_tags(tag_id)")
        elif self.db_type == 'postgres':
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_manga_id ON manga(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_chapter_manga_id ON chapters(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_files_manga_id ON files(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_manga_tags_manga_id ON manga_tags(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_manga_tags_tag_id ON manga_tags(tag_id)")


    def apply_migrations(self):
        """
        Apply SQL migrations from .sql files.
        """
        # Migration logic would go here
        pass


    def get_db_version(self) -> str:
        """
        Get the current database version.

        Returns:
            str: The current database version or "0.0.0" if not set.
        """
        cursor = self._get_connection().cursor()
        cursor.execute("SELECT version FROM db_version")
        result = cursor.fetchone()
        return result[0] if result else "0.0.0"


    def set_db_version(self, version: str):
        """
        Set the database version.

        Args:
            version (str): The version to set.
        """
        self._execute_query("INSERT OR REPLACE INTO db_version (version) VALUES (?)", (version,))


    def store_file(self, app_uid: str, manga_id: str, file_path: str):
        """
        Store information about a downloaded file.

        Args:
            app_uid (str): The application's unique identifier.
            manga_id (str): The ID of the manga.
            file_path (str): The path to the stored file.
        """
        self._execute_query(
            "INSERT OR REPLACE INTO files (app_uid, manga_id, file_path) VALUES (?, ?, ?)",
            (app_uid, manga_id, file_path)
        )
        logger.info(f"Stored file information for manga_id: {manga_id}")


    def initialize_user_config(self):
        """
        Initialize the user config table if it doesn't exist.
        """
        self._execute_query('''CREATE TABLE IF NOT EXISTS user_config (key TEXT PRIMARY KEY, value TEXT)''')


    def get_user_config(self) -> Dict[str, str]:
        """
        Retrieve user configuration from the database.

        Returns:
            Dict[str, str]: A dictionary of user configurations.
        """
        self.initialize_user_config()
        cursor = self._get_connection().cursor()
        cursor.execute("SELECT key, value FROM user_config")
        config = {key: json.loads(value) for key, value in cursor.fetchall()}
        return config


    def save_user_config(self, config_data: Dict[str, Any]):
        """
        Save user configuration to the database.

        Args:
            config_data (Dict[str, Any]): Dictionary of configuration data to save.
        """
        self.initialize_user_config()
        cursor = self._get_connection().cursor()
        for key, value in config_data.items():
            cursor.execute("INSERT OR REPLACE INTO user_config (key, value) VALUES (?, ?)",
                           (key, json.dumps(value)))
        self._get_connection().commit()


    def __del__(self):
        """
        Cleanup method to cleanup connections
        """
        try:
            if self.db_type == 'sqlite':
                self.connection_pool.close()
            elif self.db_type in ['mysql', 'postgres']:
                self.connection_pool.closeall()
        except AttributeError:
            pass  # Handle case where object was never fully initialized


    def health_check(self) -> bool:
        """
        Perform a health check on the database connection.

        Returns:
            bool: True if the database is healthy, False otherwise.
        """
        try:
            self._execute_query("SELECT 1")
            return True
        except Exception:
            return False