import json
import os
import uuid
import time
import glob
from typing import Dict, List, Any, Optional, Callable
from cryptography.fernet import Fernet
from dotenv import load_dotenv
import sqlite3
import psycopg2
from mysql.connector import connect as mysql_connect, pooling
import logging
from cachetools import TTLCache
import threading

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Check for .env file and handle environment variables
env_file = ".env"
if os.path.exists(env_file):
    load_dotenv(env_file)
else:
    with open(env_file, 'w') as f:
        f.write(f"ENCRYPTION_KEY={Fernet.generate_key().decode()}\n")
        f.write(f"DATABASE_TYPE=sqlite\n")
        f.write(f"DB_NAME=mangadex_data\n")
        f.write(f"DB_USER=admin\n")
        f.write(f"DB_PASSWORD=default_password\n")
        f.write(f"DB_HOST=localhost\n")
        f.write(f"DB_PORT=5432\n")
    load_dotenv(env_file)

# Ensure all necessary environment variables are set
if not os.environ.get('ENCRYPTION_KEY'):
    with open(env_file, 'a') as f:
        f.write(f"ENCRYPTION_KEY={Fernet.generate_key().decode()}\n")
if not os.environ.get('DATABASE_TYPE'):
    with open(env_file, 'a') as f:
        f.write(f"DATABASE_TYPE=sqlite\n")
if not os.environ.get('DB_NAME'):
    with open(env_file, 'a') as f:
        f.write(f"DB_NAME=mangadex_data\n")
if not os.environ.get('DB_USER'):
    with open(env_file, 'a') as f:
        f.write(f"DB_USER=admin\n")
if not os.environ.get('DB_PASSWORD'):
    with open(env_file, 'a') as f:
        f.write(f"DB_PASSWORD=default_password\n")
if not os.environ.get('DB_HOST'):
    with open(env_file, 'a') as f:
        f.write(f"DB_HOST=localhost\n")
if not os.environ.get('DB_PORT'):
    with open(env_file, 'a') as f:
        f.write(f"DB_PORT=5432\n")

# Reload .env to ensure new additions are in environment
load_dotenv(env_file)

# Encryption setup
ENCRYPTION_KEY = os.environ.get('ENCRYPTION_KEY')
cipher = Fernet(ENCRYPTION_KEY.encode())

class DataStorage:
    """
    Manages storage and retrieval of application data with advanced features like connection pooling, migrations,
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
        self.manga_cache = TTLCache(maxsize=100, ttl=300)  # 5 minute TTL for manga cache
        self.lock = threading.Lock()  # For concurrency control

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
        Return a connection to the pool for MySQL and PostgreSQL.

        Args:
            connection: The connection object to return.
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
            start_time = time.time()
            if many:
                cursor.executemany(query, params)
            else:
                cursor.execute(query, params)
            conn.commit()
            end_time = time.time()
            logger.info(f"Query execution time: {end_time - start_time:.4f} seconds")
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
        self._execute_query('''CREATE TABLE IF NOT EXISTS audit_log (id INTEGER PRIMARY KEY, action TEXT, details TEXT, timestamp INTEGER)''')
        self._execute_query('''CREATE TABLE IF NOT EXISTS archived_manga (manga_id TEXT PRIMARY KEY, title TEXT, description TEXT, archived_at INTEGER)''')

        # Apply existing migrations if any
        self.apply_migrations()

        # Set initial version if not set
        if not self.get_db_version():
            self.set_db_version("0.0.1")

        # Other table creations as previously defined
        if self.db_type == 'sqlite':
            self._execute_query('''CREATE TABLE IF NOT EXISTS manga (manga_id TEXT PRIMARY KEY, title TEXT, description TEXT, last_chapter TEXT, is_deleted BOOLEAN DEFAULT FALSE)''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS authors (author_id TEXT PRIMARY KEY, name TEXT)''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS artists (artist_id TEXT PRIMARY KEY, name TEXT)''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS manga_authors (manga_id TEXT, author_id TEXT, FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (author_id) REFERENCES authors(author_id), PRIMARY KEY (manga_id, author_id))''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS manga_artists (manga_id TEXT, artist_id TEXT, FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (artist_id) REFERENCES artists(artist_id), PRIMARY KEY (manga_id, artist_id))''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS chapters (chapter_id TEXT PRIMARY KEY, manga_id TEXT, chapter_number REAL, volume TEXT, title TEXT, hash TEXT, is_deleted BOOLEAN DEFAULT FALSE, FOREIGN KEY (manga_id) REFERENCES manga(manga_id))''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS files (app_uid TEXT, manga_id TEXT, file_path TEXT, PRIMARY KEY (app_uid, manga_id), FOREIGN KEY (manga_id) REFERENCES manga(manga_id))''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS credentials (username TEXT PRIMARY KEY, password TEXT)''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS tags (tag_id TEXT PRIMARY KEY, name TEXT)''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga_tags (manga_id TEXT, tag_id TEXT, FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (tag_id) REFERENCES tags(tag_id), PRIMARY KEY (manga_id, tag_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS uuids (entity_type TEXT, entity_id TEXT, uuid_value TEXT, PRIMARY KEY (entity_type, entity_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS files (app_uid TEXT, manga_id TEXT, file_path TEXT, PRIMARY KEY (app_uid, manga_id), FOREIGN KEY (manga_id) REFERENCES manga(manga_id))''')
        elif self.db_type == 'mysql':
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga (manga_id VARCHAR(255) PRIMARY KEY, title TEXT, description TEXT, last_chapter TEXT, is_deleted BOOLEAN DEFAULT FALSE)''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS authors (author_id VARCHAR(255) PRIMARY KEY, name TEXT)''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR(255) PRIMARY KEY, name TEXT)''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga_authors (manga_id VARCHAR(255), author_id VARCHAR(255), FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (author_id) REFERENCES authors(author_id), PRIMARY KEY (manga_id, author_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga_artists (manga_id VARCHAR(255), artist_id VARCHAR(255), FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (artist_id) REFERENCES artists(artist_id), PRIMARY KEY (manga_id, artist_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS chapters (chapter_id VARCHAR(255) PRIMARY KEY, manga_id VARCHAR(255), chapter_number REAL, volume VARCHAR(255), title TEXT, hash VARCHAR(255), is_deleted BOOLEAN DEFAULT FALSE, FOREIGN KEY (manga_id) REFERENCES manga(manga_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS files (app_uid VARCHAR(255), manga_id VARCHAR(255), file_path TEXT, PRIMARY KEY (app_uid, manga_id), FOREIGN KEY (manga_id) REFERENCES manga(manga_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS credentials (username VARCHAR(255) PRIMARY KEY, password TEXT)''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS tags (tag_id VARCHAR(255) PRIMARY KEY, name TEXT)''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga_tags (manga_id VARCHAR(255), tag_id VARCHAR(255), FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (tag_id) REFERENCES tags(tag_id), PRIMARY KEY (manga_id, tag_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS uuids (entity_type VARCHAR(255), entity_id VARCHAR(255), uuid_value VARCHAR(255), PRIMARY KEY (entity_type, entity_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS files (app_uid VARCHAR(255), manga_id VARCHAR(255), file_path TEXT, PRIMARY KEY (app_uid, manga_id), FOREIGN KEY (manga_id) REFERENCES manga(manga_id))''')
        elif self.db_type == 'postgres':
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga (manga_id VARCHAR(255) PRIMARY KEY, title TEXT, description TEXT, last_chapter TEXT, is_deleted BOOLEAN DEFAULT FALSE)''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS authors (author_id VARCHAR(255) PRIMARY KEY, name TEXT)''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR(255) PRIMARY KEY, name TEXT)''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga_authors (manga_id VARCHAR(255), author_id VARCHAR(255), FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (author_id) REFERENCES authors(author_id), PRIMARY KEY (manga_id, author_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga_artists (manga_id VARCHAR(255), artist_id VARCHAR(255), FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (artist_id) REFERENCES artists(artist_id), PRIMARY KEY (manga_id, artist_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS chapters (chapter_id VARCHAR(255) PRIMARY KEY, manga_id VARCHAR(255), chapter_number REAL, volume TEXT, title TEXT, hash TEXT, is_deleted BOOLEAN DEFAULT FALSE, FOREIGN KEY (manga_id) REFERENCES manga(manga_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS files (app_uid VARCHAR(255), manga_id VARCHAR(255), file_path TEXT, PRIMARY KEY (app_uid, manga_id), FOREIGN KEY (manga_id) REFERENCES manga(manga_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS credentials (username VARCHAR(255) PRIMARY KEY, password TEXT)''')
            self._execute_query('''CREATE TABLE IF NOT EXISTS tags (tag_id VARCHAR(255) PRIMARY KEY, name TEXT)''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS manga_tags (manga_id VARCHAR(255), tag_id VARCHAR(255), FOREIGN KEY (manga_id) REFERENCES manga(manga_id), FOREIGN KEY (tag_id) REFERENCES tags(tag_id), PRIMARY KEY (manga_id, tag_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS uuids (entity_type VARCHAR(255), entity_id VARCHAR(255), uuid_value VARCHAR(255), PRIMARY KEY (entity_type, entity_id))''')
            self._execute_query(
                '''CREATE TABLE IF NOT EXISTS files (app_uid VARCHAR(255), manga_id VARCHAR(255), file_path TEXT, PRIMARY KEY (app_uid, manga_id), FOREIGN KEY (manga_id) REFERENCES manga(manga_id))''')

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
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_files_manga_id ON files(manga_id)")
        elif self.db_type == 'postgres':
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_manga_id ON manga(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_chapter_manga_id ON chapters(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_files_manga_id ON files(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_manga_tags_manga_id ON manga_tags(manga_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_manga_tags_tag_id ON manga_tags(tag_id)")
            self._execute_query("CREATE INDEX IF NOT EXISTS idx_files_manga_id ON files(manga_id)")

    def apply_migrations(self):
        """
        Apply SQL migrations from .sql files.
        """
        migrations = sorted(glob.glob("migrations/*.sql"))
        current_version = self.get_db_version()
        for migration in migrations:
            version = os.path.basename(migration).split('_')[0]
            if version > current_version:
                with open(migration, 'r') as f:
                    self._execute_query(f.read())
                self.set_db_version(version)

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

    def export_schema(self, output_file: str):
        """
        Export the schema of the database to a .sql file.

        Args:
            output_file (str): Path where to save the schema.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            with open(output_file, 'w') as f:
                if self.db_type == 'sqlite':
                    for line in conn.iterdump():
                        if line.startswith('CREATE TABLE'):
                            f.write(f"{line}\n")
                elif self.db_type == 'mysql':
                    cursor.execute("SHOW TABLES")
                    tables = cursor.fetchall()
                    for table in tables:
                        cursor.execute(f"SHOW CREATE TABLE {table[0]}")
                        create_table = cursor.fetchone()[1]
                        f.write(f"{create_table};\n\n")
                elif self.db_type == 'postgres':
                    cursor.execute("""
                        SELECT table_schema || '.' || table_name AS table_name
                        FROM information_schema.tables
                        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                        AND table_type = 'BASE TABLE'
                    """)
                    tables = cursor.fetchall()
                    for table in tables:
                        cursor.execute(f"SELECT pg_catalog.pg_get_ddl('table', '{table[0]}')")
                        schema = cursor.fetchone()[0]
                        f.write(f"{schema};\n\n")
        except Exception as e:
            logger.error(f"Error exporting schema: {e}")
        finally:
            cursor.close()
            self._return_connection(conn)


    def archive_manga(self, manga_id: str):
        """
        Archive manga data, moving it from the main table to an archive table.

        Args:
            manga_id (str): ID of the manga to archive.
        """
        with self.lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("""
                        INSERT INTO archived_manga (manga_id, title, description, archived_at)
                        SELECT manga_id, title, description, ? FROM manga WHERE manga_id = ?
                    """, (int(time.time()), manga_id))
                cursor.execute("DELETE FROM manga WHERE manga_id = ?", (manga_id,))
                conn.commit()
                logger.info(f"Archived manga with ID: {manga_id}")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to archive manga {manga_id}: {e}")
            finally:
                cursor.close()
                self._return_connection(conn)


    def purge_deleted_manga(self, days_old: int = 30):
        """
        Purge manga data that has been marked as deleted for a certain number of days.

        Args:
            days_old (int): Number of days after which to purge deleted manga.
        """
        with self.lock:
            if self.db_type == 'sqlite':
                self._execute_query("""
                        DELETE FROM manga 
                        WHERE is_deleted = TRUE AND date_modified < datetime('now', '-{} days')
                    """.format(days_old))
            elif self.db_type == 'mysql':
                self._execute_query("""
                        DELETE FROM manga 
                        WHERE is_deleted = TRUE AND date_modified < DATE_SUB(NOW(), INTERVAL {} DAY)
                    """.format(days_old))
            elif self.db_type == 'postgres':
                self._execute_query("""
                        DELETE FROM manga 
                        WHERE is_deleted = TRUE AND date_modified < CURRENT_DATE - INTERVAL '{} days'
                    """.format(days_old))


    def _log_action(self, action: str, details: Dict[str, Any]):
        """
        Log an action for audit purposes.

        Args:
            action (str): The action being logged.
            details (Dict[str, Any]): Additional details about the action.
        """
        self._execute_query("INSERT INTO audit_log (action, details, timestamp) VALUES (?, ?, ?)",
                            (action, json.dumps(details), int(time.time())))


    def store_manga(self, manga_id: str, title: str, description: str, last_chapter: str, authors: List[str],
                    artists: List[str], tags: List[str]):
        """
        Store manga data with normalization for authors, artists, and tags, with concurrency control.

        Args:
            manga_id (str): Unique identifier for the manga.
            title (str): Title of the manga.
            description (str): Manga's description.
            last_chapter (str): Last chapter number or identifier.
            authors (List[str]): List of author IDs.
            artists (List[str]): List of artist IDs.
            tags (List[str]): List of tag IDs.
        """
        with self.lock:
            self._execute_query(
                "INSERT OR REPLACE INTO manga (manga_id, title, description, last_chapter) VALUES (?, ?, ?, ?)",
                (manga_id, title, description, last_chapter))

            for author_id in authors:
                self._execute_query("INSERT OR IGNORE INTO authors (author_id, name) VALUES (?, ?)",
                                    (author_id, f"Author_{author_id}"))
                self._execute_query("INSERT OR REPLACE INTO manga_authors (manga_id, author_id) VALUES (?, ?)",
                                    (manga_id, author_id))

            for artist_id in artists:
                self._execute_query("INSERT OR IGNORE INTO artists (artist_id, name) VALUES (?, ?)",
                                    (artist_id, f"Artist_{artist_id}"))
                self._execute_query("INSERT OR REPLACE INTO manga_artists (manga_id, artist_id) VALUES (?, ?)",
                                    (manga_id, artist_id))

            for tag_id in tags:
                self._execute_query("INSERT OR IGNORE INTO tags (tag_id, name) VALUES (?, ?)", (tag_id, f"Tag_{tag_id}"))
                self._execute_query("INSERT OR REPLACE INTO manga_tags (manga_id, tag_id) VALUES (?, ?)",
                                    (manga_id, tag_id))

            # Clear cache for this manga to ensure fresh data on next access
            if manga_id in self.manga_cache:
                del self.manga_cache[manga_id]

        self._log_action("store_manga", {"manga_id": manga_id})


    def get_manga(self, manga_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve manga data, using cache if available.

        Args:
            manga_id (str): ID of the manga to retrieve.

        Returns:
            Optional[Dict[str, Any]]: Manga data or None if not found.
        """
        if manga_id in self.manga_cache:
            return dict(self.manga_cache[manga_id])

        with self.lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute("""
                        SELECT m.*, GROUP_CONCAT(DISTINCT a.author_id) AS authors, 
                               GROUP_CONCAT(DISTINCT art.artist_id) AS artists, 
                               GROUP_CONCAT(DISTINCT t.tag_id) AS tags
                        FROM manga m
                        LEFT JOIN manga_authors a ON m.manga_id = a.manga_id
                        LEFT JOIN manga_artists art ON m.manga_id = art.manga_id
                        LEFT JOIN manga_tags mt ON m.manga_id = mt.manga_id
                        LEFT JOIN tags t ON mt.tag_id = t.tag_id
                        WHERE m.manga_id = ? AND m.is_deleted = FALSE
                        GROUP BY m.manga_id
                    """, (manga_id,))
                manga = cursor.fetchone()

                if manga:
                    result = {
                        'manga_id': manga[0],
                        'title': manga[1],
                        'description': manga[2],
                        'last_chapter': manga[3],
                        'authors': manga[4].split(',') if manga[4] else [],
                        'artists': manga[5].split(',') if manga[5] else [],
                        'tags': manga[6].split(',') if manga[6] else []
                    }
                    self.manga_cache[manga_id] = result
                    return result
            except Exception as e:
                logger.error(f"Error fetching manga {manga_id}: {e}")
            finally:
                cursor.close()
                self._return_connection(conn)
        return None


    def check_data_consistency(self):
        """
        Perform checks to ensure data consistency across tables.
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if self.db_type == 'sqlite':
                # SQLite consistency checks
                cursor.execute("""
                        SELECT ma.manga_id FROM manga_authors ma 
                        LEFT JOIN manga m ON ma.manga_id = m.manga_id 
                        WHERE m.manga_id IS NULL
                        UNION
                        SELECT ma.manga_id FROM manga_artists ma 
                        LEFT JOIN manga m ON ma.manga_id = m.manga_id 
                        WHERE m.manga_id IS NULL
                    """)
                orphans = cursor.fetchall()
                if orphans:
                    logger.warning(f"Orphaned manga entries found: {orphans}")

                cursor.execute("""
                        SELECT mt.manga_id, mt.tag_id FROM manga_tags mt 
                        LEFT JOIN manga m ON mt.manga_id = m.manga_id 
                        LEFT JOIN tags t ON mt.tag_id = t.tag_id 
                        WHERE m.manga_id IS NULL OR t.tag_id IS NULL
                    """)
                inconsistent_tags = cursor.fetchall()
                if inconsistent_tags:
                    logger.warning(f"Inconsistent manga-tag relationships: {inconsistent_tags}")

            elif self.db_type == 'mysql':
                cursor.execute("""
                        SELECT ma.manga_id FROM manga_authors ma 
                        LEFT JOIN manga m ON ma.manga_id = m.manga_id 
                        WHERE m.manga_id IS NULL
                        UNION
                        SELECT ma.manga_id FROM manga_artists ma 
                        LEFT JOIN manga m ON ma.manga_id = m.manga_id 
                        WHERE m.manga_id IS NULL
                    """)
                orphans = cursor.fetchall()
                if orphans:
                    logger.warning(f"Orphaned manga entries found: {orphans}")

                cursor.execute("""
                        SELECT mt.manga_id, mt.tag_id FROM manga_tags mt 
                        LEFT JOIN manga m ON mt.manga_id = m.manga_id 
                        LEFT JOIN tags t ON mt.tag_id = t.tag_id 
                        WHERE m.manga_id IS NULL OR t.tag_id IS NULL
                    """)
                inconsistent_tags = cursor.fetchall()
                if inconsistent_tags:
                    logger.warning(f"Inconsistent manga-tag relationships: {inconsistent_tags}")

            elif self.db_type == 'postgres':
                cursor.execute("""
                        SELECT ma.manga_id FROM manga_authors ma 
                        LEFT JOIN manga m ON ma.manga_id = m.manga_id 
                        WHERE m.manga_id IS NULL
                        UNION
                        SELECT ma.manga_id FROM manga_artists ma 
                        LEFT JOIN manga m ON ma.manga_id = m.manga_id 
                        WHERE m.manga_id IS NULL
                    """)
                orphans = cursor.fetchall()
                if orphans:
                    logger.warning(f"Orphaned manga entries found: {orphans}")

                cursor.execute("""
                        SELECT mt.manga_id, mt.tag_id FROM manga_tags mt 
                        LEFT JOIN manga m ON mt.manga_id = m.manga_id 
                        LEFT JOIN tags t ON mt.tag_id = t.tag_id 
                        WHERE m.manga_id IS NULL OR t.tag_id IS NULL
                    """)
                inconsistent_tags = cursor.fetchall()
                if inconsistent_tags:
                    logger.warning(f"Inconsistent manga-tag relationships: {inconsistent_tags}")

        except Exception as e:
            logger.error(f"Data consistency check failed: {e}")
        finally:
            cursor.close()
            self._return_connection(conn)


    def monitor_performance(self):
        """
        Log statistics about database performance, like query count or average execution time.
        """
        # This method should be called periodically or on certain events
        # For demonstration, we'll just log some dummy data:
        logger.info("Performance Monitoring:")
        logger.info(f"Number of manga entries: {self.get_manga_count()}")
        logger.info(f"Cache hits: {len(self.manga_cache)}")
        # Here, you might want to collect more specific performance metrics


    def get_manga_count(self) -> int:
        """
        Get the count of manga entries in the database.

        Returns:
            int: Number of manga entries.
        """
        cursor = self._get_connection().cursor()
        cursor.execute("SELECT COUNT(*) FROM manga")
        count = cursor.fetchone()[0]
        cursor.close()
        return count


    def enhance_security(self):
        """
        Apply security enhancements like row-level security or encryption of data at rest.
        Note: This method includes examples for PostgreSQL. Adjust for other databases.
        """
        if self.db_type == 'postgres':
            try:
                # Example of row-level security in PostgreSQL
                self._execute_query("""
                        CREATE POLICY manga_access_policy ON manga
                        FOR ALL
                        USING (current_user = 'admin' OR manga_id = current_setting('app.user_manga_id')::text)
                    """)
                logger.info("Row-level security policy applied to manga table.")
            except Exception as e:
                logger.error(f"Failed to apply row-level security policy: {e}")

            # Placeholder for data encryption at rest - this would require additional setup
            logger.info(
                "Note: For encryption at rest, consider using database-level encryption features or third-party solutions.")

        elif self.db_type == 'mysql':
            # MySQL does not have built-in row-level security like PostgreSQL, but you can simulate it in the application layer
            logger.info("For MySQL, row-level security would need to be implemented at the application level.")

        elif self.db_type == 'sqlite':
            # SQLite doesn't support row-level security directly
            logger.info("SQLite does not support row-level security. Consider application-level controls.")


    def __del__(self):
        """
        Cleanup method to ensure connections are closed and resources are released.
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


if __name__ == "__main__":
    # Example usage or CLI for testing
    storage = DataStorage()

    # Example: Store manga
    storage.store_manga("manga123", "Manga Title", "This is a description", "10",
                        ["author1", "author2"], ["artist1"], ["tag1", "tag2"])

    # Example: Get manga - this will also test the cache functionality
    manga = storage.get_manga("manga123")
    print(f"Manga: {manga}")

    # Example: Health check
    if storage.health_check():
        print("Database is healthy")
    else:
        print("Database health check failed")

    # Example: Archive manga, purge old data, check consistency
    # storage.archive_manga("manga123")
    # storage.purge_deleted_manga(1)  # Purge deleted manga older than 1 day
    storage.check_data_consistency()
    storage.monitor_performance()
    storage.enhance_security()

    # Export schema (for SQLite, this will dump CREATE TABLE statements)
    storage.export_schema("schema.sql")