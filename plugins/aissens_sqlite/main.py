import asyncio
import sqlite3
from typing import Any

from plugins.interface import PluginInterface


class AissensSqlite(PluginInterface):
    """AISSENS with sqlite plugin for OPC UA server"""

    def __init__(self, config: dict):
        """
        Initialize SQLite database connection

        Args:
            config: Configuration dictionary containing database settings

        Schema:
            - id: INTEGER PRIMARY KEY
            - timestamp: TEXT (ISO format string)
            - name: TEXT
            - json_value: TEXT (JSON string)
        """
        self.database_path = config["database_path"]
        if not isinstance(self.database_path, str):
            raise ValueError("database_path must be a string")
        if not self.database_path:
            raise ValueError("database_path must be specified in config")

    def _connect(self):
        """Establish database connection"""
        try:
            self.conn = sqlite3.connect(self.database_path)
            self.conn.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            raise

    async def get_value(self, namespace: str, variable_name: str) -> Any:
        """
        Get latest value for a variable from the database

        Args:
            namespace: The namespace name
            variable_name: The variable name within the namespace

        Returns:
            Any: The value from the database

        The method expects a table structure like:
        CREATE TABLE measurements (
            id INTEGER PRIMARY KEY,
            namespace TEXT,
            variable TEXT,
            value REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
        try:
            if not self.conn:
                self._connect()

            if self.conn is None:
                return None

            # Run database query in thread pool to avoid blocking
            query = """
                SELECT value 
                FROM measurements 
                WHERE namespace = ? AND variable = ?
                ORDER BY timestamp DESC 
                LIMIT 1
            """

            loop = asyncio.get_running_loop()
            value = await loop.run_in_executor(
                None,
                lambda: self.conn.execute(query, (namespace, variable_name)).fetchone()
                if self.conn
                else None,
            )

            if value:
                return value[0]
            return None

        except sqlite3.Error as e:
            print(f"Database error: {e}")
            # Attempt to reconnect on error
            self._connect()
            return None
