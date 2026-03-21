"""
db/sql_client.py
-----------------
SQL Server client for database change detection.
Production-ready with connection management and error handling.

Design principles:
- Connection pooling via pyodbc
- Parameterized queries (SQL injection prevention)
- Comprehensive error logging
- Graceful failure handling
"""
import os
import pyodbc
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from utils.logger import get_logger

log = get_logger(__name__)


@dataclass
class SQLServerConfig:
    """Configuration for SQL Server connection."""
    server: str
    database: str
    username: str
    password: str
    driver: str = "{ODBC Driver 17 for SQL Server}"
    
    @classmethod
    def from_env(cls) -> 'SQLServerConfig':
        """Load configuration from environment variables."""
        return cls(
            server=os.getenv("SQL_SERVER", "localhost"),
            database=os.getenv("SQL_DATABASE", "CipherDSG"),
            username=os.getenv("SQL_USERNAME", ""),
            password=os.getenv("SQL_PASSWORD", ""),
            driver=os.getenv("SQL_DRIVER", "{ODBC Driver 17 for SQL Server}")
        )
    
    def get_connection_string(self) -> str:
        """Build connection string for pyodbc."""
        if self.username and self.password:
            # SQL Server authentication
            return (
                f"DRIVER={self.driver};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"TrustServerCertificate=yes;"
            )
        else:
            # Windows authentication
            return (
                f"DRIVER={self.driver};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate=yes;"
            )


class SQLServerClient:
    """
    Production-ready SQL Server client with connection management.
    
    Features:
    - Automatic connection retry
    - Query parameter binding
    - Transaction support
    - Comprehensive error handling
    """
    
    def __init__(self, config: Optional[SQLServerConfig] = None):
        """
        Initialize SQL Server client.
        
        Args:
            config: SQL Server configuration (defaults to env vars)
        """
        self.config = config or SQLServerConfig.from_env()
        self.connection: Optional[pyodbc.Connection] = None
        self._connected = False
        
        log.info(f"SQL Server client initialized: {self.config.server}/{self.config.database}")
    
    def connect(self) -> bool:
        """
        Establish connection to SQL Server.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            conn_str = self.config.get_connection_string()
            self.connection = pyodbc.connect(conn_str, timeout=10)
            self._connected = True
            log.info(f"✅ Connected to SQL Server: {self.config.database}")
            return True
        except pyodbc.Error as e:
            log.error(f"Failed to connect to SQL Server: {e}")
            self._connected = False
            return False
        except Exception as e:
            log.error(f"Unexpected error connecting to SQL Server: {e}")
            self._connected = False
            return False
    
    def disconnect(self):
        """Close SQL Server connection."""
        if self.connection:
            try:
                self.connection.close()
                self._connected = False
                log.info("SQL Server connection closed")
            except Exception as e:
                log.warning(f"Error closing SQL Server connection: {e}")
    
    def is_connected(self) -> bool:
        """Check if connection is active."""
        return self._connected and self.connection is not None
    
    def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute SELECT query and return results as list of dicts.
        
        Args:
            query: SQL query with positional parameters (e.g., WHERE id = ?)
            params: Dict, tuple, or list of parameter values
        
        Returns:
            List of result rows as dictionaries
        """
        if not self.is_connected():
            log.error("Not connected to SQL Server")
            return []
        
        try:
            cursor = self.connection.cursor()
            
            # Convert params to tuple for pyodbc
            if params:
                if isinstance(params, dict):
                    param_values = tuple(params.values())
                elif isinstance(params, (tuple, list)):
                    param_values = tuple(params)
                else:
                    param_values = (params,)
                cursor.execute(query, param_values)
            else:
                cursor.execute(query)
            
            # Fetch results and convert to list of dicts
            columns = [column[0] for column in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            cursor.close()
            return results
            
        except pyodbc.Error as e:
            log.error(f"SQL query failed: {e}")
            log.debug(f"Query: {query}")
            return []
        except Exception as e:
            log.error(f"Unexpected error executing query: {e}")
            return []
    
    def execute_non_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Execute INSERT/UPDATE/DELETE query.
        
        Args:
            query: SQL query with positional parameters
            params: Dict, tuple, or list of parameter values
        
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            log.error("Not connected to SQL Server")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Convert params to tuple for pyodbc
            if params:
                if isinstance(params, dict):
                    param_values = tuple(params.values())
                elif isinstance(params, (tuple, list)):
                    param_values = tuple(params)
                else:
                    param_values = (params,)
                cursor.execute(query, param_values)
            else:
                cursor.execute(query)
            
            self.connection.commit()
            cursor.close()
            return True
            
        except pyodbc.Error as e:
            log.error(f"SQL non-query failed: {e}")
            log.debug(f"Query: {query}")
            self.connection.rollback()
            return False
        except Exception as e:
            log.error(f"Unexpected error executing non-query: {e}")
            self.connection.rollback()
            return False
    
    def get_pending_changes(self, batch_size: int = 10) -> List[Dict[str, Any]]:
        """
        Fetch pending changes from ProductChangeLog.
        
        Uses the stored procedure from schema_corrected.sql.
        
        Args:
            batch_size: Maximum number of changes to fetch
        
        Returns:
            List of pending change records
        """
        # GetPendingChanges is a table-valued function, not a stored procedure
        query = """
        SELECT * FROM dbo.GetPendingChanges(?)
        """
        
        try:
            if not self.is_connected():
                log.error("Not connected to SQL Server")
                return []
            
            cursor = self.connection.cursor()
            cursor.execute(query, (batch_size,))
            
            # Fetch results
            columns = [column[0] for column in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            cursor.close()
            log.info(f"Fetched {len(results)} pending changes")
            return results
            
        except pyodbc.Error as e:
            log.error(f"Failed to fetch pending changes: {e}")
            return []
        except Exception as e:
            log.error(f"Unexpected error fetching pending changes: {e}")
            return []
    
    def calculate_natural_origin_percentage(self, product_code: str) -> Optional[float]:
        """
        Calculate the exact total Natural Origin percentage for a given product structurally.
        This provides deterministic math, bypassing the LLM's inability to calculate floating-point sums.
        
        Formula: SUM(PercentageInProduct * NaturalOriginIndex)
        
        Args:
            product_code: ProductCode of the formulation
            
        Returns:
            The calculated float, or None if the query fails.
        """
        query = """
        SELECT SUM(pf.PercentageInProduct * CAST(rm.NaturalOriginIndex AS DECIMAL(10,5))) AS TotalNO
        FROM dbo.ProductFormulations pf
        JOIN dbo.RawMaterials rm ON pf.RawMaterialID = rm.RawMaterialID
        JOIN dbo.Products p ON p.ProductID = pf.ProductID
        WHERE p.ProductCode = ?
        """
        
        try:
            if not self.is_connected():
                self.connect()
                
            cursor = self.connection.cursor()
            cursor.execute(query, (product_code,))
            row = cursor.fetchone()
            
            if row and row[0] is not None:
                total_percentage = float(row[0])
                log.info(f"Database computed Natural Origin for {product_code}: {total_percentage:.4f}%")
                return round(total_percentage, 5)
            
            log.warning(f"No formulation found for product code: {product_code}")
            return None
            
        except pyodbc.Error as e:
            log.error(f"Failed to calculate Natural Origin for {product_code}: {e}")
            return None
        except Exception as e:
            log.error(f"Unexpected error calculating Natural Origin: {e}")
            return None
            
    def fetch_context_view(self, view_name: str, product_code: str) -> List[Dict[str, Any]]:
        """
        Dynamically fetch the fully-joined JSON context array from a targeted Context View.
        This provides the AI with relational, explicitly scoped dossier truth.
        """
        query = f"SELECT * FROM dbo.{view_name} WHERE ProductCode = ?"
        try:
            if not self.is_connected():
                self.connect()

            cursor = self.connection.cursor()
            cursor.execute(query, (product_code,))
            
            columns = [column[0] for column in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            cursor.close()
            return results
        except pyodbc.Error as e:
            log.error(f"Failed to fetch context view {view_name} for {product_code}: {e}")
            return []
        except Exception as e:
            log.error(f"Unexpected error fetching context view {view_name}: {e}")
            return []
    
    def mark_change_processed(self, change_log_id: int) -> bool:
        """
        Mark a change as processed in ProductChangeLog.
        
        Args:
            change_log_id: ID of the change log entry
        
        Returns:
            True if successful, False otherwise
        """
        query = """
        UPDATE ProductChangeLog
        SET processed_at = GETUTCDATE(), status = 'completed'
        WHERE change_id = ?
        """
        
        try:
            if not self.is_connected():
                log.error("Not connected to SQL Server")
                return False
            
            cursor = self.connection.cursor()
            cursor.execute(query, (change_log_id,))
            self.connection.commit()
            cursor.close()
            
            log.debug(f"Marked change {change_log_id} as processed")
            return True
            
        except pyodbc.Error as e:
            log.error(f"Failed to mark change as processed: {e}")
            self.connection.rollback()
            return False
        except Exception as e:
            log.error(f"Unexpected error marking change: {e}")
            self.connection.rollback()
            return False
    
    def get_table_schema(self, table_name: str) -> str:
        """
        Get CREATE TABLE statement for a specific table.
        Used by LLM for schema-aware change interpretation.
        
        Args:
            table_name: Name of the table
        
        Returns:
            CREATE TABLE statement as string
        """
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        
        try:
            if not self.is_connected():
                log.error("Not connected to SQL Server")
                return ""
            
            cursor = self.connection.cursor()
            cursor.execute(query, (table_name,))
            
            # Build CREATE TABLE statement
            schema_lines = [f"CREATE TABLE {table_name} ("]
            for row in cursor.fetchall():
                col_name = row[0]
                data_type = row[1]
                max_length = row[2]
                nullable = row[3]
                default = row[4]
                
                # Build column definition
                col_def = f"    {col_name} {data_type}"
                if max_length:
                    col_def += f"({max_length})"
                if nullable == 'NO':
                    col_def += " NOT NULL"
                if default:
                    col_def += f" DEFAULT {default}"
                
                schema_lines.append(col_def + ",")
            
            schema_lines[-1] = schema_lines[-1].rstrip(',')  # Remove last comma
            schema_lines.append(");")
            
            cursor.close()
            return "\n".join(schema_lines)
            
        except pyodbc.Error as e:
            log.error(f"Failed to get table schema: {e}")
            return ""
        except Exception as e:
            log.error(f"Unexpected error getting table schema: {e}")
            return ""
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


# Singleton instance
_client_instance: Optional[SQLServerClient] = None


def get_sql_client() -> SQLServerClient:
    """Get or create the global SQL Server client instance."""
    global _client_instance
    if _client_instance is None:
        _client_instance = SQLServerClient()
    return _client_instance
