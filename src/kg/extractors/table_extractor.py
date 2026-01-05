import logging
from typing import List
from uuid import UUID
import psycopg2
from psycopg2.extras import RealDictCursor

from ..models import Table

logger = logging.getLogger(__name__)

class TableExtractor:
    """
        Extracts table metadata from PostgreSQL information_schema.
    """
    def __init__(self, connection):
        self.conn = connection
        
    def extract_tables(self, kg_id: UUID, schema_name: str = "public") -> List[Table]:
        """
            Extract all tables from source database.
        """
        logger.info(f"Extracting tables from schema '{schema_name}'")
        
        query = """
            SELECT
                table_name,
                table_type
            FROM information_schema.tables
            WHERE table_schema = %s
                AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        
        tables = []
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (schema_name,))
            rows = cur.fetchall()
            
            for row in rows:
                table_name = row['table_name']
                qualified_name = f"{schema_name}.{table_name}"
                
                # Get row count estimate
                row_count = self._get_row_count(table_name, schema_name)
                
                table = Table(
                    kg_id=kg_id,
                    table_name=table_name,
                    schema_name=schema_name,
                    qualified_name=qualified_name,
                    table_type=row['table_type'].lower(),
                    row_count_estimate=row_count
                )
                tables.append(table)
                
        logger.info(f"Extracted {len(tables)} tables")
        return tables
    
    def _get_row_count(self, table_name: str, schema_name: str) -> int:
        """
            Get estimated row count for a table from PostgreSQL's system catalog.
        """
        
        query = """
            SELECT reltuples::bigint AS estimate
            FROM pg_class
            WHERE oid = %s::regclass
        """
        
        qualified_name = f"{schema_name}.{table_name}"
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (qualified_name,))
                result = cur.fetchone()
                return result[0] if result else 0
                
        except Exception:
            # Fallback to exact count for small tables
            return self._get_exact_count(table_name, schema_name)
        
    def _get_exact_count(self, table_name: str, schema_name: str) -> int:
        """
           Get exact row count (fallback).
        """
        
        query = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
    
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchone()[0]
            
        except Exception:
            return 0
        