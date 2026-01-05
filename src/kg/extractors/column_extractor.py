import logging
from typing import List, Dict, Any
from uuid import UUID
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from pydantic import BaseModel

from ..models import Column, Table

class PIIDetectionOutput(BaseModel):
    is_pii: bool
    reasoning: str

logger = logging.getLogger(__name__)

class ColumnExtractor:
    """
        Extracts column metadata from PostgreSQL information_schema.
    """
    def __init__(self, connection, openai_client=None):
        self.conn = connection
        self.openai_client = openai_client
        
    def extract_columns(self, table: Table) -> List[Column]:
        """
            Extract all columns for a table.
        """
        logger.info(f"Extracting columns for table '{table.table_name}'")
        
        # Get column metadata
        columns_metadata = self._get_columns_metadata(table.table_name, table.schema_name)
        
        # Get primary keys
        primary_keys = self._get_primary_keys(table.table_name, table.schema_name)
        
        # Get unique constraints
        unique_columns = self._get_unique_columns(table.table_name, table.schema_name)
        
        # Get foreign keys
        foreign_keys = self._get_foreign_keys(table.table_name, table.schema_name)
        
        columns = []
        for col_meta in columns_metadata:
            column_name = col_meta['column_name']
            qualified_name = f"{table.table_name}.{column_name}"
            data_type = col_meta['data_type']
            
            # Sample data and statistics
            sample_values = self._get_sample_values(table.table_name, table.schema_name, column_name)
            stats = self._calculate_statistics(table.table_name, table.schema_name, column_name)
            
            # Detect PII
            is_pii = self._is_pii_column(column_name, data_type, sample_values)
            
            column = Column(
                table_id=table.table_id,
                column_name=column_name,
                qualified_name=qualified_name,
                data_type=col_meta['data_type'],
                is_nullable=col_meta['is_nullable'] == 'YES',
                is_primary_key=column_name in primary_keys,
                is_unique=column_name in unique_columns,
                is_foreign_key=column_name in foreign_keys,
                column_position=col_meta['ordinal_position'],
                sample_values=sample_values,
                enum_values=stats.get('enum_values'),
                cardinality=stats.get('cardinality'),
                null_percentage=stats.get('null_percentage'),
                is_pii=is_pii
            )
            columns.append(column)
            
        logger.info(f"Extracted {len(columns)} columns for '{table.table_name}'")
        return columns
    
    
    def _get_columns_metadata(self, table_name: str, schema_name: str) -> List[Dict[str, Any]]:
        """
            Get basic column metadata from information_schema.
        """
        
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (schema_name, table_name))
            return cur.fetchall()
        
    def _get_primary_keys(self, table_name: str, schema_name: str) -> set:
        """
            Get primary key columns.
        """
        
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = %s 
                AND tc.table_name = %s
                AND tc.constraint_type = 'PRIMARY KEY'
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query, (schema_name, table_name))
            return {row[0] for row in cur.fetchall()}
        
    def _get_unique_columns(self, table_name: str, schema_name: str) -> set:
        """
            Get columns with unique constraints.
        """
        
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = %s 
                AND tc.table_name = %s
                AND tc.constraint_type = 'UNIQUE'
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query, (schema_name, table_name))
            return {row[0] for row in cur.fetchall()}
        
    def _get_foreign_keys(self, table_name: str, schema_name: str) -> set:
        """
            Get foreign key columns.
        """
        
        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = %s 
                AND tc.table_name = %s
                AND tc.constraint_type = 'FOREIGN KEY'
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query, (schema_name, table_name))
            return {row[0] for row in cur.fetchall()}
        
    def _get_sample_values(self, table_name: str, schema_name: str, column_name: str, limit: int = 5) -> List[str]:
        """
            Get sample values for a column.
        """
        query = f"""
            SELECT DISTINCT "{column_name}"
            FROM "{schema_name}"."{table_name}"
            WHERE "{column_name}" IS NOT NULL
            LIMIT {limit}
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return [str(row[0]) for row in cur.fetchall()]
        except Exception:
            return []
        
    def _calculate_statistics(self, table_name: str, schema_name: str, column_name: str) -> Dict[str, Any]:
        """
            Calculate column statistics.
        """
        
        query = f"""
            SELECT 
                COUNT(DISTINCT "{column_name}") as unique_count,
                COUNT(*) as total_count,
                COUNT("{column_name}") as non_null_count
            FROM "{schema_name}"."{table_name}"
        """
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                result = cur.fetchone()
                unique_count = result['unique_count']
                total_count = result['total_count']
                non_null_count = result['non_null_count']
                
                # Calculate null percentage
                null_percentage = ((total_count - non_null_count) / total_count * 100) if total_count > 0 else 0
                
                # Determine cardinality
                if unique_count < 10:
                    cardinality = "low"
                    # Get enum values for low cardinality
                    enum_values = self._get_sample_values(table_name, schema_name, column_name, limit=20)
                
                elif unique_count < total_count * 0.5:
                    cardinality = "medium"
                    enum_values = None
                else:
                    cardinality = "high"
                    enum_values = None
                
                return {
                    "cardinality": cardinality,
                    "null_percentage": round(null_percentage, 2),
                    "enum_values": enum_values
                }
                
        except Exception:
            return {"cardinality": None, "null_percentage": None, "enum_values": None}
        
    def _is_pii_column(self, column_name: str, data_type: str, sample_values: List[str]) -> bool:
        """
            Detect if column might contain PII based on name.
        """
        if not self.openai_client:
            pii_keywords = ['email', 'phone', 'ssn', 'social_security', 'credit_card', 'password', 'address']
            column_lower = column_name.lower()
            return any(keyword in column_lower for keyword in pii_keywords)

        # Use OpenAI for intelligent PII detection
        try:
            sample_str = ', '.join(sample_values[:3]) if sample_values else 'No samples'
            
            prompt = f"""Analyze if this database column contains Personally Identifiable Information (PII).

                    Column name: {column_name}
                    Data type: {data_type}
                    Sample values: {sample_str}

                    PII includes: names, email addresses, phone numbers, SSN, addresses, credit card numbers, passwords, birth dates, etc.

                    Respond with ONLY "true" or "false".
                """
                
            
            response = self.openai_client.responses.parse(
                input=[
                    {"role": "system", "content": "You are a data privacy expert. Respond only with true or false."},
                    {"role": "user", "content": prompt}
                ],
                model="gpt-4o-mini",
                temperature=0.0,
                text_format=PIIDetectionOutput,
            )
            
            result = response.output[0].content[0].parsed
            
            return result.is_pii

        except Exception as e:
            logger.warning(f"OpenAI PII detection failed for '{column_name}': {e}. Using fallback.")
            
            # Fallback to keyword-based detection
            pii_keywords = ['email', 'phone', 'ssn', 'social_security', 'credit_card', 'password', 'address']
            column_lower = column_name.lower()
            return any(keyword in column_lower for keyword in pii_keywords)
        