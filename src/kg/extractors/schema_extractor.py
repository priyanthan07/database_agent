import logging
from typing import Dict
from uuid import UUID

from ..models import Table, Column, Relationship
from .table_extractor import TableExtractor
from .column_extractor import ColumnExtractor
from .relationship_extractor import RelationshipExtractor

logger = logging.getLogger(__name__)

class SchemaExtractor:
    """Orchestrates all extraction operations."""
    
    def __init__(self, connection, openai_client=None):
        self.conn = connection
        self.table_extractor = TableExtractor(connection)
        self.column_extractor = ColumnExtractor(connection)
        self.relationship_extractor = RelationshipExtractor(connection)
        
    def extract_schema(self, kg_id: UUID, schema_name: str = "public") -> Dict:
        """
            Extract complete schema metadata.
        """
        logger.info(f"Starting schema extraction for kg_id={kg_id}")
        
        # Step 1: Extract tables
        tables = self.table_extractor.extract_tables(kg_id, schema_name)
        
        # Create table_id mapping
        table_id_map = {table.table_name: table.table_id for table in tables}
        
        # Step 2: Extract columns for each table
        all_columns = []
        for table in tables:
            columns = self.column_extractor.extract_columns(table)
            all_columns.extend(columns)
            
            # Add columns to table object
            for column in columns:
                table.columns[column.column_name] = column
                
        # Step 3: Extract relationships
        relationships = self.relationship_extractor.extract_relationships(
            kg_id, table_id_map, schema_name
        )
        
        logger.info(
            f"Schema extraction complete: {len(tables)} tables, "
            f"{len(all_columns)} columns, {len(relationships)} relationships"
        )
        
        return {
            "tables": tables,
            "columns": all_columns,
            "relationships": relationships
        }
        