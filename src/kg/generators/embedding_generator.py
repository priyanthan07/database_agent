import logging
from typing import List, Dict

from ..models import Table, Column

logger = logging.getLogger(__name__)

class EmbeddingGenerator:
    def __init__(self, openai_client):
        self.client = openai_client

    def generate_table_embeddings(self, tables: List[Table]) -> Dict[str, List[float]]:
        """Generate embeddings for all tables"""
        logger.info(f"Generating embeddings for {len(tables)} tables")
        
        # Prepare texts for embedding
        texts = []
        table_names = []
        
        for table in tables:
            text = self._create_table_text(table)
            texts.append(text)
            table_names.append(table.table_name)
            
        try:
            embeddings = self.client.generate_embeddings(texts)
            
            # Map table names to embeddings
            result = {name: emb for name, emb in zip(table_names, embeddings)}
            logger.info(f"Generated embeddings for {len(result)} tables")
            return result
            
        except Exception as e:
            logger.error(f"Failed to generate table embeddings: {e}")
            return {}
        
    def generate_column_embeddings(self, columns: List[Column], tables_dict: Dict[str, Table]) -> Dict[str, List[float]]:
        """Generate embeddings for important columns"""
        logger.info(f"Generating embeddings for key columns")
        
        skip_keywords = ['id', 'created_at', 'updated_at', 'deleted_at']
        columns_to_embed = [
            col for col in columns
            if not col.is_foreign_key 
            and not any(kw in col.column_name.lower() for kw in skip_keywords)
            and col.description  # Only embed if we have a description
        ]
        
        if not columns_to_embed:
            logger.info("No columns to embed")
            return {}
        
        # Prepare texts
        texts = []
        column_qualified_names = []
        
        for column in columns_to_embed:
            text = self._create_column_text(column)
            texts.append(text)
            column_qualified_names.append(column.qualified_name)
        
        # Generate embeddings in batch
        try:
            embeddings = self.client.generate_embeddings(texts)
            
            # Map qualified names to embeddings
            result = {name: emb for name, emb in zip(column_qualified_names, embeddings)}
            logger.info(f"Generated embeddings for {len(result)} columns")
            return result
            
        except Exception as e:
            logger.error(f"Failed to generate column embeddings: {e}")
            return {}
        
    def _create_table_text(self, table: Table) -> str:
        """Create rich text representation of table for embedding"""
        parts = [f"Table: {table.table_name}"]
        
        if table.description:
            parts.append(f"Description: {table.description}")
        
        if table.business_domain:
            parts.append(f"Domain: {table.business_domain}")
        
        if table.typical_use_cases:
            use_cases = ', '.join(table.typical_use_cases)
            parts.append(f"Use cases: {use_cases}")
        
        # Add column names
        if table.columns:
            column_names = ', '.join(list(table.columns.keys())[:10])  # First 10 columns
            parts.append(f"Columns: {column_names}")
        
        return "\n".join(parts)
    
    def _create_column_text(self, column: Column) -> str:
        """Create rich text representation of column for embedding"""
        parts = [f"{column.qualified_name}"]
        
        if column.description:
            parts.append(column.description)
        
        if column.business_meaning:
            parts.append(column.business_meaning)
        
        parts.append(f"Type: {column.data_type}")
        
        if column.enum_values:
            enum_str = ', '.join(column.enum_values[:5])
            parts.append(f"Values: {enum_str}")
        
        return " - ".join(parts)
    