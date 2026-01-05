import logging
import json
from typing import List, Dict, Any

from ..models import Table, Column
from .openai_client import OpenAIClient

logger = logging.getLogger(__name__)

class DescriptionGenerator:
    """Generates AI descriptions using OpenAI."""
    def __init__(self, openai_client: OpenAIClient):
        self.client = openai_client
        
    def generate_table_description(self, table: Table) -> Dict[str, Any]:
        """ Generate description for a table. """
        logger.info(f"Generating description for table '{table.table_name}'")
        
        # Prepare column information
        column_info = []
        for col in table.columns.values():
            col_str = f"{col.column_name} ({col.data_type})"
            if col.is_primary_key:
                col_str += " [PK]"
            if col.is_foreign_key:
                col_str += " [FK]"
            column_info.append(col_str)
        
        # Get sample values for context
        sample_data = self._format_sample_data(table)
        
        prompt = f"""Analyze this database table and provide a structured response.

                    Table name: {table.table_name}
                    Columns: {', '.join(column_info)}
                    Row count: {table.row_count_estimate or 'Unknown'}

                    {sample_data}

                    Respond ONLY with valid JSON in this exact format:
                    {{
                    "description": "Brief 1-2 sentence description of this table's purpose",
                    "business_domain": "Single category like Sales, Finance, Inventory, Customer Management, etc.",
                    "typical_use_cases": ["use case 1", "use case 2", "use case 3"]
                }}"""