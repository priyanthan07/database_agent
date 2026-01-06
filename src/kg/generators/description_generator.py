import logging
from typing import List, Dict, Any
from pydantic import BaseModel, Field

from ..models import Table, Column

logger = logging.getLogger(__name__)

class TableDescriptionOutput(BaseModel):
    description: str = Field(description="Brief 1-2 sentence description of the table's purpose")
    business_domain: str = Field(description="Single category like Sales, Finance, Inventory, Customer Management, etc.")
    typical_use_cases: List[str] = Field(description="List of 2-3 typical use cases for this table")

class ColumnDescriptionOutput(BaseModel):
    description: str = Field(description="Brief description of what this column stores")
    business_meaning: str = Field(description="Business context and meaning of this column")
    
class PIIDetectionOutput(BaseModel):
    is_pii: bool = Field(description="Whether this column contains Personally Identifiable Information")
    reasoning: str = Field(description="Brief explanation of the PII determination")

class DescriptionGenerator:
    """Generates AI descriptions using OpenAI."""
    def __init__(self, openai_client):
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
        
        prompt = f"""
            Analyze this database table and provide a structured response.

            Table name: {table.table_name}
            Columns: {', '.join(column_info)}
            Row count: {table.row_count_estimate or 'Unknown'}

            {sample_data}

            Provide:
            1. A brief 1-2 sentence description of the table's purpose
            2. The business domain (e.g., Sales, Finance, Customer Management, Inventory)
            3. Three typical use cases for this table
        """
        try:
            # Use structured output with Pydantic model via beta.chat.completions.parse
            result = self.client.generate_structured_completion(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are a database expert. Analyze the table and provide structured information."},
                    {"role": "user", "content": prompt}
                ],
                response_format=TableDescriptionOutput,
                temperature=0.0
            )
            
            logger.info(f"Generated description for '{table.table_name}': {result.business_domain}")
            
            return {
                "description": result.description,
                "business_domain": result.business_domain,
                "typical_use_cases": result.typical_use_cases
            }
        except Exception as e:
            logger.error(f"Failed to generate description for '{table.table_name}': {e}")
            return {
                "description": f"Table storing {table.table_name} data",
                "business_domain": "General",
                "typical_use_cases": []
            }
            
    def generate_column_description(self, column: Column, table: Table) -> Dict[str, Any]:
        """Generate description for a column """
        skip_keywords = ['id', 'created_at', 'updated_at', 'deleted_at']
        
        if column.is_foreign_key or any(kw in column.column_name.lower() for kw in skip_keywords):
            return {"description": None, "business_meaning": None}
        
        logger.info(f"Generating description for column '{column.qualified_name}'")
        
        sample_str = ', '.join(column.sample_values[:3]) if column.sample_values else 'No samples'
        enum_str = ', '.join(column.enum_values) if column.enum_values else 'N/A'
        
        prompt = f"""Analyze this database column and provide a structured description.

                Table: {table.table_name}
                Column: {column.column_name}
                Data type: {column.data_type}
                Sample values: {sample_str}
                Enum values: {enum_str}
                Cardinality: {column.cardinality}

                Provide:
                1. A brief description of what this column stores
                2. The business meaning and context of this column
            """
            
        messages = [
            {"role": "system", "content": "You are a database expert. Analyze the column and provide structured information."},
            {"role": "user", "content": prompt}
        ]
        
        try:
            result = self.client.generate_structured_completion(
                messages=messages,
                response_model=ColumnDescriptionOutput,
                model="gpt-4o-mini",
                temperature=0.0
            )
            
            return {
                "description": result.description,
                "business_meaning": result.business_meaning
            }
            
        except Exception as e:
            logger.error(f"Failed to generate column description for '{column.qualified_name}': {e}")
            return {"description": None, "business_meaning": None}
        
    def detect_pii(self, column: Column, table: Table) -> bool:
        """Detect if column contains PII using AI with structured outputs"""
        logger.info(f"Detecting PII for column '{column.qualified_name}'")
        
        sample_str = ', '.join(column.sample_values[:3]) if column.sample_values else 'No samples'
        
        prompt = f"""Analyze if this database column contains Personally Identifiable Information (PII).

                Column name: {column.column_name}
                Data type: {column.data_type}
                Sample values: {sample_str}

                PII includes: names, email addresses, phone numbers, SSN, addresses, credit card numbers, passwords, birth dates, IP addresses, etc.

                Determine if this column contains PII and explain your reasoning.
            """
        messages = [
            {"role": "system", "content": "You are a data privacy expert. Analyze if the column contains PII."},
            {"role": "user", "content": prompt}
        ]
        
        try:
            result = self.client.generate_structured_completion(
                messages=messages,
                response_model=PIIDetectionOutput,
                model="gpt-4o-mini",
                temperature=0.0
            )
            
            logger.info(f"PII detection for '{column.qualified_name}': {result.is_pii} - {result.reasoning}")
            return result.is_pii
            
        except Exception as e:
            logger.warning(f"OpenAI PII detection failed for '{column.qualified_name}': {e}. Using heuristic fallback.")
            return None
        
    def _format_sample_data(self, table: Table) -> str:
        """Format sample data for context"""
        if not table.columns:
            return ""
        
        sample_lines = []
        for col_name, col in list(table.columns.items())[:5]:  # First 5 columns
            if col.sample_values:
                samples = ', '.join(col.sample_values[:3])
                sample_lines.append(f"  {col_name}: {samples}")
        
        if sample_lines:
            return "Sample data:\n" + "\n".join(sample_lines)
        return ""
        
        