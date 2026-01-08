import logging
import re
from typing import Dict, List, Optional, Any
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Function
from sqlparse.tokens import Keyword, DML

logger = logging.getLogger(__name__)

class SQLValidationTool:
    """Validates SQL syntax and structure"""
    
    def validate_sql(
        self,
        sql: str,
        expected_tables: Optional[List[str]] = None,
        kg_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
            Validate SQL query syntax and structure.
        """
        logger.info("Validating SQL query")
        
        result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "parsed_tables": [],
            "parsed_columns": [],
            "has_joins": False,
            "has_where": False,
            "query_type": None
        }
        
        try:
            # Parse SQL
            parsed = sqlparse.parse(sql)
            
            if not parsed:
                result["is_valid"] = False
                result["errors"].append("Failed to parse SQL query")
                return result
            
            statement = parsed[0]
            
            # Check query type
            query_type = self._get_query_type(statement)
            result["query_type"] = query_type
            
            if query_type != "SELECT":
                result["warnings"].append(f"Query type is {query_type}, expected SELECT")
            
            # Extract tables
            tables = self._extract_tables(statement)
            result["parsed_tables"] = tables
            
            # Extract columns
            columns = self._extract_columns(statement)
            result["parsed_columns"] = columns
            
            # Check for JOINs
            result["has_joins"] = "join" in sql.lower()
            
            # Check for WHERE clause
            result["has_where"] = self._has_where_clause(statement)
            
            # Validate expected tables
            if expected_tables:
                missing_tables = set(expected_tables) - set(tables)
                if missing_tables:
                    result["warnings"].append(
                        f"Expected tables not found in query: {list(missing_tables)}"
                    )
            
            # Validate column references with KG context
            if kg_context:
                column_errors = self._validate_columns_with_kg(columns, kg_context)
                result["errors"].extend(column_errors)
                if column_errors:
                    result["is_valid"] = False
            
            # Basic syntax checks
            syntax_errors = self._check_basic_syntax(sql)
            result["errors"].extend(syntax_errors)
            if syntax_errors:
                result["is_valid"] = False
            
            logger.info(
                f"Validation result: {'VALID' if result['is_valid'] else 'INVALID'} "
                f"(tables: {len(tables)}, columns: {len(columns)})"
            )
            
        except Exception as e:
            logger.error(f"SQL validation failed: {e}")
            result["is_valid"] = False
            result["errors"].append(f"Validation error: {str(e)}")
        
        return result
    
    def _get_query_type(self, statement) -> str:
        """
            Get query type (SELECT, INSERT, etc.)
        """
        
        for token in statement.tokens:
            if token.ttype is DML:
                return token.value.upper()
        return "UNKNOWN"
    
    def _extract_tables(self, statement) -> List[str]:
        """
            Extract table names from parsed statement
        """
        tables = []
        from_seen = False
        
        for token in statement.tokens:
            if from_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        tables.append(self._get_real_name(identifier))
                elif isinstance(token, Identifier):
                    tables.append(self._get_real_name(token))
                from_seen = False
            
            if token.ttype is Keyword and token.value.upper() in ('FROM', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN'):
                from_seen = True
        
        return tables
    
    def _extract_columns(self, statement) -> List[str]:
        """Extract column references from parsed statement"""
        columns = []
        
        for token in statement.tokens:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    col_name = str(identifier)
                    if col_name != '*':
                        columns.append(col_name)
            elif isinstance(token, Identifier):
                col_name = str(token)
                if col_name != '*':
                    columns.append(col_name)
        
        return columns
    
    def _get_real_name(self, identifier) -> str:
        """Get real table name from identifier (handles aliases)"""
        if isinstance(identifier, Identifier):
            return identifier.get_real_name()
        return str(identifier)
    
    def _has_where_clause(self, statement) -> bool:
        """Check if statement has WHERE clause"""
        for token in statement.tokens:
            if isinstance(token, Where):
                return True
        return False
    
    def _validate_columns_with_kg(
        self,
        columns: List[str],
        kg_context: Dict[str, Any]
    ) -> List[str]:
        """Validate column references against KG context"""
        errors = []
        
        # Extract available columns from KG context
        available_columns = set()
        for table_name, table_data in kg_context.items():
            if isinstance(table_data, dict) and 'columns' in table_data:
                for col_name in table_data['columns'].keys():
                    available_columns.add(f"{table_name}.{col_name}")
                    available_columns.add(col_name)
        
        # Check each column reference
        for col_ref in columns:
            # Skip functions and special cases
            if '(' in col_ref or col_ref.upper() in ('COUNT', 'SUM', 'AVG', 'MIN', 'MAX'):
                continue
            
            # Check if column exists
            if '.' in col_ref:
                # Qualified reference
                if col_ref not in available_columns:
                    errors.append(f"Column not found: {col_ref}")
            else:
                # Unqualified reference - check if it exists in any table
                found = False
                for avail in available_columns:
                    if avail.endswith(f".{col_ref}"):
                        found = True
                        break
                
                if not found and col_ref not in available_columns:
                    errors.append(f"Column not found: {col_ref}")
        
        return errors
    
    def _check_basic_syntax(self, sql: str) -> List[str]:
        """Basic syntax validation"""
        errors = []
        
        # Check for balanced parentheses
        if sql.count('(') != sql.count(')'):
            errors.append("Unbalanced parentheses")
        
        # Check for semicolon at end (remove it if present for safety)
        if sql.rstrip().endswith(';'):
            errors.append("Remove semicolon from end of query")
        
        # Check for basic SQL injection patterns
        dangerous_patterns = [
            r';\s*DROP\s+TABLE',
            r';\s*DELETE\s+FROM',
            r';\s*INSERT\s+INTO',
            r';\s*UPDATE\s+',
            r'--\s*$'
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                errors.append(f"Potentially dangerous SQL pattern detected: {pattern}")
        
        return errors