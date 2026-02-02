import logging
import re
from typing import Dict, List, Optional, Any
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Function
from sqlparse.tokens import Keyword, DML
from langfuse import observe
from langfuse import Langfuse

from config.settings import Settings

logger = logging.getLogger(__name__)

class SQLValidationTool:
    """Validates SQL syntax and structure"""
    
    def __init__(self):
        self.setting = Settings()
        
        self.langfuse = Langfuse(
            public_key=self.setting.LANGFUSE_PUBLIC_KEY,
            secret_key=self.setting.LANGFUSE_SECRET_KEY,
            host=self.setting.LANGFUSE_HOST
        )
    
    @observe(
        name="tool_validate_sql_query",
        as_type="span"
    ) 
    def validate_sql(
        self,
        sql: str,
        expected_tables: Optional[List[str]] = None,
        kg_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
            Validate SQL query syntax and structure.
        """
        
        self.langfuse.update_current_span(
            input={
                "sql": sql,
                "expected_tables": expected_tables,
                "kg_context": kg_context
            }
        )
        
        logger.info("Validating SQL query")
        
        result = {
            "is_valid": True,
            "errors": [],
            "warnings": [],
            "query_type": None
        }
        
        try:
            # Parse SQL
            parsed = sqlparse.parse(sql)
            
            if not parsed:
                result["is_valid"] = False
                result["errors"].append("Failed to parse SQL query")
                
                self.langfuse.update_current_span(
                    output={
                        "result": result
                    }
                )
                
                return result
            
            statement = parsed[0]
            
            # Check 1: Query type
            query_type = self._get_query_type(statement)
            result["query_type"] = query_type
            
            if query_type != "SELECT":
                result["warnings"].append(f"Query type is {query_type}, expected SELECT")
            
            # Check 2: Basic syntax issues
            syntax_errors = self._check_basic_syntax(sql)
            result["errors"].extend(syntax_errors)
            if syntax_errors:
                result["errors"].extend(syntax_errors)
                result["is_valid"] = False
            
            # Check 3: Dangerous patterns
            dangerous = self._check_dangerous_patterns(sql)
            if dangerous:
                result["errors"].extend(dangerous)
                result["is_valid"] = False
            
            # Check 4: Expected tables mentioned 
            if expected_tables and kg_context:
                table_warnings = self._check_expected_tables_mentioned(
                    sql, expected_tables
                )
                result["warnings"].extend(table_warnings)
            
            logger.info(
                f"Validation result: {'VALID' if result['is_valid'] else 'INVALID'}"
            )
            
            if result["errors"]:
                logger.warning(f"Errors: {result['errors']}")
            if result["warnings"]:
                logger.info(f"Warnings: {result['warnings']}")
            
        except Exception as e:
            logger.error(f"SQL validation failed: {e}")
            result["is_valid"] = False
            result["errors"].append(f"Validation error: {str(e)}")
            
        self.langfuse.update_current_span(
                    output={
                        "result": result
                    }
                )
        
        return result
    
    def _get_query_type(self, statement) -> str:
        """
            Get query type (SELECT, INSERT, etc.)
        """
        
        for token in statement.tokens:
            if token.ttype is DML:
                return token.value.upper()
        return "UNKNOWN"
    
    def _check_basic_syntax(self, sql: str) -> List[str]:
        """Basic syntax validation"""
        errors = []
        
        # Check 1: Balanced parentheses
        if sql.count('(') != sql.count(')'):
            errors.append("Unbalanced parentheses")
        
        # Check 2: No trailing semicolon (we add LIMIT, semicolon breaks it)
        if sql.rstrip().endswith(';'):
            errors.append("Remove semicolon from end of query")
        
        # Check 3: Must contain FROM for SELECT
        sql_upper = sql.upper()
        if 'SELECT' in sql_upper and 'FROM' not in sql_upper:
            errors.append("SELECT query must have FROM clause")
        
        # Check 4: Unclosed quotes
        single_quotes = sql.count("'")
        if single_quotes % 2 != 0:
            errors.append("Unclosed single quote")
        
        return errors
    
    def _check_dangerous_patterns(self, sql: str) -> List[str]:
        """Check for SQL injection patterns"""
        errors = []
        
        dangerous_patterns = [
            (r';\s*DROP\s+TABLE', "Potential DROP TABLE injection"),
            (r';\s*DELETE\s+FROM', "Potential DELETE injection"),
            (r';\s*INSERT\s+INTO', "Potential INSERT injection"),
            (r';\s*UPDATE\s+\w+\s+SET', "Potential UPDATE injection"),
            (r'--\s*$', "SQL comment at end of query"),
            (r'/\*.*?\*/', "SQL block comment detected"),
        ]
        
        for pattern, message in dangerous_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                errors.append(message)
        
        return errors
    
    def _check_expected_tables_mentioned(
        self,
        sql: str,
        expected_tables: List[str]
    ) -> List[str]:
        """
            Loose check: Are expected table names mentioned anywhere in SQL?
            This is NOT a guarantee they're used correctly, just a sanity check.
        """
        warnings = []
        sql_lower = sql.lower()
        
        for table in expected_tables:
            # Check if table name appears in SQL (case-insensitive)
            if table.lower() not in sql_lower:
                warnings.append(
                    f"Expected table '{table}' not found in query "
                    f"(may be using alias)"
                )
        
        return warnings
    