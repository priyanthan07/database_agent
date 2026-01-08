import logging
import re
from typing import Dict, Any

from .agent_state import AgentState

logger = logging.getLogger(__name__)

class ErrorRouter:
    """
        Routes errors to the correct agent for correction.
    """
    
    # Error patterns and their categories
    ERROR_PATTERNS = [
        # Syntax errors
        {
            "pattern": r"syntax error",
            "category": "syntax_error",
            "keywords": ["syntax", "unexpected", "parse"]
        },
        # Column not found
        {
            "pattern": r"column.*does not exist",
            "category": "column_not_found",
            "keywords": ["column", "does not exist", "unknown column"]
        },
        # Table not found
        {
            "pattern": r"relation.*does not exist",
            "category": "table_not_found",
            "keywords": ["relation", "table", "does not exist"]
        },
        # JOIN errors
        {
            "pattern": r"(missing FROM|join.*reference)",
            "category": "join_error",
            "keywords": ["missing FROM", "join", "cross-database"]
        },
        # Type mismatch
        {
            "pattern": r"(type|data type|invalid input)",
            "category": "type_mismatch",
            "keywords": ["type", "data type", "invalid input", "cannot cast"]
        },
        # Permission denied
        {
            "pattern": r"permission denied",
            "category": "permission_denied",
            "keywords": ["permission", "denied", "access"]
        },
        # Timeout
        {
            "pattern": r"(timeout|cancelled|query timeout)",
            "category": "timeout",
            "keywords": ["timeout", "cancelled", "time limit"]
        },
        # Division by zero
        {
            "pattern": r"division by zero",
            "category": "logic_error",
            "keywords": ["division by zero"]
        },
        # Ambiguous column reference
        {
            "pattern": r"ambiguous",
            "category": "ambiguous_reference",
            "keywords": ["ambiguous", "not unique"]
        }
    ]
    
    def classify_error(
        self,
        error_message: str,
        generated_sql: str,
        table_contexts: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
            Classify error into category.
        """
        logger.info(f"Classifying error: {error_message[:100]}...")
        
        error_lower = error_message.lower()
        
        # Try pattern matching
        for error_pattern in self.ERROR_PATTERNS:
            if re.search(error_pattern["pattern"], error_lower, re.IGNORECASE):
                category = error_pattern["category"]
                logger.info(f"Matched pattern: {category}")
                
                return {
                    "category": category,
                    "pattern_matched": error_pattern["pattern"],
                    "error_message": error_message
                }
        
        # Default to logic_error if no pattern matched
        logger.warning("No pattern matched, defaulting to logic_error")
        return {
            "category": "logic_error",
            "pattern_matched": None,
            "error_message": error_message
        }
    
    def route_error(
        self,
        error_category: str,
        state: AgentState
    ) -> Dict[str, str]:
        """
        Determine which agent should handle the error.
        
        Args:
            error_category: Error category from classify_error
            state: Current agent state
            
        Returns:
            Dict with route_to and reason
        """
        logger.info(f"Routing error category: {error_category}")
        
        # SQL syntax errors → Agent 2
        if error_category == "syntax_error":
            return {
                "route_to": "agent_2",
                "reason": "SQL syntax error, routing to SQL Generator for correction"
            }
        
        # Column not found → Check if in provided context
        elif error_category == "column_not_found":
            # Extract column name from error
            column_name = self._extract_column_name(state.error_message)
            
            if column_name and self._column_in_context(column_name, state.table_contexts):
                # Column exists in schema but SQL has wrong reference
                return {
                    "route_to": "agent_2",
                    "reason": f"Column '{column_name}' exists but wrong reference in SQL"
                }
            else:
                # Column doesn't exist in selected tables, need different tables
                return {
                    "route_to": "agent_1",
                    "reason": f"Column '{column_name}' not in selected tables, need to select different tables"
                }
        
        # Table not found → Agent 1
        elif error_category == "table_not_found":
            return {
                "route_to": "agent_1",
                "reason": "Table not found, routing to Schema Selector to re-select tables"
            }
        
        # JOIN errors → Agent 1
        elif error_category == "join_error":
            return {
                "route_to": "agent_1",
                "reason": "JOIN error, routing to Schema Selector to check relationships"
            }
        
        # Type mismatch → Agent 2
        elif error_category == "type_mismatch":
            return {
                "route_to": "agent_2",
                "reason": "Data type mismatch, routing to SQL Generator for correction"
            }
        
        # Ambiguous reference → Agent 2
        elif error_category == "ambiguous_reference":
            return {
                "route_to": "agent_2",
                "reason": "Ambiguous column reference, routing to SQL Generator to add table prefixes"
            }
        
        # Logic errors → Agent 2
        elif error_category == "logic_error":
            return {
                "route_to": "agent_2",
                "reason": "Logic error in SQL, routing to SQL Generator for correction"
            }
        
        # Permission denied or timeout → Don't retry
        elif error_category in ["permission_denied", "timeout"]:
            return {
                "route_to": "complete",
                "reason": f"{error_category}: Cannot be fixed by retry"
            }
        
        # Unknown category → Try Agent 2
        else:
            logger.warning(f"Unknown error category: {error_category}, defaulting to Agent 2")
            return {
                "route_to": "agent_2",
                "reason": "Unknown error, routing to SQL Generator to attempt fix"
            }
    
    def _extract_column_name(self, error_message: str) -> str:
        """Extract column name from error message"""
        # Pattern: column "column_name" does not exist
        match = re.search(r'column "([^"]+)"', error_message, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Pattern: column column_name does not exist
        match = re.search(r'column (\w+) does not exist', error_message, re.IGNORECASE)
        if match:
            return match.group(1)
        
        return ""
    
    def _column_in_context(self, column_name: str, table_contexts: Dict) -> bool:
        """Check if column exists in any of the provided table contexts"""
        for table_name, context in table_contexts.items():
            if "columns" in context:
                # Check both qualified and unqualified names
                if column_name in context["columns"]:
                    return True
                
                # Check if it's a qualified reference (table.column)
                if "." in column_name:
                    table_part, col_part = column_name.split(".", 1)
                    if table_part == table_name and col_part in context["columns"]:
                        return True
        
        return False