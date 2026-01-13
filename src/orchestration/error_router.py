import logging
import re
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
from .agent_state import AgentState

logger = logging.getLogger(__name__)

class ErrorClassificationOutput(BaseModel):
    error_category: str = Field(description="Primary error category: schema_error, sql_syntax_error, sql_logic_error, execution_error, or system_error")
    sub_category: str = Field(description="Specific sub-category: column_not_found, table_not_found, join_error, syntax_error, type_mismatch, ambiguous_reference, groupby_error, aggregate_error, function_error, permission_denied, timeout, or other")
    is_schema_related: bool = Field(description="True if error is related to missing/incorrect schema elements (tables, columns, relationships)")
    is_sql_generation_related: bool = Field(description="True if error is related to SQL syntax or logic problems")
    requires_table_reselection: bool = Field(description="True if fixing this error requires selecting different tables")
    requires_sql_regeneration: bool = Field(description="True if fixing this error requires regenerating the SQL")
    confidence: float = Field(description="Confidence in classification (0.0-1.0)",ge=0.0,le=1.0)
    reasoning: str = Field(description="Brief explanation of why this classification was chosen")

class ColumnExtractionOutput(BaseModel):
    primary_column: Optional[str] = Field(description="The main column name mentioned in the error (just the column name, without table prefix)")
    qualified_column: Optional[str] = Field(description="Full qualified column name if present (e.g., 'orders.customer_id')")
    additional_columns: List[str] = Field(default_factory=list,description="Any other column names mentioned in the error")
    suggested_column: Optional[str] = Field(description="Suggested correct column name if PostgreSQL provided a hint")
    confidence: float = Field(description="Confidence in extraction (0.0-1.0)",ge=0.0,le=1.0)
    reasoning: str = Field(description="Brief explanation of extraction")
    
class ColumnContextCheckOutput(BaseModel):
    column_exists: bool = Field(description="True if the column exists in any of the provided tables")
    found_in_tables: List[str] = Field(default_factory=list,description="List of table names where the column was found")
    is_ambiguous: bool = Field(description="True if column exists in multiple tables without qualification")
    case_mismatch: bool = Field(description="True if column exists but with different casing")
    correct_column_name: Optional[str] = Field(description="The correct column name with proper casing if case mismatch")
    suggested_table: Optional[str] = Field(description="Suggested table to use if column is ambiguous or missing")
    confidence: float = Field(description="Confidence in the check (0.0-1.0)",ge=0.0,le=1.0)
    reasoning: str = Field(description="Explanation of the column context check result")
    
class ErrorRoutingOutput(BaseModel):
    route_to: str = Field(description="Which agent to route to: 'agent_1' (Schema Selector), 'agent_2' (SQL Generator), or 'complete' (end workflow)")
    reasoning: str = Field(description="Detailed explanation of why this routing decision was made")
    priority_action: str = Field(description="Specific action the target agent should take to fix the error")
    confidence: float = Field(description="Confidence in routing decision (0.0-1.0)",ge=0.0,le=1.0)

class ErrorRouter:
    """
        Routes errors to the correct agent for correction.
    """
    def __init__(self, openai_client):
        """
            Initialize ErrorRouter with OpenAI client.
        """
        self.openai_client = openai_client
    
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
        
        # Prepare table context summary for LLM
        tables_summary = self._format_tables_summary(table_contexts)
        
        prompt = f"""Analyze this PostgreSQL query execution error and classify it.

                    Error Message:
                    {error_message}

                    Generated SQL:
                    {generated_sql}

                    Available Tables and Columns:
                    {tables_summary}

                    Classify this error by determining:
                    1. The primary error category (schema_error, sql_syntax_error, sql_logic_error, execution_error, system_error)
                    2. The specific sub-category
                    3. Whether it's related to schema (missing tables/columns) or SQL generation (syntax/logic)
                    4. Whether fixing requires table reselection or SQL regeneration
                    5. Your confidence level and reasoning
                    
                    Guidelines:
                    - schema_error: Missing tables, columns, or relationships in the schema
                    - sql_syntax_error: Invalid SQL syntax (keywords, operators, structure)
                    - sql_logic_error: Valid syntax but incorrect logic (GROUP BY, JOINs, aggregates, WHERE clause issues)
                    - execution_error: Runtime issues (type mismatches, division by zero, data issues)
                    - system_error: Permission, timeout, or infrastructure issues
                    
                    Strictly do not hallucinate. Do not make any mistakes.
                """
                
        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": "You are a PostgreSQL expert analyzing query errors. Provide accurate classification to help route errors for correction."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_model=ErrorClassificationOutput,
                model="gpt-4o",
                temperature=0.0
            )
        except Exception as e:
            logger.error(f"Error classification with GPT-4 failed: {e}", exc_info=True)
            # Fallback to basic classification
            return {
                "category": "sql_logic_error",
                "sub_category": "other",
                "is_schema_related": False,
                "is_sql_generation_related": True,
                "requires_table_reselection": False,
                "requires_sql_regeneration": True,
                "confidence": 0.3,
                "reasoning": "Classification failed, defaulting to SQL logic error",
                "pattern_matched": None,
                "error_message": error_message
            }
    
    def route_error(
        self,
        classification: Dict[str, Any],
        state: AgentState
    ) -> Dict[str, str]:
        """
            Determine which agent should handle the error.
        """
        logger.info(f"Determining error routing using GPT-4")
        
        # Prepare error history
        error_history_text = self._format_error_history(state.error_history)
        
        prompt = f"""Decide how to route this error for correction.

                    Current Error:
                    - Category: {classification['category']}
                    - Sub-category: {classification['sub_category']}
                    - Schema-related: {classification['is_schema_related']}
                    - SQL-related: {classification['is_sql_generation_related']}
                    - Reasoning: {classification['reasoning']}

                    Current State:
                    - Retry count: {state.retry_count}
                    - Max retries: {state.max_retries}
                    - Selected tables: {state.final_tables if state.final_tables else 'None'}

                    Error History:
                    {error_history_text}

                    Available Agents:
                    1. agent_1 (Schema Selector): Re-selects tables, columns, and relationships from knowledge graph
                    2. agent_2 (SQL Generator): Re-generates SQL query with better syntax/logic
                    3. complete: Stop retrying (for fatal errors or max retries exceeded)

                    Routing Guidelines:
                    - If error is schema-related (missing tables/columns/relationships): Route to agent_1
                    - If error is SQL syntax/logic related: Route to agent_2
                    - If this is a retry and same error category occurred before: Consider routing to the OTHER agent
                    - If retry_count >= max_retries: Route to 'complete'
                    - If error is permission_denied or timeout: Route to 'complete'
                    - Provide the specific action the target agent should take

                    Make the best routing decision to fix this error.
            """
        
        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": "You are an intelligent error routing system. Route errors to the appropriate agent that can best fix the issue. Consider retry history and avoid routing to the same agent repeatedly if it's not working."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_model=ErrorRoutingOutput,
                model="gpt-4o",
                temperature=0.0
            )
            
            logger.info(f"Routing decision: {result.route_to}")
            logger.info(f"Reasoning: {result.reasoning}")
            logger.info(f"Priority action: {result.priority_action}")
            
            # Validate routing decision
            if result.route_to not in ["agent_1", "agent_2", "complete"]:
                logger.warning(f"Invalid route '{result.route_to}', defaulting to agent_2")
                result.route_to = "agent_2"
            
            return {
                "route_to": result.route_to,
                "reason": result.reasoning,
                "priority_action": result.priority_action,
                "confidence": result.confidence
            }
            
        except Exception as e:
            logger.error(f"Error routing with GPT-4 failed: {e}", exc_info=True)
            
            # Fallback routing logic
            if state.retry_count >= state.max_retries:
                return {
                    "route_to": "complete",
                    "reason": f"Max retries ({state.max_retries}) exceeded",
                    "priority_action": "None",
                    "confidence": 1.0
                }
            
            # Default to agent_2 for SQL issues
            return {
                "route_to": "agent_2",
                "reason": "Routing failed, defaulting to SQL Generator for correction",
                "priority_action": "Regenerate SQL with error context",
                "confidence": 0.3
            }
            
    def _format_tables_summary(self, table_contexts: Dict[str, Any]) -> str:
        """Format table contexts as a summary for LLM."""
        if not table_contexts:
            return "No tables available"
        
        lines = []
        for table_name, context in table_contexts.items():
            columns = context.get("columns", {})
            relationships = context.get("relationships", [])
            
            lines.append(f"\nTable: {table_name}")
            lines.append(f"  Total columns: {len(columns)}")
            
             # Column details with data types and constraints
            lines.append(f"  Columns:")
            for col_name, col_data in list(columns.items())[:15]:  # First 15 columns
                data_type = col_data.get("data_type", "unknown")
                constraints = []
                
                if col_data.get("is_primary_key"):
                    constraints.append("PRIMARY KEY")
                if col_data.get("is_foreign_key"):
                    constraints.append("FOREIGN KEY")
                if not col_data.get("is_nullable", True):
                    constraints.append("NOT NULL")
                
                constraint_str = f" [{', '.join(constraints)}]" if constraints else ""
                lines.append(f"    - {col_name} ({data_type}){constraint_str}")
            
            if len(columns) > 15:
                lines.append(f"    ... and {len(columns) - 15} more columns")
                
            # Relationships
            if relationships:
                lines.append(f"  Relationships:")
                for rel in relationships[:5]:  # First 5 relationships
                    from_table = rel.get("from_table", "")
                    from_col = rel.get("from_column", "")
                    to_table = rel.get("to_table", "")
                    to_col = rel.get("to_column", "")
                    
                    if from_table == table_name:
                        lines.append(f"    - {from_table}.{from_col} → {to_table}.{to_col}")
                    else:
                        lines.append(f"    - {to_table}.{to_col} ← {from_table}.{from_col}")
                
                if len(relationships) > 5:
                    lines.append(f"    ... and {len(relationships) - 5} more relationships")
        
        return "\n".join(lines)
    
    
    def _format_error_history(self, error_history: List[Dict[str, Any]]) -> str:
        """Format error history for LLM context."""
        if not error_history:
            return "No previous errors"
        
        lines = []
        for i, error_record in enumerate(error_history, 1):
            agent = error_record.get("agent", "Unknown")
            category = error_record.get("error_category", "Unknown")
            message = error_record.get("error_message", "")[:100]
            
            lines.append(f"{i}. Agent: {agent}")
            lines.append(f"   Category: {category}")
            lines.append(f"   Message: {message}...")
        
        return "\n".join(lines)
    