import logging
import time
import psycopg2
from typing import Dict, Any, Optional
from psycopg2.extras import RealDictCursor

from .base_agent import BaseAgent
from ..orchestration.agent_state import AgentState
from ..orchestration.error_router import ErrorRouter

logger = logging.getLogger(__name__)

class ExecutorValidatorAgent(BaseAgent):
    """
        Agent 3: Execution & Validation
    """
    
    def __init__(self, kg_manager, openai_client, source_db_conn, memory_repository):
        super().__init__(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_db_conn,
            agent_name="Executor & Validator Agent"
        )
        
        self.memory_repository = memory_repository
        self.error_router = ErrorRouter()
        
        # SQL execution settings
        self.timeout_seconds = 30
        self.max_rows = 10000
        
    def process(self, state: AgentState) -> AgentState:
        """Main processing logic for execution and validation"""
        self.log_start(state)
        start_time = time.time()
        
        try:
            if not state.generated_sql:
                raise ValueError("No SQL query to execute")
            
            # Step 1: Execute SQL
            self.logger.info("Step 1: Executing SQL query")
            self.logger.info(f"SQL:\n{state.generated_sql}")
            
            execution_result = self._execute_sql_safely(state.generated_sql)
            
            # Record execution time
            state.execution_time_ms = int((time.time() - start_time) * 1000)
            
            # Step 2: Handle result
            if execution_result["success"]:
                self.logger.info("✓ SQL execution successful")
                state.execution_success = True
                state.execution_result = execution_result["data"]
                state.final_result = execution_result["data"]
                state.route_to_agent = "complete"
                
                # Store successful query in memory
                self._store_query_log(state, success=True)
                
            else:
                self.logger.error(f"✗ SQL execution failed: {execution_result['error']}")
                state.execution_success = False
                state.error_message = execution_result["error"]
                
                # Step 3: Classify error and route
                error_classification = self.error_router.classify_error(
                    error_message=execution_result["error"],
                    generated_sql=state.generated_sql,
                    table_contexts=state.table_contexts
                )
                
                state.error_category = error_classification["category"]
                
                self.logger.info(f"Error classified as: {state.error_category}")
                
                # Step 4: Decide on routing
                if state.retry_count >= state.max_retries:
                    self.logger.warning(f"Max retries ({state.max_retries}) reached")
                    state.route_to_agent = "complete"
                    # Store failed query for learning
                    self._store_query_log(state, success=False)
                else:
                    # Route to appropriate agent for correction
                    route_decision = self.error_router.route_error(
                        error_category=state.error_category,
                        state=state
                    )
                    
                    state.route_to_agent = route_decision["route_to"]
                    state.correction_summary = route_decision["reason"]
                    state.retry_count += 1
                    
                    self.logger.info(
                        f"Routing to {state.route_to_agent} for correction "
                        f"(retry {state.retry_count}/{state.max_retries})"
                    )
            
            self.log_end(state, success=state.execution_success)
            return state
            
        except Exception as e:
            self.logger.error(f"Execution failed: {e}", exc_info=True)
            self.record_error(state, "execution_error", str(e))
            state.route_to_agent = "complete"
            self.log_end(state, success=False)
            return state
    
    def _execute_sql_safely(self, sql: str) -> Dict[str, Any]:
        """
            Execute SQL with safety measures (timeout, row limit).
        """
        result = {
            "success": False,
            "data": None,
            "error": None,
            "row_count": 0
        }
        
        # Safety: Remove any semicolons
        sql = sql.rstrip(';')
        
        # Wrap query with LIMIT if not present
        sql_upper = sql.upper()
        if 'LIMIT' not in sql_upper:
            sql = f"{sql} LIMIT {self.max_rows}"
        
        try:
            with self.source_db_conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Set statement timeout
                cur.execute(f"SET statement_timeout = {self.timeout_seconds * 1000}")
                
                # Execute query
                cur.execute(sql)
                
                # Fetch results
                rows = cur.fetchall()
                
                result["success"] = True
                result["data"] = [dict(row) for row in rows]
                result["row_count"] = len(rows)
                
                self.logger.info(f"Query returned {result['row_count']} rows")
                
        except psycopg2.Error as e:
            result["error"] = str(e)
            self.logger.error(f"PostgreSQL error: {e}")
            
        except Exception as e:
            result["error"] = str(e)
            self.logger.error(f"Execution error: {e}")
        
        return result
    
    def _store_query_log(self, state: AgentState, success: bool):
        """Store query log in memory for learning"""
        try:
            query_log = {
                "kg_id": str(state.kg_id),
                "user_question": state.user_query,
                "refined_query": state.refined_query,
                "intent_summary": state.intent_summary,
                "selected_tables": state.selected_tables,
                "generated_sql": state.generated_sql,
                "execution_success": success,
                "execution_time_ms": state.execution_time_ms,
                "error_message": state.error_message,
                "error_category": state.error_category,
                "correction_summary": state.correction_summary,
                "tables_used": state.final_tables,
                "correction_applied": state.retry_count > 0,
                "iterations_count": state.retry_count + 1,
                "schema_retrieval_time_ms": state.schema_retrieval_time_ms,
                "sql_generation_time_ms": state.sql_generation_time_ms,
                "confidence_score": state.confidence_score
            }
            
            # Store in repository
            self.memory_repository.insert_query_log(query_log)
            
            self.logger.info("Query log stored successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to store query log: {e}")
            