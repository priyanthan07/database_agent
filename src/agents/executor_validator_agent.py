import logging
import time
import psycopg2
from typing import Dict, Any
from psycopg2.extras import RealDictCursor

from .base_agent import BaseAgent
from ..orchestration.agent_state import AgentState
from ..orchestration.error_router import ErrorRouter

logger = logging.getLogger(__name__)

class ExecutorValidatorAgent(BaseAgent):
    """
        Agent 3: Execution & Validation
    """
    
    def __init__(self, kg_manager, openai_client, source_db_conn, memory_repository, error_summary_manager=None):
        super().__init__(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_db_conn,
            agent_name="Executor & Validator Agent"
        )
        
        self.memory_repository = memory_repository
        self.error_summary_manager = error_summary_manager
        self.error_router = ErrorRouter(openai_client=openai_client)
        
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
            
            previous_error = None
            previous_category = None
            if state.retry_count > 0 and state.error_history:
                last_error = state.error_history[-1]
                previous_error = last_error.get("error_message")
                previous_category = last_error.get("error_category")
            
            # Step 1: Execute SQL
            self.logger.info("Step 1: Executing SQL query")
            self.logger.info(f"SQL:\n{state.generated_sql}")
            
            execution_result = self._execute_sql_safely(state.generated_sql)
            
            # Record execution time
            state.execution_time_ms = int((time.time() - start_time) * 1000)
            
            # Step 2: Handle result
            if execution_result["success"]:
                self.logger.info(" SQL execution successful")
                state.execution_success = True
                state.execution_result = execution_result["data"]
                state.final_result = execution_result["data"]
                state.route_to_agent = "complete"
                
                if state.retry_count > 0 and previous_error:
                    state.is_retry_success = True
                    state.previous_error_message = previous_error
                    state.previous_error_category = previous_category
                    state.fix_that_worked = state.correction_summary or "SQL regenerated with corrections"
                    
                    self.logger.info("Retry succeeded! Extracting lesson...")
                    
                    # Extract and store lesson from successful retry
                    self._extract_and_store_lesson(state)
                
                # Store successful query in memory
                self._store_query_log(state, success=True)
                
            else:
                self.logger.error(f" SQL execution failed: {execution_result['error']}")
                state.execution_success = False
                state.error_message = execution_result["error"]
                
                # Step 3: Classify error and route
                error_classification = self.error_router.classify_error(
                    error_message=execution_result["error"],
                    generated_sql=state.generated_sql,
                    table_contexts=state.table_contexts
                )
                
                state.error_category = error_classification["sub_category"]
                
                self.logger.info(f"Error classified as: {error_classification['category']} / {error_classification['sub_category']}")
                
                state.error_history.append({
                    "error_message": state.error_message,
                    "error_category": state.error_category,
                    "sql": state.generated_sql,
                    "retry_count": state.retry_count
                })
                
                # Step 4: Decide on routing
                if state.retry_count >= state.max_retries:
                    self.logger.warning(f"Max retries ({state.max_retries}) reached")
                    state.route_to_agent = "complete"
                    
                    # Store failed query for learning
                    self._store_query_log(state, success=False)
                    
                else:
                    # Route to appropriate agent for correction                    
                    route_decision = self.error_router.route_error(
                        classification=error_classification,
                        state=state
                    )
                    
                    state.route_to_agent = route_decision["route_to"]
                    state.correction_summary = route_decision["reason"]
                    
                    if route_decision["route_to"] != "complete":
                        state.retry_count += 1
                    
                    self.logger.info(
                        f"Routing to {state.route_to_agent} for correction (retry {state.retry_count}/{state.max_retries})"
                    )
                    
                    self.logger.info(f"Priority action: {route_decision.get('priority_action', 'N/A')}")
                    
                    # Store error pattern even during retries
                    self._store_error_pattern(state, error_classification)
            
            self.log_end(state, success=state.execution_success)
            return state
            
        except Exception as e:
            self.logger.error(f"Execution failed: {e}", exc_info=True)
            self.record_error(state, "execution_error", str(e))
            state.route_to_agent = "complete"
            self._store_query_log(state, success=False)
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
            try:
                self.source_db_conn.rollback()
            except:
                pass
            
        except Exception as e:
            result["error"] = str(e)
            self.logger.error(f"Execution error: {e}")
        
        return result
    
    def _extract_and_store_lesson(self, state: AgentState):
        """Extract lesson from successful retry and store in summary"""
        
        if not self.error_summary_manager:
            self.logger.warning("Error summary manager not available, skipping lesson extraction")
            return
        
        try:
            success = self.error_summary_manager.add_lesson_from_error(
                kg_id=state.kg_id,
                error_message=state.previous_error_message or "",
                error_category=state.previous_error_category or "unknown",
                fix_applied=state.fix_that_worked or "",
                affected_tables=state.final_tables or [],
                generated_sql=state.generated_sql or ""
            )
            
            if success:
                self.logger.info("Lesson extracted and added to summary")
            else:
                self.logger.warning("Failed to extract lesson from retry success")
                
        except Exception as e:
            self.logger.error(f"Failed to extract/store lesson: {e}")
            
    def _store_query_log(self, state: AgentState, success: bool):
        """Store query log in memory for learning"""
        try:
            final_query = state.refined_query if state.refined_query else state.user_query
            
            query_log = {
                "kg_id": str(state.kg_id),
                "user_question": state.user_query,
                "refined_query": final_query,
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
                "confidence_score": state.confidence_score,
                "query_embedding": state.query_embedding,
                "user_feedback": state.user_feedback,
                "user_feedback_rating": state.user_feedback_rating
            }
            
            # Store in repository
            success_result = self.memory_repository.insert_query_log(query_log)
            
            if success_result:
                self.logger.info(" Query log stored successfully in kg_query_log table")
            else:
                self.logger.error(" Failed to store query log")
            
        except Exception as e:
            self.logger.error(f"Failed to store query log: {e}")
            
    def _store_error_pattern(self, state: AgentState, classification: Dict[str, Any]):
        """Store error pattern for future learning"""
        try:
            if not state.error_message or not state.error_category:
                self.logger.warning("No error information to store")
                return
            
            # Use LLM classification reasoning as the error pattern
            error_pattern = classification.get("reasoning", "Error occurred during query execution")
            
            # Determine fix applied
            fix_applied = state.correction_summary if state.correction_summary else "Retry with different approach"
            
            # Build error pattern data
            pattern_data = {
                "kg_id": str(state.kg_id),
                "error_category": state.error_category,
                "error_pattern": error_pattern,
                "example_error_message": state.error_message[:500],  # Truncate to 500 chars
                "fix_applied": fix_applied,
                "affected_tables": state.final_tables if state.final_tables else []
            }
            
            # Store in repository
            success_result = self.memory_repository.insert_error_pattern(pattern_data)
            
            if success_result:
                self.logger.info(f"  Error pattern stored successfully in query_error_patterns table")
                self.logger.info(f"   Category: {state.error_category}")
                self.logger.info(f"   Pattern: {error_pattern}")
            else:
                self.logger.error("✗ Failed to store error pattern")
            
        except Exception as e:
            self.logger.error(f"Failed to store error pattern: {e}", exc_info=True)
                