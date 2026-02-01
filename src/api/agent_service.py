import logging
import time
from typing import Dict, Any, Optional
from uuid import UUID

from ..kg.manager.kg_manager import KGManager
from ..openai_client import OpenAIClient
from ..memory.query_memory_repository import QueryMemoryRepository
from ..memory.error_summary_manager import ErrorSummaryManager
from ..orchestration.agent_state import AgentState
from ..orchestration.workflow_graph import AgentWorkflow
from ..agents.tools.clarification_tool import ClarificationTool


logger = logging.getLogger(__name__)


class AgentService:
    """
        Main service for text-to-SQL agent system.
    """
    def __init__(
        self,
        kg_manager: KGManager,
        openai_client: OpenAIClient,
        source_db_conn,
        kg_conn
    ):
        self.kg_manager = kg_manager
        self.openai_client = openai_client
        self.source_db_conn = source_db_conn
        self.kg_conn = kg_conn
        
        # Initialize components
        self.memory_repository = QueryMemoryRepository(kg_conn)
        
        # Initialize error summary manager
        self.error_summary_manager = ErrorSummaryManager(kg_conn, openai_client)
        
        # Initialize workflow
        self.workflow = AgentWorkflow(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_db_conn,
            memory_repository=self.memory_repository,
            error_summary_manager=self.error_summary_manager
        )
        self.clarification_tool = ClarificationTool(openai_client)
        
    def query(
        self,
        user_query: str,
        kg_id: UUID,
        clarifications: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
            Process a natural language query and return SQL results.
        """
        logger.info(f"Processing query: '{user_query}'")
        start_time = time.time()
        
        try:
            # Load KG to verify it exists
            kg = self.kg_manager.load_kg(kg_id)
            if not kg:
                return {
                    "success": False,
                    "error": f"Knowledge Graph not found: {kg_id}",
                    "error_type": "kg_not_found"
                }
            
            # Check for ambiguities (if no clarifications provided)
            if not clarifications:
                ambiguity_result = self.clarification_tool.detect_ambiguities(user_query)
                
                if ambiguity_result["has_ambiguity"]:
                    logger.info("Ambiguities detected, requesting clarification")
                    
                    mcq = self.clarification_tool.generate_mcq(
                        user_query=user_query,
                        ambiguities=ambiguity_result["ambiguities"]
                    )
                    
                    return {
                        "success": False,
                        "needs_clarification": True,
                        "clarification_request": {
                            "question": mcq.question,
                            "options": mcq.options,
                            "ambiguity": mcq.detected_ambiguity,
                            "reasoning": ambiguity_result["reasoning"]
                        }
                    }
            
            # Apply clarifications to query if provided
            refined_query = self._apply_clarifications(user_query, clarifications)
            
            logger.info("Loading error summary for KG...")
            error_summary = self.error_summary_manager.get_summary(kg_id)
            
            schema_lessons = error_summary.get("schema_lessons", "") if error_summary else ""
            sql_lessons = error_summary.get("sql_lessons", "") if error_summary else ""
            
            if schema_lessons or sql_lessons:
                lesson_count = error_summary.get("lesson_count", 0)
                word_count = error_summary.get("word_count", 0)
                logger.info(f"Loaded error summary: {lesson_count} lessons, {word_count} words")
            else:
                logger.info("No error lessons available for this KG")
            
            # Create initial state
            initial_state = AgentState(
                kg_id=kg_id,
                user_query=user_query,
                refined_query=refined_query if refined_query != user_query else None,
                clarifications_provided=clarifications or {},
                schema_lessons=schema_lessons,
                sql_lessons=sql_lessons
            )
            
            # Execute workflow
            final_state = self.workflow.execute(initial_state)
            
            # Calculate total time
            total_time_ms = int((time.time() - start_time) * 1000)
            final_state.total_time_ms = total_time_ms
            
            # Format response
            response = self._format_response(final_state)
            
            logger.info(f"Query processing complete in {total_time_ms}ms")
            
            return response
            
        except Exception as e:
            logger.error(f"Query processing failed: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "error_type": "processing_error"
            }
    
    def _apply_clarifications(
        self,
        user_query: str,
        clarifications: Optional[Dict[str, str]]
    ) -> str:
        """Apply clarification responses to refine the query"""
        if not clarifications:
            return user_query
        
        refined = user_query
        
        # Simple substitution for common clarifications
        for question, answer in clarifications.items():
            if "time period" in question.lower() or "month" in question.lower() or "year" in question.lower():
                # Replace vague time reference with specific one
                refined = refined.replace("last month", answer)
                refined = refined.replace("this month", answer)
                refined = refined.replace("last year", answer)
                refined = refined.replace("this year", answer)
                refined = refined.replace("recent", answer)
                refined = refined.replace("recently", answer)
            
            elif "rank" in question.lower() or "metric" in question.lower():
                # Add ranking criteria
                refined += f" (ranked by {answer})"
            
            elif "threshold" in question.lower() or "value" in question.lower():
                # Add threshold criteria
                refined += f" (threshold: {answer})"
                
            else:
                # Generic: append clarification
                refined += f" ({answer})"
        
        logger.info(f"Refined query: '{refined}'")
        return refined
    
    def _format_response(self, state: AgentState) -> Dict[str, Any]:
        """Format final state into API response"""
        
        if state.execution_success:
            return {
                "success": True,
                "data": state.final_result,
                "sql": state.generated_sql,
                "explanation": state.sql_explanation,
                "metadata": {
                    "tables_used": state.final_tables,
                    "confidence_score": state.confidence_score,
                    "iterations": state.retry_count + 1,
                    "is_retry_success": state.is_retry_success,
                    "timing": {
                        "schema_retrieval_ms": state.schema_retrieval_time_ms,
                        "sql_generation_ms": state.sql_generation_time_ms,
                        "execution_ms": state.execution_time_ms,
                        "total_ms": state.total_time_ms
                    }
                }
            }
        else:
            return {
                "success": False,
                "error": state.error_message,
                "error_category": state.error_category,
                "sql_attempted": state.generated_sql,
                "metadata": {
                    "tables_selected": state.final_tables,
                    "iterations": state.retry_count + 1,
                    "error_history": state.error_history,
                    "timing": {
                        "total_ms": state.total_time_ms
                    }
                }
            }
    def submit_feedback(
        self,
        query_log_id: UUID,
        feedback: str,
        rating: Optional[int] = None
    ) -> Dict[str, Any]:
        """
            Submit user feedback for a query result.
        """
        try:
            success = self.memory_repository.update_query_feedback(
                query_log_id=query_log_id,
                feedback=feedback,
                rating=rating
            )
            
            if not success:
                return {"success": False, "error": "Failed to update feedback"}
            
            logger.info("Feedback saved successfully")
            
            # Step 2: Determine if lesson extraction should be triggered
            should_extract_lesson = self._should_extract_lesson_from_feedback(
                feedback=feedback,
                rating=rating
            )
            
            if should_extract_lesson:
                
                logger.info("Feedback indicates issue - triggering lesson extraction")
            
                # Step 3: Retrieve full query log for context
                query_log = self.memory_repository.get_query_log_by_id(query_log_id)
                
                if not query_log:
                    logger.warning(f"Could not retrieve query log {query_log_id} for lesson extraction")
                    return {"success": True, "lesson_extracted": False}
                
                # Step 4: Extract kg_id from query log
                kg_id = query_log.get("kg_id")
                if not kg_id:
                    logger.warning("No kg_id in query log")
                    return {"success": True, "lesson_extracted": False}
                
                # Step 4.5: Retrieve related error patterns for additional context
                error_patterns = self.memory_repository.get_error_patterns_for_query(
                    kg_id=kg_id,
                    error_category=query_log.get("error_category"),
                    affected_tables=query_log.get("tables_used"),
                    limit=3  # Get top 3 most relevant patterns
                )
                
                if error_patterns:
                    logger.info(f"Found {len(error_patterns)} related error patterns")
                else:
                    logger.info("No related error patterns found")
                
                # Step 5: Trigger lesson extraction
                lesson_success = self.error_summary_manager.add_lesson_from_feedback(
                    kg_id=UUID(kg_id),
                    query_log=query_log,
                    feedback=feedback,
                    rating=rating,
                    error_patterns=error_patterns
                )
                
                if lesson_success:
                    logger.info("Lesson extracted and added to error summary")
                    return {"success": True, "lesson_extracted": True}
                else:
                    logger.info("No lesson extracted from feedback")
                    return {"success": True, "lesson_extracted": False}
                
            else:
                logger.info("Positive feedback - no lesson extraction needed")
                return {"success": True, "lesson_extracted": False}
            
        except Exception as e:
            logger.error(f"Failed to submit feedback: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    def _should_extract_lesson_from_feedback(
        self,
        feedback: str,
        rating: Optional[int] = None
    ) -> bool:
        """
            Determine if feedback warrants lesson extraction.
        """
        # Negative emoji feedback
        if feedback in ["Not helpful", "not_helpful", "incorrect"]:
            return True
        
        # Low rating
        if rating is not None and rating <= 2:
            return True
        
        # Positive emoji feedback - skip
        if feedback == "Helpful":
            return False
        
        # Custom text feedback - always analyze (user provided details)
        # Check if feedback is more than just emoji text
        if len(feedback) > 20:  # More than just "Not helpful"
            return True
        
        # Check for negative keywords
        negative_keywords = [
            "wrong", "incorrect", "bad", "error", "missing", 
            "failed", "issue", "problem", "not working"
        ]
        
        feedback_lower = feedback.lower()
        if any(keyword in feedback_lower for keyword in negative_keywords):
            return True
        
        return False   