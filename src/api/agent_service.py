import logging
import time
from typing import Dict, Any, Optional
from uuid import UUID
from pydantic import BaseModel, Field

from ..kg.manager.kg_manager import KGManager
from ..openai_client import OpenAIClient
from ..memory.query_memory_repository import QueryMemoryRepository
from ..memory.error_summary_manager import ErrorSummaryManager
from ..orchestration.agent_state import AgentState
from ..orchestration.workflow_graph import AgentWorkflow
from ..agents.tools.clarification_tool import ClarificationTool

logger = logging.getLogger(__name__)

class DecisionFormat(BaseModel):
    should_extract_lesson: bool = Field(description="Final decision to extract the lesson")
    reasoning: str = Field(description="Brief explanation of your decision")
    identified_issue_type: str = Field(description="schema/sql/formatting/other")

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
                    "query_log_id": str(state.query_log_id) if state.query_log_id else None,
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
                    "query_log_id": str(state.query_log_id) if state.query_log_id else None,
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
            
            # Step 2: Retrieve full query log for context
            query_log = self.memory_repository.get_query_log_by_id(query_log_id)
            
            if not query_log:
                logger.warning(f"Could not retrieve query log {query_log_id} for lesson extraction")
                return {"success": True, "lesson_extracted": False}
            
            # Step 3: Determine if lesson extraction should be triggered
            should_extract_lesson = self._should_extract_lesson_from_feedback(
                feedback=feedback,
                rating=rating,
                query_log=query_log
            )
            
            if should_extract_lesson:
                
                logger.info("Feedback indicates issue - triggering lesson extraction")
            
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
        rating: Optional[int] = None,
        query_log: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
            Determine if feedback warrants lesson extraction.
        """
        # 1. Skip very short positive responses
        if feedback in ["Helpful", "helpful", "Good", "good", "Great", "great", "👍", "👍🏻"]:
            logger.info("Simple positive emoji - no lesson needed")
            return False
        
        # 2. Always trigger for very low ratings
        if rating is not None and rating == 1:
            logger.info("Rating of 1 - definitely triggering lesson extraction")
            return True
        
        # 3. Skip if feedback is too short to be meaningful
        if len(feedback) < 15:
            logger.info("Feedback too short - no lesson needed")
            return False
        
        # Extract context from query_log if available
        query_success = True
        generated_sql = ""
        user_query = ""
        
        if query_log:
            query_success = query_log.get("execution_success", True)
            generated_sql = query_log.get("generated_sql", "")
            user_query = query_log.get("user_question", "")
        
        # Now use LLM to make intelligent decision
        logger.info("Calling LLM to determine if feedback warrants lesson extraction...")
        
        prompt = f"""You are analyzing user feedback on a Text-to-SQL system to determine if it contains actionable insights that should be used to improve the system.
            **User's Original Query:**
            {user_query if user_query else "Not available"}

            **Generated SQL:**
            ```sql
            {generated_sql[:500] if generated_sql else "Not available"}
            ```

            **Query Execution Status:** {"✅ Successful" if query_success else "❌ Failed"}

            **User's Feedback:**
            "{feedback}"

            **User's Rating:** {rating if rating else "Not provided"} out of 5

            **Your Task:**
            Determine if this feedback contains constructive criticism, improvement suggestions, or identifies issues that the system should learn from.

            **Trigger lesson extraction if:**
            - User points out incorrect results, missing data, or logical errors
            - User suggests better formatting, structure, or data presentation
            - User identifies missing columns, wrong joins, or schema issues
            - User provides constructive criticism even if query succeeded
            - User explains what could be improved or done differently

            **Do NOT trigger if:**
            - Feedback is purely positive with no suggestions ("Great!", "Perfect!")
            - Feedback is generic encouragement without specifics
            - Feedback is unrelated to the query quality

            """

        try:
            response = self.openai_client.generate_structured_completion(
                messages=[{"role": "user", "content": prompt}],
                response_model=DecisionFormat,
                model="gpt-4o-mini",  # Cheap model for classification
                temperature=0.0,
            )
            
            should_extract = response.should_extract_lesson
            reasoning = response.reasoning
            issue_type = response.identified_issue_type
            
            logger.info(f"LLM decision: {'EXTRACT' if should_extract else 'SKIP'}")
            logger.info(f"Reasoning: {reasoning}")
            if should_extract:
                logger.info(f"Issue type: {issue_type}")
            
            return should_extract
            
        except Exception as e:
            logger.error(f"LLM decision failed: {e}")
            # Fallback: if rating ≤2 or long feedback, extract
            fallback_decision = (rating is not None and rating <= 2) or len(feedback) > 50
            logger.info(f"Using fallback decision: {fallback_decision}")
            return fallback_decision