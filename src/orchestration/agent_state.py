from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime

class ClarificationRequest(BaseModel):
    """MCQ clarification request"""
    
    clarification_type: str = "mcq"  # "mcq" | "yes_no" | "suggestion" | "open_text"
    question: str
    options: List[str] = Field(default_factory=list)  # For MCQ type
    suggested_action: Optional[str] = None          # For suggestion type: system auto-proceeds with this if user doesn't object
    suggested_interpretation: Optional[str] = None
    proposed_interpretation: Optional[str] = None   # For yes_no type
    detected_ambiguity: str = ""
    trigger_phase: str = "pre_schema"  # "pre_schema" | "post_schema" | "error_retry"
    
class AgentState(BaseModel):
    kg_id: UUID
    user_query: str
    query_timestamp: datetime = Field(default_factory=datetime.now)
    query_embedding: Optional[List[float]] = None
    
    # Phase 1: Query Understanding & Clarification
    detected_ambiguities: List[str] = Field(default_factory=list)
    clarification_requests: List[ClarificationRequest] = Field(default_factory=list)
    clarifications_provided: Dict[str, str] = Field(default_factory=dict)  # question -> answer
    refined_query: Optional[str] = None
    intent_summary: Optional[str] = None
    
    schema_lessons: Optional[str] = None # Lessons for Agent 1 (table selection)
    sql_lessons: Optional[str] = None # Lessons for Agent 2 (SQL generation)
    
    # Phase 2: Schema Selection (Agent 1)
    vector_search_results: List[Dict[str, Any]] = Field(default_factory=list)
    candidate_tables: List[str] = Field(default_factory=list)  # After vector search
    selected_tables: List[str] = Field(default_factory=list)   # After LLM filtering
    bridging_tables: List[str] = Field(default_factory=list)   # Added by graph traversal
    enrichment_tables: List[str] = Field(default_factory=list) # Added by graph traversal
    final_tables: List[str] = Field(default_factory=list)      # All tables to use
    table_contexts: Dict[str, Dict] = Field(default_factory=dict)  # Full KG context per table
    schema_retrieval_time_ms: Optional[int] = None
    
    # Schema-aware clarification (Phase B) 
    needs_schema_clarification: bool = False
    schema_clarification_request: Optional[ClarificationRequest] = None
    
    # Error retry context - passed when Agent 3 routes back
    retry_error_context: Optional[str] = None  # Error message from failed attempt
    retry_error_category: Optional[str] = None  # Error category from failed attempt
    
    # Phase 3: SQL Generation (Agent 2)
    similar_past_queries: List[Dict[str, Any]] = Field(default_factory=list)
    generated_sql: Optional[str] = None
    sql_explanation: Optional[str] = None
    confidence_score: Optional[float] = None
    sql_generation_time_ms: Optional[int] = None
    generation_reasoning: Optional[str] = None  # Chain-of-thought
    
    # Phase 4: Execution & Validation (Agent 3)
    execution_result: Optional[Any] = None
    execution_success: bool = False
    execution_time_ms: Optional[int] = None
    error_message: Optional[str] = None
    error_category: Optional[str] = None       # syntax_error, column_not_found, etc.
    correction_summary: Optional[str] = None
    
    query_log_id: Optional[str] = None
    
    # Retry & Error Handling
    retry_count: int = 0
    max_retries: int = 3
    error_history: List[Dict[str, Any]] = Field(default_factory=list)
    route_to_agent: Optional[str] = None  # "agent_1", "agent_2", "agent_3", "clarification_needed" or "complete"
    
    is_retry_success: bool = False
    previous_error_message: Optional[str] = None
    previous_error_category: Optional[str] = None
    fix_that_worked: Optional[str] = None
    
    # User Feedback (for learning)
    user_feedback: Optional[str] = None
    user_feedback_rating: Optional[int] = None
    
    # Final output
    final_result: Optional[Any] = None
    total_time_ms: Optional[int] = None
    
    class Config:
        arbitrary_types_allowed = True
        