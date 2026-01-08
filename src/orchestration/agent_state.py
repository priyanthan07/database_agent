from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime

class ClarificationRequest(BaseModel):
    """MCQ clarification request"""
    question: str
    options: List[str]
    detected_ambiguity: str
    
class AgentState(BaseModel):
    kg_id: UUID
    user_query: str
    query_timestamp: datetime = Field(default_factory=datetime.now)
    
    # Phase 1: Query Understanding & Clarification
    detected_ambiguities: List[str] = Field(default_factory=list)
    clarification_requests: List[ClarificationRequest] = Field(default_factory=list)
    clarifications_provided: Dict[str, str] = Field(default_factory=dict)  # question -> answer
    refined_query: Optional[str] = None
    intent_summary: Optional[str] = None
    
    # Phase 2: Schema Selection (Agent 1)
    vector_search_results: List[Dict[str, Any]] = Field(default_factory=list)
    candidate_tables: List[str] = Field(default_factory=list)  # After vector search
    selected_tables: List[str] = Field(default_factory=list)   # After LLM filtering
    bridging_tables: List[str] = Field(default_factory=list)   # Added by graph traversal
    final_tables: List[str] = Field(default_factory=list)      # All tables to use
    table_contexts: Dict[str, Dict] = Field(default_factory=dict)  # Full KG context per table
    schema_retrieval_time_ms: Optional[int] = None
    
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
    
    # Retry & Error Handling
    retry_count: int = 0
    max_retries: int = 3
    error_history: List[Dict[str, Any]] = Field(default_factory=list)
    route_to_agent: Optional[str] = None  # "agent_1", "agent_2", "agent_3", or "complete"
    
    # Final output
    final_result: Optional[Any] = None
    total_time_ms: Optional[int] = None
    
    class Config:
        arbitrary_types_allowed = True
        