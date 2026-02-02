import logging
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from langfuse import observe
from langfuse import Langfuse

from ...openai_client import OpenAIClient
from config.settings import Settings

logger = logging.getLogger(__name__)

class TableSelectionOutput(BaseModel):
    """Structured output for table selection"""
    reasoning: str = Field(description="Chain-of-thought reasoning for table selection")
    selected_tables: List[str] = Field(description="List of selected table names")
    confidence: float = Field(description="Confidence score 0.0-1.0")
    
class LLMFilterTool:
    """Uses LLM to filter and select relevant tables with reasoning"""
    
    def __init__(self, openai_client: OpenAIClient):
        self.openai_client = openai_client
        self.setting = Settings()
        
        self.langfuse = Langfuse(
            public_key=self.setting.LANGFUSE_PUBLIC_KEY,
            secret_key=self.setting.LANGFUSE_SECRET_KEY,
            host=self.setting.LANGFUSE_HOST
        )
    
    @observe(
        name="tool_llm_filter_tables",
        as_type="span"
    )  
    def filter_tables(
        self,
        user_query: str,
        candidate_tables: List[Dict[str, Any]],
        kg_context: Dict[str, Any],
        max_tables: int = 5,
        schema_lessons: Optional[str] = None
    ) -> Dict[str, Any]:
        """Filter candidate tables using LLM reasoning."""
        
        self.langfuse.update_current_span(
            input={
                "user_query": user_query,
                "candidate_count": len(candidate_tables),
                "max_tables": max_tables,
                "has_schema_lessons": bool(schema_lessons)
            }
        )
        
        logger.info(f"Filtering {len(candidate_tables)} candidate tables")
        
        # Prepare prompt
        candidates_text = self._format_candidates(candidate_tables)
        
        lessons_section = ""
        if schema_lessons and schema_lessons.strip():
            lessons_section = f"""
                IMPORTANT - Learned Rules from Past Mistakes:
                {schema_lessons}

                Apply these rules when selecting tables. These rules were derived from previous errors and their successful fixes.
            """
            
            logger.info(f"Including {len(schema_lessons.split(chr(10)))} schema lessons in prompt")
        
        prompt = f"""You are a database expert analyzing which tables are needed to answer a user's question.

                    User Query: "{user_query}"

                    Candidate Tables (from vector search):
                    {candidates_text}
                    
                    {lessons_section}

                    Your task:
                    1. Think step-by-step about what data is needed to answer the query
                    2. Select the MINIMUM set of tables required (ideally 2-{max_tables} tables)
                    3. Consider relationships between tables for JOINs
                    4. Provide clear reasoning for each selection

                    Use chain-of-thought reasoning:
                    Thought 1: What entities are mentioned in the query?
                    Thought 2: What data do I need to answer this?
                    Thought 3: Which tables contain this data?
                    Thought 4: Are there relationships between these tables?
                    Action: Select the necessary tables

                    Important:
                    - Include tables needed for meaningful output (names, descriptions, not just IDs)
                    - Consider foreign key relationships
                    - Don't select redundant tables
                """
        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": f"You are a database expert. Analyze queries and select relevant tables with clear reasoning. CRITICAL RULES - YOU MUST FOLLOW THESE:{lessons_section}"
                    },
                    {"role": "user", "content": prompt}
                ],
                response_model=TableSelectionOutput,
                model="gpt-4o",  
                temperature=0.0
            )
            
            logger.info(f"LLM selected {len(result.selected_tables)} tables")
            logger.info(f"Reasoning: {result.reasoning}...")
            logger.info(f"Confidence: {result.confidence}")
            logger.info(f"Selected: {', '.join(result.selected_tables)}")
            
            self.langfuse.update_current_span(
                output={
                    "selected_count": len(result.selected_tables),
                    "selected_tables": result.selected_tables,
                    "confidence": result.confidence
                },
                metadata={
                    "reasoning_length": len(result.reasoning)
                }
            )
            
            return {
                "selected_tables": result.selected_tables,
                "reasoning": result.reasoning,
                "confidence": result.confidence
            }
            
        except Exception as e:
            logger.error(f"LLM filtering failed: {e}")
            # Fallback: return top N tables by score
            fallback_tables = [t["table_name"] for t in candidate_tables[:max_tables]]
            return {
                "selected_tables": fallback_tables,
                "reasoning": f"LLM filtering failed, using top {max_tables} by similarity score",
                "confidence": 0.5
            }
            
    def _format_candidates(self, candidates: List[Dict[str, Any]]) -> str:
        """Format candidate tables for prompt"""
        lines = []
        for i, table in enumerate(candidates, 1):
            lines.append(
                f"{i}. {table['table_name']}\n"
                f"   Domain: {table.get('business_domain', 'N/A')}\n"
                f"   Score: {table.get('similarity_score', 0):.3f}\n"
                f"   Context: {table.get('context', 'N/A')}"
            )
        return "\n\n".join(lines)
    