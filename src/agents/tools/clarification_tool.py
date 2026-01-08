import logging
from typing import List, Dict, Optional
from pydantic import BaseModel, Field

from ...openai_client import OpenAIClient
from ...orchestration.agent_state import ClarificationRequest

logger = logging.getLogger(__name__)

class AmbiguityDetectionOutput(BaseModel):
    """Structured output for ambiguity detection"""
    has_ambiguity: bool = Field(description="Whether the query has ambiguities")
    ambiguities: List[str] = Field(default_factory=list, description="List of specific ambiguities detected")
    reasoning: str = Field(description="Explanation of why query is ambiguous or clear")

class MCQGenerationOutput(BaseModel):
    """Structured output for MCQ generation"""
    question: str = Field(description="Clear clarification question")
    options: List[str] = Field(description="3-4 specific, concrete options")
    default_option: Optional[str] = Field(
        default=None,
        description="Recommended default option if applicable"
    )
    
class ClarificationTool:
    """Detects ambiguities in queries and generates MCQ clarifications"""
    
    def __init__(self, openai_client: OpenAIClient):
        self.openai_client = openai_client
        
    def detect_ambiguities(self, user_query: str) -> Dict:
        """
            Detect ambiguities in user query using pattern matching and LLM.
        """
        logger.info(f"Detecting ambiguities in query: '{user_query}'")
        
        prompt = f"""Analyze this database query for ambiguities that would prevent accurate SQL generation.

            User Query: "{user_query}"

            Identify any ambiguities such as:
            - Vague time references (e.g., "last month", "recently", "this year")
            - Unclear ranking criteria (e.g., "top products" - by what metric?)
            - Undefined thresholds (e.g., "high value customers" - what's the threshold?)
            - Ambiguous aggregation scope (e.g., "average sales" - per what period/group?)
            - Multiple possible interpretations of the query

            Be conservative - only flag genuine ambiguities that would lead to incorrect SQL.

            If ambiguous:
            - Set has_ambiguity = true
            - List each specific ambiguity in the ambiguities array
            - Explain your reasoning

            If clear:
            - Set has_ambiguity = false
            - Explain why the query is unambiguous in reasoning
        """

        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": "You are a database expert analyzing queries for ambiguities. Be precise and conservative in your analysis."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_model=AmbiguityDetectionOutput,
                model="gpt-4o-mini",
                temperature=0.0
            )
            
            if result.has_ambiguity:
                logger.info(f"Detected {len(result.ambiguities)} ambiguities")
                logger.info(f"Ambiguities: {result.ambiguities}")
                logger.info(f"Reasoning: {result.reasoning}")
            else:
                logger.info(f"No ambiguities detected: {result.reasoning}")
            
            return {
                "has_ambiguity": result.has_ambiguity,
                "ambiguities": result.ambiguities,
                "reasoning": result.reasoning,
                "clarification_needed": result.has_ambiguity
            }
            
        except Exception as e:
            logger.error(f"Ambiguity detection failed: {e}")
            # On failure, assume no ambiguity to not block the query
            return {
                "has_ambiguity": False,
                "ambiguities": [],
                "reasoning": f"Detection failed: {e}",
                "clarification_needed": False
            }
    
    def generate_mcq(
        self,
        user_query: str,
        ambiguities: List[str]
    ) -> ClarificationRequest:
        """
            Generate MCQ clarification for detected ambiguities using LLM.
        """
        logger.info(f"Generating MCQ for ambiguities: {ambiguities}")
        
        # Format ambiguities for prompt
        ambiguities_text = "\n".join(f"- {amb}" for amb in ambiguities)
        
        prompt = f"""Generate a multiple-choice question to clarify the ambiguities in this database query.

            User Query: "{user_query}"

            Detected Ambiguities:
            {ambiguities_text}

            Create a clarification question with 3-4 specific, concrete, mutually exclusive options.

            Guidelines:
            - Focus on the MOST CRITICAL ambiguity if there are multiple
            - Make the question clear and direct
            - Options should be specific and actionable (e.g., "December 2025" not "last month")
            - Options should cover the most likely interpretations
            - Avoid vague or overlapping options

            Examples of good options:
            - Time: ["December 2025", "November 2025", "December 2024", "Last 30 days"]
            - Ranking: ["By Total Revenue", "By Quantity Sold", "By Number of Orders", "By Average Rating"]
            - Threshold: ["Greater than $1,000", "Greater than $5,000", "Greater than $10,000", "Top 10%"]
        """

        try:
            result = self.openai_client.generate_structured_completion(
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful assistant generating clarification questions for database queries. Make questions clear and options specific."
                    },
                    {"role": "user", "content": prompt}
                ],
                response_model=MCQGenerationOutput,
                model="gpt-4o-mini",
                temperature=0.3
            )
            
            logger.info(f"Generated question: {result.question}")
            logger.info(f"Options: {result.options}")
            
            return ClarificationRequest(
                question=result.question,
                options=result.options,
                detected_ambiguity=", ".join(ambiguities)
            )
            
        except Exception as e:
            logger.error(f"MCQ generation failed: {e}")
            # Fallback to basic clarification
            return ClarificationRequest(
                question=f"The query is ambiguous. Please clarify: {ambiguities[0] if ambiguities else 'your intent'}",
                options=["Option 1", "Option 2", "Option 3"],
                detected_ambiguity=", ".join(ambiguities)
            )
        