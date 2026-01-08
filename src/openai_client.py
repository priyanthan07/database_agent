import logging
from typing import List, Dict, Any, Optional, Type, TypeVar
from pydantic import BaseModel
from openai import OpenAI
from langfuse.openai import openai as langfuse_openai

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseModel)

class OpenAIClient:
    """Wrapper for OpenAI API with Langfuse monitoring."""
    
    def __init__(self, api_key: str, enable_langfuse: bool = True):
        self.api_key = api_key
        self.enable_langfuse = enable_langfuse
        
        if enable_langfuse:
            self.client = langfuse_openai.OpenAI(api_key=api_key)
        else:
            self.client = OpenAI(api_key=api_key)
            
    def generate_completion(
        self,
        messages: List[Dict[str, str]],
        model: str = "gpt-4o-mini",
        temperature: float = 0.0,
        response_format: Optional[Dict] = None
    ) -> str:
        """ Generate chat completion. """
        try:
            response = self.client.chat.completions.create(
                model=model,
                messages=messages,
                temperature=temperature
            )
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"OpenAI completion failed: {e}")
            raise
        
    def generate_structured_completion(
        self,
        messages: List[Dict[str, str]],
        response_model: Type[T],
        model: str = "gpt-4o-mini",
        temperature: float = 0.0,
    ) -> T:
        """Generate chat completion with structured output using Pydantic model"""
        try:
            response = self.client.beta.chat.completions.parse(
                model=model,
                messages=messages,
                temperature=temperature,
                response_format=response_model
            )
            return response.choices[0].message.parsed
            
        except Exception as e:
            logger.error(f"OpenAI structured completion failed: {e}")
            raise
        
    def generate_embeddings(
        self,
        texts: List[str],
        model: str = "text-embedding-3-small"
    ) -> List[List[float]]:
        """
            Generate embeddings for texts.
        """
        
        try:
            response = self.client.embeddings.create(
                model=model,
                input=texts
            )
            return [item.embedding for item in response.data]
            
        except Exception as e:
            logger.error(f"OpenAI embeddings failed: {e}")
            raise
        