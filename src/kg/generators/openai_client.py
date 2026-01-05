import logging
from typing import List, Dict, Any, Optional
from openai import OpenAI
from langfuse.openai import openai as langfuse_openai

logger = logging.getLogger(__name__)

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
            kwargs = {
                "model": model,
                "messages": messages,
                "temperature": temperature
            }
            
            if response_format:
                kwargs["response_format"] = response_format
            
            response = self.client.chat.completions.create(**kwargs)
            return response.choices[0].message.content
            
        except Exception as e:
            logger.error(f"OpenAI completion failed: {e}")
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