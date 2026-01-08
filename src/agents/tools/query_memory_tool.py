import logging
from typing import List, Dict, Any, Optional

from openai_client import OpenAIClient
from ...memory.query_memory_repository import QueryMemoryRepository

logger = logging.getLogger(__name__)

class QueryMemoryTool:
    """Retrieves similar past queries for RAG-based SQL generation"""
    
    def __init__(
        self,
        memory_repository: QueryMemoryRepository,
        openai_client: OpenAIClient
    ):
        self.memory_repository = memory_repository
        self.openai_client = openai_client
        
    def get_similar_queries(
        self,
        kg_id: str,
        user_query: str,
        limit: int = 5,
        only_successful: bool = True
    ) -> List[Dict[str, Any]]:
        """   
            Retrieve similar successful queries for few-shot learning.
        """
        logger.info(f"Searching for {limit} similar past queries")
        
        try:
            # Generate embedding for current query
            query_embedding = self.openai_client.generate_embeddings([user_query])[0]
            
            # Search in memory repository
            similar_queries = self.memory_repository.search_similar_queries(
                kg_id=kg_id,
                query_embedding=query_embedding,
                limit=limit,
                only_successful=only_successful
            )
            
            if similar_queries:
                logger.info(f"Found {len(similar_queries)} similar past queries")
                for i, query in enumerate(similar_queries, 1):
                    logger.info(
                        f"  {i}. Query: '{query['user_question'][:50]}...' "
                        f"(similarity: {query.get('similarity', 0):.3f})"
                    )
            else:
                logger.info("No similar past queries found")
            
            return similar_queries
            
        except Exception as e:
            logger.error(f"Failed to retrieve similar queries: {e}")
            return []
        
    def get_error_patterns(
        self,
        kg_id: str,
        error_category: Optional[str] = None,
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
            Retrieve common error patterns to avoid.
        """
        logger.info(f"Retrieving error patterns (category: {error_category})")
        
        try:
            patterns = self.memory_repository.get_error_patterns(
                kg_id=kg_id,
                error_category=error_category,
                limit=limit
            )
            
            if patterns:
                logger.info(f"Found {len(patterns)} error patterns")
                for pattern in patterns:
                    logger.info(
                        f"  - {pattern['error_category']}: {pattern['error_pattern'][:60]}..."
                    )
            else:
                logger.info("No error patterns found")
            
            return patterns
            
        except Exception as e:
            logger.error(f"Failed to retrieve error patterns: {e}")
            return []
        
    def format_examples_for_prompt(
        self,
        similar_queries: List[Dict[str, Any]]
    ) -> str:
        """
            Format similar queries as few-shot examples for LLM prompt.
        """
        if not similar_queries:
            return "No similar past queries available."
        
        examples = []
        for i, query in enumerate(similar_queries, 1):
            example = f"""
                Example {i}:
                User Question: {query['user_question']}
                Selected Tables: {', '.join(query.get('tables_used', []))}
                Generated SQL:
                {query['generated_sql']}
                Result: {'Success' if query['execution_success'] else 'Failed'}
            """
            
            examples.append(example)
        
        return "\n".join(examples)
    