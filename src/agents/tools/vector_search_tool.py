import logging
from typing import List, Dict, Any

from ...kg.manager.kg_manager import KGManager
from openai_client import OpenAIClient

logger = logging.getLogger(__name__)

class VectorSearchTool:
    """Performs vector similarity search for tables"""
    
    def __init__(self, kg_manager: KGManager, openai_client: OpenAIClient):
        self.kg_manager = kg_manager
        self.openai_client = openai_client
        
    def search_tables(
        self,
        kg_id: str,
        query: str,
        k: int = 10
    ) -> List[Dict[str, Any]]:
        """
            Search for relevant tables using vector similarity.
        """
        logger.info(f"Vector search for query: '{query}' (K={k})")
        
        try:
            # Get Chroma collection for this KG
            collection = self.kg_manager.get_vector_collection(kg_id)
            
            # Generate embedding for query
            query_embedding = self.openai_client.generate_embeddings([query])[0]
            
            # Search for similar tables
            results = self.kg_manager.vector_store.search_tables(
                collection,
                query_embedding,
                n_results=k
            )
            
            logger.info(f"Found {len(results)} candidate tables")
            
            # Format results
            formatted_results = []
            for result in results:
                formatted_results.append({
                    "table_name": result["table_name"],
                    "qualified_name": result["qualified_name"],
                    "business_domain": result.get("business_domain", ""),
                    "similarity_score": 1 - result["distance"],  # Convert distance to similarity
                    "distance": result["distance"],
                    "context": result.get("document", "")
                })
            
            # Log top 3 results
            for i, result in enumerate(formatted_results[:3], 1):
                logger.info(
                    f"  {i}. {result['table_name']} "
                    f"(domain: {result['business_domain']}, "
                    f"score: {result['similarity_score']:.3f})"
                )
                
            return formatted_results
            
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            raise
        