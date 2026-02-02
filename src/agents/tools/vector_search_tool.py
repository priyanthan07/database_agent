import logging
from typing import List, Dict, Any
from langfuse import observe
from langfuse import Langfuse

from ...kg.manager.kg_manager import KGManager
from ...openai_client import OpenAIClient
from config.settings import Settings

logger = logging.getLogger(__name__)

class VectorSearchTool:
    """Performs vector similarity search for tables"""
    
    def __init__(self, kg_manager: KGManager, openai_client: OpenAIClient):
        self.kg_manager = kg_manager
        self.openai_client = openai_client
        
        self.setting = Settings()
        
        self.langfuse = Langfuse(
            public_key=self.setting.LANGFUSE_PUBLIC_KEY,
            secret_key=self.setting.LANGFUSE_SECRET_KEY,
            host=self.setting.LANGFUSE_HOST
        )
    
    @observe(
        name="tool_vector_search_tables",
        as_type="span"
    )  
    def search_tables(
        self,
        kg_id: str,
        query_embedding: str,
        k: int = 10
    ) -> List[Dict[str, Any]]:
        """
            Search for relevant tables using vector similarity.
        """
        self.langfuse.update_current_span(
            input={
                "kg_id": kg_id,
                "k": k
            },
            metadata={
                "embedding_dim": len(query_embedding)
            }
        )
        
        logger.info(f"Vector search for query. (K={k})")
        
        try:
            # Get Chroma collection for this KG
            collection = self.kg_manager.get_vector_collection(kg_id)

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
            for i, result in enumerate(formatted_results, 1):
                logger.info(
                    f"  {i}. {result['table_name']} "
                    f"(domain: {result['business_domain']}, "
                    f"score: {result['similarity_score']:.3f})"
                )
                
            self.langfuse.update_current_span(
                output={
                    "results_count": len(formatted_results),
                    "top_table": formatted_results[0]["table_name"] if formatted_results else None,
                    "top_similarity_score": formatted_results[0]["similarity_score"] if formatted_results else None
                },
                metadata={
                    "avg_similarity": sum(r["similarity_score"] for r in formatted_results) / len(formatted_results) if formatted_results else 0
                }
            )
                
            return formatted_results
            
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            raise
        