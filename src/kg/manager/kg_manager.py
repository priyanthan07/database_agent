import logging
from typing import Optional
from uuid import UUID

from ..storage import KGRepository, VectorStore
from ..models import KnowledgeGraph

logger = logging.getLogger(__name__)

class KGManager:
    def __init__(self, kg_conn, chroma_persist_dir: str):
        self.kg_repository = KGRepository(kg_conn)
        self.vector_store = VectorStore(chroma_persist_dir)
        self.loaded_kgs = {}
        
    def load_kg(self, kg_id: UUID) -> Optional[KnowledgeGraph]:
        """Load KG from PostgreSQL into memory"""
        
        kg_id_str = str(kg_id)
        
        if kg_id_str in self.loaded_kgs:
            logger.info(f"Returning cached KG: {kg_id}")
            self._ensure_vector_store_ready(kg_id_str)
            return self.loaded_kgs[kg_id_str]
        
        # Load from database
        kg = self.kg_repository.load_kg(kg_id)
        
        if kg:
            # Ensure vector store has embeddings
            vector_ready = self._ensure_vector_store_ready(kg_id_str)
            
            if not vector_ready:
                logger.warning(f"Vector store not ready for KG {kg_id}, but KG loaded")
        
            # Cache it
            self.loaded_kgs[kg_id_str] = kg
            logger.info(f"Loaded and cached KG: {kg_id}")
            
        return kg
    
    def _ensure_vector_store_ready(self, kg_id: str) -> bool:
        """Ensure vector store collection has embeddings loaded"""
        try:
            # Use the kg_repository's connection for loading embeddings
            return self.vector_store.ensure_collection_loaded(
                kg_id,
                self.kg_repository.conn
            )
        except Exception as e:
            logger.error(f"Failed to ensure vector store ready: {e}")
            return False
    
    def get_kg_by_source(self, source_db_hash: str) -> Optional[KnowledgeGraph]:
        """Get KG by source database hash"""
        kg_id = self.kg_repository.get_kg_by_hash(source_db_hash)
        if kg_id:
            return self.load_kg(kg_id)
        return None
    
    def get_vector_collection(self, kg_id: UUID):
        """Get Chroma collection for a KG"""
        return self.vector_store.get_or_create_collection(str(kg_id))
    