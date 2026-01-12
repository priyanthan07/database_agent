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
        
        if str(kg_id) in self.loaded_kgs:
            logger.info(f"Returning cached KG: {kg_id}")
            return self.loaded_kgs[str(kg_id)]
        
        # Load from database
        kg = self.kg_repository.load_kg(kg_id)
        
        if kg:
            # Cache it
            self.loaded_kgs[str(kg_id)] = kg
            logger.info(f"Loaded and cached KG: {kg_id}")
        return kg
    
    def get_kg_by_source(self, source_db_hash: str) -> Optional[KnowledgeGraph]:
        """Get KG by source database hash"""
        kg_id = self.kg_repository.get_kg_by_hash(source_db_hash)
        if kg_id:
            return self.load_kg(kg_id)
        return None
    
    def get_vector_collection(self, kg_id: UUID):
        """Get Chroma collection for a KG"""
        return self.vector_store.get_or_create_collection(str(kg_id))
    