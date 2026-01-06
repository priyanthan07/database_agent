import logging
from typing import List, Dict, Optional
import chromadb
from chromadb.config import Settings

from ..models import Table, Column

logger = logging.getLogger(__name__)

class VectorStore:
    """Chroma vector store for semantic search"""
    
    def __init__(self, persist_directory: str = "./chroma_db"):
        """Initialize Chroma client"""
        logger.info(f"Initializing Chroma vector store at: {persist_directory}")
        
        self.client = chromadb.PersistentClient(
            path=persist_directory,
            settings=Settings(anonymized_telemetry=False)
        )
        
    def get_or_create_collection(self, kg_id: str) -> chromadb.Collection:
        """Get or create collection for a KG"""
        collection_name = f"kg_{kg_id.replace('-', '_')}"
        
        try:
            collection = self.client.get_or_create_collection(
                name=collection_name,
                metadata={"kg_id": kg_id}
            )
            logger.info(f"Using collection: {collection_name}")
            return collection
        except Exception as e:
            logger.error(f"Failed to get/create collection: {e}")
            raise
        
    def add_table_embeddings(
        self,
        collection: chromadb.Collection,
        tables: List[Table],
        embeddings: Dict[str, List[float]]
    ) -> bool:
        """Add table embeddings to collection"""
        logger.info(f"Adding {len(embeddings)} table embeddings to vector store")
        
        ids = []
        vectors = []
        metadatas = []
        documents = []
        
        for table in tables:
            if table.table_name not in embeddings:
                continue
            
            # ID for this embedding
            emb_id = f"table_{table.table_name}"
            ids.append(emb_id)
            
            # Embedding vector
            vectors.append(embeddings[table.table_name])
            
            # Metadata
            metadata = {
                "entity_type": "table",
                "table_name": table.table_name,
                "qualified_name": table.qualified_name,
                "schema_name": table.schema_name,
                "business_domain": table.business_domain or "",
                "row_count": table.row_count_estimate or 0
            }
            metadatas.append(metadata)
            
            # Document text (for retrieval context)
            doc_text = f"Table: {table.table_name}"
            if table.description:
                doc_text += f"\nDescription: {table.description}"
            if table.business_domain:
                doc_text += f"\nDomain: {table.business_domain}"
            documents.append(doc_text)
        
        try:
            collection.add(
                ids=ids,
                embeddings=vectors,
                metadatas=metadatas,
                documents=documents
            )
            logger.info(f"Added {len(ids)} table embeddings successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to add table embeddings: {e}")
            return False
    
    def add_column_embeddings(
        self,
        collection: chromadb.Collection,
        columns: List[Column],
        embeddings: Dict[str, List[float]]
    ) -> bool:
        """Add column embeddings to collection"""
        logger.info(f"Adding {len(embeddings)} column embeddings to vector store")
        
        ids = []
        vectors = []
        metadatas = []
        documents = []
        
        for column in columns:
            if column.qualified_name not in embeddings:
                continue
            
            # ID for this embedding
            emb_id = f"column_{column.qualified_name.replace('.', '_')}"
            ids.append(emb_id)
            
            # Embedding vector
            vectors.append(embeddings[column.qualified_name])
            
            # Metadata
            metadata = {
                "entity_type": "column",
                "qualified_name": column.qualified_name,
                "column_name": column.column_name,
                "data_type": column.data_type,
                "is_pii": column.is_pii,
                "cardinality": column.cardinality or ""
            }
            metadatas.append(metadata)
            
            # Document text
            doc_text = f"Column: {column.qualified_name}"
            if column.description:
                doc_text += f"\nDescription: {column.description}"
            documents.append(doc_text)
        
        try:
            collection.add(
                ids=ids,
                embeddings=vectors,
                metadatas=metadatas,
                documents=documents
            )
            logger.info(f"Added {len(ids)} column embeddings successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to add column embeddings: {e}")
            return False
    
    def search_tables(
        self,
        collection: chromadb.Collection,
        query_embedding: List[float],
        n_results: int = 5
    ) -> List[Dict]:
        """Search for similar tables"""
        try:
            results = collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results,
                where={"entity_type": "table"}
            )
            
            # Format results
            tables = []
            if results['ids'] and results['ids'][0]:
                for i in range(len(results['ids'][0])):
                    tables.append({
                        "table_name": results['metadatas'][0][i]['table_name'],
                        "qualified_name": results['metadatas'][0][i]['qualified_name'],
                        "business_domain": results['metadatas'][0][i].get('business_domain'),
                        "distance": results['distances'][0][i],
                        "document": results['documents'][0][i]
                    })
            
            return tables
        except Exception as e:
            logger.error(f"Failed to search tables: {e}")
            return []
    
    def search_columns(
        self,
        collection: chromadb.Collection,
        query_embedding: List[float],
        n_results: int = 10
    ) -> List[Dict]:
        """Search for similar columns"""
        try:
            results = collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results,
                where={"entity_type": "column"}
            )
            
            # Format results
            columns = []
            if results['ids'] and results['ids'][0]:
                for i in range(len(results['ids'][0])):
                    columns.append({
                        "qualified_name": results['metadatas'][0][i]['qualified_name'],
                        "column_name": results['metadatas'][0][i]['column_name'],
                        "data_type": results['metadatas'][0][i]['data_type'],
                        "distance": results['distances'][0][i],
                        "document": results['documents'][0][i]
                    })
            
            return columns
        except Exception as e:
            logger.error(f"Failed to search columns: {e}")
            return []