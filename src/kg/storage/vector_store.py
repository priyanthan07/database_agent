import logging
from typing import List, Dict, Optional
import chromadb
from chromadb.config import Settings
import json
from psycopg2.extras import RealDictCursor
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
        
    def ensure_collection_loaded(
        self,
        kg_id: str,
        kg_conn
    ) -> bool:
        """
            Ensure Chroma collection has embeddings loaded.
            If collection is empty, load from PostgreSQL.
            
            Returns:
                True if collection has data, False otherwise
        """
        collection = self.get_or_create_collection(kg_id)
        count = collection.count()
        
        logger.info(f"Chroma collection for {kg_id} has {count} embeddings")
        
        if count == 0:
            logger.warning(f"Chroma collection is empty, loading from PostgreSQL...")
            return self._load_embeddings_from_postgres(kg_id, kg_conn, collection)
        
        return True
    
    def _load_embeddings_from_postgres(
        self,
        kg_id: str,
        kg_conn,
        collection
    ) -> bool:
        """
        Load embeddings from PostgreSQL into Chroma.
        
        CRITICAL: Must produce EXACT SAME structure as add_table_embeddings/add_column_embeddings
        to ensure vector search works identically whether KG was built fresh or loaded.
        """
        
        try:
            # Step 1: Load TABLE embeddings with full context
            table_data = self._load_table_embeddings_data(kg_id, kg_conn)
            
            if table_data:
                logger.info(f"Loading {len(table_data)} table embeddings into Chroma...")
                
                # Build in EXACT format as add_table_embeddings()
                ids = []
                vectors = []
                metadatas = []
                documents = []
                
                for item in table_data:
                    # EXACT ID format from add_table_embeddings
                    ids.append(f"table_{item['table_name']}")
                    
                    # Vector
                    vectors.append(item['embedding'])
                    
                    # EXACT metadata structure from add_table_embeddings
                    metadatas.append({
                        "entity_type": "table",
                        "table_name": item['table_name'],
                        "qualified_name": item['qualified_name'],
                        "schema_name": item['schema_name'],
                        "business_domain": item['business_domain'] or "",
                        "row_count": item['row_count'] or 0
                    })
                    
                    # EXACT document format from add_table_embeddings
                    doc_text = f"Table: {item['table_name']}"
                    if item['description']:
                        doc_text += f"\nDescription: {item['description']}"
                    if item['business_domain']:
                        doc_text += f"\nDomain: {item['business_domain']}"
                    documents.append(doc_text)
                
                # Add to Chroma in batches (same as build)
                batch_size = 100
                for i in range(0, len(ids), batch_size):
                    batch_ids = ids[i:i+batch_size]
                    batch_vectors = vectors[i:i+batch_size]
                    batch_metadatas = metadatas[i:i+batch_size]
                    batch_documents = documents[i:i+batch_size]
                    
                    collection.add(
                        ids=batch_ids,
                        embeddings=batch_vectors,
                        metadatas=batch_metadatas,
                        documents=batch_documents
                    )
                
                logger.info(f"✓ Loaded {len(table_data)} table embeddings")
            
            # Step 2: Load COLUMN embeddings with full context
            column_data = self._load_column_embeddings_data(kg_id, kg_conn)
            
            if column_data:
                logger.info(f"Loading {len(column_data)} column embeddings into Chroma...")
                
                # Build in EXACT format as add_column_embeddings()
                ids = []
                vectors = []
                metadatas = []
                documents = []
                
                for item in column_data:
                    # EXACT ID format from add_column_embeddings
                    # Format: "column_{qualified_name with dots replaced by underscores}"
                    qualified_name = item['qualified_name']
                    ids.append(f"column_{qualified_name.replace('.', '_')}")
                    
                    # Vector
                    vectors.append(item['embedding'])
                    
                    # EXACT metadata structure from add_column_embeddings
                    metadatas.append({
                        "entity_type": "column",
                        "qualified_name": item['qualified_name'],
                        "column_name": item['column_name'],
                        "data_type": item['data_type'],
                        "is_pii": item['is_pii'],
                        "cardinality": item['cardinality'] or ""
                    })
                    
                    # EXACT document format from add_column_embeddings
                    doc_text = f"Column: {item['qualified_name']}"
                    if item['description']:
                        doc_text += f"\nDescription: {item['description']}"
                    documents.append(doc_text)
                
                # Add to Chroma in batches
                batch_size = 100
                for i in range(0, len(ids), batch_size):
                    batch_ids = ids[i:i+batch_size]
                    batch_vectors = vectors[i:i+batch_size]
                    batch_metadatas = metadatas[i:i+batch_size]
                    batch_documents = documents[i:i+batch_size]
                    
                    collection.add(
                        ids=batch_ids,
                        embeddings=batch_vectors,
                        metadatas=batch_metadatas,
                        documents=batch_documents
                    )
                
                logger.info(f"✓ Loaded {len(column_data)} column embeddings")
            
            # Verify final count
            final_count = collection.count()
            expected_count = len(table_data or []) + len(column_data or [])
            
            if final_count != expected_count:
                logger.warning(f"Count mismatch: expected {expected_count}, got {final_count}")
            else:
                logger.info(f"✓ Vector store verified: {final_count} embeddings loaded correctly")
            
            return final_count > 0
            
        except Exception as e:
            logger.error(f"Failed to load embeddings from PostgreSQL: {e}", exc_info=True)
            return False
    
    def _load_table_embeddings_data(self, kg_id: str, kg_conn) -> List[Dict]:
        """
        Load table embeddings data from PostgreSQL with ALL required fields.
        Returns list matching the structure used in add_table_embeddings.
        """
        
        query = """
            SELECT 
                t.table_name,
                t.qualified_name,
                t.schema_name,
                t.business_domain,
                t.row_count_estimate,
                t.description,
                e.embedding_vector
            FROM kg_embeddings e
            JOIN kg_tables t ON e.entity_id = t.table_id
            WHERE e.kg_id = %s
                AND e.entity_type = 'table'
            ORDER BY t.table_name
        """
        
        try:      
            with kg_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (kg_id,))
                results = cur.fetchall()
            
            table_data = []
            for row in results:
                # Parse embedding from bytes
                embedding_bytes = row['embedding_vector']
                
                # Handle different byte formats
                if isinstance(embedding_bytes, memoryview):
                    embedding_bytes = embedding_bytes.tobytes()
                elif isinstance(embedding_bytes, str):
                    embedding_bytes = embedding_bytes.encode('utf-8')
                
                embedding_list = json.loads(embedding_bytes.decode('utf-8'))
                
                table_data.append({
                            'table_name': row['table_name'],
                            'qualified_name': row['qualified_name'],
                    'schema_name': row['schema_name'],
                    'business_domain': row['business_domain'],
                    'row_count': row['row_count_estimate'],
                    'description': row['description'],
                    'embedding': embedding_list
                })
            
            logger.info(f"Fetched {len(table_data)} table embeddings from PostgreSQL")
            return table_data
            
        except Exception as e:
            logger.error(f"Failed to load table embeddings data: {e}", exc_info=True)
            return []
    
    def _load_column_embeddings_data(self, kg_id: str, kg_conn) -> List[Dict]:
        """
        Load column embeddings data from PostgreSQL with ALL required fields.
        Returns list matching the structure used in add_column_embeddings.
        """
        
        query = """
            SELECT 
                c.qualified_name,
                c.column_name,
                c.data_type,
                c.is_pii,
                c.cardinality,
                c.description,
                e.embedding_vector
            FROM kg_embeddings e
            JOIN kg_columns c ON e.entity_id = c.column_id
            WHERE e.kg_id = %s 
                AND e.entity_type = 'column'
            ORDER BY c.qualified_name
        """
        
        try:
            with kg_conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (kg_id,))
                results = cur.fetchall()
            
            column_data = []
            for row in results:
                # Parse embedding from bytes
                embedding_bytes = row['embedding_vector']
                
                # Handle different byte formats
                if isinstance(embedding_bytes, memoryview):
                    embedding_bytes = embedding_bytes.tobytes()
                elif isinstance(embedding_bytes, str):
                    embedding_bytes = embedding_bytes.encode('utf-8')
                
                embedding_list = json.loads(embedding_bytes.decode('utf-8'))
                
                column_data.append({
                    'qualified_name': row['qualified_name'],
                    'column_name': row['column_name'],
                    'data_type': row['data_type'],
                    'is_pii': row['is_pii'],
                    'cardinality': row['cardinality'],
                    'description': row['description'],
                    'embedding': embedding_list
                })
            
            logger.info(f"Fetched {len(column_data)} column embeddings from PostgreSQL")
            return column_data
            
        except Exception as e:
            logger.error(f"Failed to load column embeddings data: {e}", exc_info=True)
            return []
        