import logging
import hashlib
import time
from uuid import uuid4
from typing import Optional
import psycopg2

from ..models import KnowledgeGraph
from ..extractors import SchemaExtractor
from ..generators import DescriptionGenerator, EmbeddingGenerator
from ..storage import KGRepository, VectorStore


logger = logging.getLogger(__name__)

class KGBuilder:
    """Orchestrates the complete KG building process"""
    def __init__(
        self,
        source_conn,
        kg_conn,
        openai_client,
        settings
    ):
        self.source_conn = source_conn
        self.kg_conn = kg_conn
        self.openai_client = openai_client
        self.settings = settings
        
        # Initialize components
        self.schema_extractor = SchemaExtractor(source_conn, openai_client)
        self.description_generator = DescriptionGenerator(openai_client)
        self.embedding_generator = EmbeddingGenerator(openai_client)
        self.kg_repository = KGRepository(kg_conn)
        self.vector_store = VectorStore(settings.CHROMA_PERSIST_DIR)
        
    def build_kg(
        self,
        source_db_name: str,
        source_db_host: str,
        source_db_port: int,
        schema_name: str = "public",
        generate_descriptions: bool = True,
        generate_embeddings: bool = True
    ) -> Optional[KnowledgeGraph]:
        """Build complete knowledge graph"""
        
        start_time = time.time()
        
        # Create source database hash
        source_db_hash = self._create_db_hash(source_db_host, source_db_port, source_db_name)
        
        # Check if KG already exists
        existing_kg_id = self.kg_repository.get_kg_by_hash(source_db_hash)
        
        if existing_kg_id:
            logger.info(f"KG already exists for this database: {existing_kg_id}")
            logger.info("Loading existing KG...")
            return self.kg_repository.load_kg(existing_kg_id)
        
        # Create new KG
        kg_id = uuid4()
        kg = KnowledgeGraph(
            kg_id=kg_id,
            source_db_host=source_db_host,
            source_db_port=source_db_port,
            source_db_name=source_db_name,
            source_db_hash=source_db_hash,
            status="building"
        )
        
        try:
            logger.info(f"Starting KG build for database: {source_db_name}")
            logger.info(f"KG ID: {kg_id}")
            
            # Insert metadata
            self.kg_repository.insert_kg_metadata(kg)
            
            # PHASE 1: Extract schema 
            logger.info("PHASE 1: Extracting schema metadata")
            phase1_start = time.time()
            
            schema_data = self.schema_extractor.extract_schema(kg_id, schema_name)
            tables = schema_data['tables']
            columns = schema_data['columns']
            relationships = schema_data['relationships']
            
            phase1_duration = time.time() - phase1_start
            logger.info(f"Phase 1 complete in {phase1_duration:.2f}s")
            logger.info(f"Extracted: {len(tables)} tables, {len(columns)} columns, {len(relationships)} relationships")
            
            # PHASE 2: Generate AI descriptions (costs money)
            if generate_descriptions:
                logger.info("PHASE 2: Generating AI descriptions")
                phase2_start = time.time()
                
                # Generate table descriptions
                logger.info("Generating table descriptions...")
                for table in tables:
                    desc_data = self.description_generator.generate_table_description(table)
                    table.description = desc_data['description']
                    table.business_domain = desc_data['business_domain']
                    table.typical_use_cases = desc_data['typical_use_cases']
                    
                # Generate column descriptions (for key columns only)
                logger.info("Generating column descriptions...")
                
                for column in columns:
                    parent_table = next((t for t in tables if t.table_id == column.table_id), None)
                    if parent_table:
                        desc_data = self.description_generator.generate_column_description(column, parent_table)
                        column.description = desc_data['description']
                        column.business_meaning = desc_data['business_meaning']
                        
                        # Update PII detection with AI
                        if not column.is_pii:  # Only if heuristic didn't catch it
                            column.is_pii = self.description_generator.detect_pii(column, parent_table)
                            
                phase2_duration = time.time() - phase2_start
                logger.info(f"Phase 2 complete in {phase2_duration:.2f}s")
                
            else:
                logger.info("Skipping AI description generation")
                
            # PHASE 3: Generate embeddings
            if generate_embeddings:
                logger.info("PHASE 3: Generating embeddings")
                phase3_start = time.time()
                
                # Generate table embeddings
                table_embeddings = self.embedding_generator.generate_table_embeddings(tables)
                
                # Generate column embeddings
                tables_dict = {t.table_name: t for t in tables}
                column_embeddings = self.embedding_generator.generate_column_embeddings(columns, tables_dict)
                
                phase3_duration = time.time() - phase3_start
                logger.info(f"Phase 3 complete in {phase3_duration:.2f}s")
                
            else:
                logger.info("Skipping embedding generation")
                table_embeddings = {}
                column_embeddings = {}
                
            # PHASE 4: Store in PostgreSQL
            logger.info("PHASE 4: Storing in PostgreSQL")
            
            phase4_start = time.time()
            
            self.kg_repository.insert_tables(tables)
            self.kg_repository.insert_columns(columns)
            self.kg_repository.insert_relationships(relationships)
            
            # Store embeddings
            if table_embeddings or column_embeddings:
                embeddings_data = []
                
                # Prepare table embeddings
                for table in tables:
                    if table.table_name in table_embeddings:
                        embeddings_data.append({
                            'kg_id': kg_id,
                            'entity_type': 'table',
                            'entity_id': table.table_id,
                            'text': f"Table: {table.table_name}",
                            'embedding': table_embeddings[table.table_name]
                        })
            
            # Prepare column embeddings
            for column in columns:
                if column.qualified_name in column_embeddings:
                    embeddings_data.append({
                        'kg_id': kg_id,
                        'entity_type': 'column',
                        'entity_id': column.column_id,
                        'text': column.qualified_name,
                        'embedding': column_embeddings[column.qualified_name]
                    })
            
            self.kg_repository.insert_embeddings(embeddings_data)
            
            phase4_duration = time.time() - phase4_start
            logger.info(f"Phase 4 complete in {phase4_duration:.2f}s")
            
            # PHASE 5: Store in Chroma
            if table_embeddings or column_embeddings:
                logger.info("PHASE 5: Storing in Chroma vector store")
                phase5_start = time.time()
                
                collection = self.vector_store.get_or_create_collection(str(kg_id))
                
                if table_embeddings:
                    self.vector_store.add_table_embeddings(collection, tables, table_embeddings)
                
                if column_embeddings:
                    self.vector_store.add_column_embeddings(collection, columns, column_embeddings)
                
                phase5_duration = time.time() - phase5_start
                logger.info(f"Phase 5 complete in {phase5_duration:.2f}s")
            else:
                logger.info("Skipping Chroma storage (no embeddings)")
                
            # Update status to ready
            self.kg_repository.update_kg_status(kg_id, "ready")
            kg.status = "ready"
            
            # Build in-memory KG
            for table in tables:
                kg.add_table(table)
            for rel in relationships:
                kg.add_relationship(rel)
                
            total_duration = time.time() - start_time
            logger.info(f"KG BUILD COMPLETE in {total_duration:.2f}s")
            logger.info(f"KG ID: {kg_id}")
            logger.info(f"Tables: {len(tables)}")
            logger.info(f"Columns: {len(columns)}")
            logger.info(f"Relationships: {len(relationships)}")
            logger.info(f"Table embeddings: {len(table_embeddings)}")
            logger.info(f"Column embeddings: {len(column_embeddings)}")
            
            return kg
            
        except Exception as e:
            logger.error(f"KG build failed: {e}", exc_info=True)
            self.kg_repository.update_kg_status(kg_id, "error", str(e))
            raise
        
    def _create_db_hash(self, host: str, port: int, database: str) -> str:
        """Create unique hash for source database"""
        hash_str = f"{host}:{port}:{database}"
        return hashlib.sha256(hash_str.encode()).hexdigest()
        