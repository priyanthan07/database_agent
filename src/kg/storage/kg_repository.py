import logging
from typing import List, Dict, Optional
from uuid import UUID
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import json

from ..models import KnowledgeGraph, Table, Column, Relationship

logger = logging.getLogger(__name__)

class KGRepository:
    """PostgreSQL repository for knowledge graph persistence"""
    
    def __init__(self, connection):
        self.conn = connection
        
    def insert_kg_metadata(self, kg: KnowledgeGraph) -> bool:
        """Insert KG metadata"""
        logger.info(f"Inserting KG metadata for kg_id={kg.kg_id}")
        
        query = """
            INSERT INTO kg_metadata (
                kg_id, source_db_host, source_db_port, source_db_name,
                source_db_hash, status, version, created_at, last_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_db_hash) DO UPDATE
            SET last_updated = EXCLUDED.last_updated,
                version = kg_metadata.version + 1
            RETURNING kg_id
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (
                    str(kg.kg_id),
                    kg.source_db_host,
                    kg.source_db_port,
                    kg.source_db_name,
                    kg.source_db_hash,
                    kg.status,
                    1,
                    kg.created_at,
                    kg.last_updated
                ))
                self.conn.commit()
                logger.info(f"Inserted KG metadata successfully")
                return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert KG metadata: {e}")
            return False
        
    def insert_tables(self, tables: List[Table]) -> bool:
        """Batch insert tables"""
        if not tables:
            return True
            
        logger.info(f"Inserting {len(tables)} tables")
        
        query = """
            INSERT INTO kg_tables (
                table_id, kg_id, table_name, schema_name, qualified_name,
                table_type, row_count_estimate, description, business_domain,
                typical_use_cases
            ) VALUES %s
            ON CONFLICT (kg_id, qualified_name) DO UPDATE
            SET description = EXCLUDED.description,
                business_domain = EXCLUDED.business_domain,
                typical_use_cases = EXCLUDED.typical_use_cases,
                updated_at = CURRENT_TIMESTAMP
        """
        
        values = [
            (
                str(t.table_id),
                str(t.kg_id),
                t.table_name,
                t.schema_name,
                t.qualified_name,
                t.table_type,
                t.row_count_estimate,
                t.description,
                t.business_domain,
                json.dumps(t.typical_use_cases) if t.typical_use_cases else None
            )
            for t in tables
        ]
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, values)
                self.conn.commit()
                logger.info(f"Inserted {len(tables)} tables successfully")
                return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert tables: {e}")
            return False
        
    def insert_columns(self, columns: List[Column]) -> bool:
        """Batch insert columns"""
        if not columns:
            return True
            
        logger.info(f"Inserting {len(columns)} columns")
        
        query = """
            INSERT INTO kg_columns (
                column_id, table_id, column_name, qualified_name, data_type,
                is_nullable, is_primary_key, is_unique, is_foreign_key,
                column_position, description, business_meaning, sample_values,
                enum_values, cardinality, null_percentage, is_pii
            ) VALUES %s
            ON CONFLICT (table_id, column_name) DO UPDATE
            SET description = EXCLUDED.description,
                business_meaning = EXCLUDED.business_meaning,
                sample_values = EXCLUDED.sample_values,
                enum_values = EXCLUDED.enum_values,
                cardinality = EXCLUDED.cardinality,
                null_percentage = EXCLUDED.null_percentage,
                is_pii = EXCLUDED.is_pii,
                updated_at = CURRENT_TIMESTAMP
        """
        
        values = [
            (
                str(c.column_id),
                str(c.table_id),
                c.column_name,
                c.qualified_name,
                c.data_type,
                c.is_nullable,
                c.is_primary_key,
                c.is_unique,
                c.is_foreign_key,
                c.column_position,
                c.description,
                c.business_meaning,
                json.dumps(c.sample_values) if c.sample_values else None,
                json.dumps(c.enum_values) if c.enum_values else None,
                c.cardinality,
                c.null_percentage,
                c.is_pii
            )
            for c in columns
        ]
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, values)
                self.conn.commit()
                logger.info(f"Inserted {len(columns)} columns successfully")
                return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert columns: {e}")
            return False
        
    def insert_relationships(self, relationships: List[Relationship]) -> bool:
        """Batch insert relationships"""
        if not relationships:
            return True
            
        logger.info(f"Inserting {len(relationships)} relationships")
        
        query = """
            INSERT INTO kg_relationships (
                relationship_id, kg_id, from_table_id, to_table_id,
                from_column, to_column, relationship_type, constraint_name,
                join_condition, business_meaning, is_self_reference
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """
        
        values = [
            (
                str(r.relationship_id),
                str(r.kg_id),
                str(r.from_table_id),
                str(r.to_table_id),
                r.from_column,
                r.to_column,
                r.relationship_type,
                r.constraint_name,
                r.join_condition,
                r.business_meaning,
                r.is_self_reference
            )
            for r in relationships
        ]
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, values)
                self.conn.commit()
                logger.info(f"Inserted {len(relationships)} relationships successfully")
                return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert relationships: {e}")
            return False
        
    def insert_embeddings(self, embeddings_data: List[Dict]) -> bool:
        """Batch insert embeddings"""
        if not embeddings_data:
            return True
            
        logger.info(f"Inserting {len(embeddings_data)} embeddings")
        
        query = """
            INSERT INTO kg_embeddings (
                embedding_id, kg_id, entity_type, entity_id,
                embedding_text, embedding_vector, embedding_model, vector_dimension
            ) VALUES %s
            ON CONFLICT (entity_type, entity_id) DO UPDATE
            SET embedding_vector = EXCLUDED.embedding_vector,
                embedding_text = EXCLUDED.embedding_text
        """
        
        # Convert embeddings to bytes for storage
        from uuid import uuid4
        values = [
            (
                str(uuid4()),
                str(emb['kg_id']),
                emb['entity_type'],
                str(emb['entity_id']),
                emb['text'],
                json.dumps(emb['embedding']).encode('utf-8'),  # Store as bytes
                emb.get('model', 'text-embedding-3-small'),
                len(emb['embedding'])
            )
            for emb in embeddings_data
        ]
        
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, query, values)
                self.conn.commit()
                logger.info(f"Inserted {len(embeddings_data)} embeddings successfully")
                return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to insert embeddings: {e}")
            return False
        
    def update_kg_status(self, kg_id: UUID, status: str, error_message: Optional[str] = None) -> bool:
        """Update KG build status"""
        logger.info(f"Updating KG status to '{status}' for kg_id={kg_id}")
        
        query = """
            UPDATE kg_metadata
            SET status = %s,
                error_message = %s
            WHERE kg_id = %s
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (status, error_message, str(kg_id)))
                self.conn.commit()
                return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to update KG status: {e}")
            return False
        
    def get_kg_by_hash(self, source_db_hash: str) -> Optional[UUID]:
        """Get KG ID by source database hash"""
        query = """
            SELECT kg_id FROM kg_metadata
            WHERE source_db_hash = %s
            ORDER BY version DESC
            LIMIT 1
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (source_db_hash,))
                result = cur.fetchone()
                return UUID(result[0]) if result else None
        except Exception as e:
            logger.error(f"Failed to get KG by hash: {e}")
            return None
        
    def load_kg(self, kg_id: UUID) -> Optional[KnowledgeGraph]:
        """Load complete KG from database into memory"""
        logger.info(f"Loading KG from database: kg_id={kg_id}")
        
        try:
            # Load metadata
            kg_meta = self._load_kg_metadata(kg_id)
            if not kg_meta:
                return None
            
            # Load tables
            tables = self._load_tables(kg_id)
            
            # Load columns
            columns = self._load_columns(kg_id)
            
            # Load relationships
            relationships = self._load_relationships(kg_id)
            
            # Build KG object
            kg = KnowledgeGraph(**kg_meta)
            
            # Organize columns by table
            columns_by_table = {}
            for col in columns:
                table_id = col['table_id']
                if table_id not in columns_by_table:
                    columns_by_table[table_id] = []
                columns_by_table[table_id].append(col)
            
            # Build tables with columns
            for table_data in tables:
                table = Table(**table_data)
                
                # Add columns to table
                table_columns = columns_by_table.get(str(table.table_id), [])
                for col_data in table_columns:
                    col = Column(**col_data)
                    table.columns[col.column_name] = col
                
                kg.add_table(table)
            
            # Add relationships
            for rel_data in relationships:
                rel = Relationship(**rel_data)
                kg.add_relationship(rel)
            
            logger.info(f"Loaded KG: {len(kg.tables)} tables, {len(kg.relationships)} relationships")
            return kg
            
        except Exception as e:
            logger.error(f"Failed to load KG: {e}")
            return None
    
    def _load_kg_metadata(self, kg_id: UUID) -> Optional[Dict]:
        """Load KG metadata"""
        query = """
            SELECT kg_id, source_db_host, source_db_port, source_db_name,
                   source_db_hash, status, created_at, last_updated
            FROM kg_metadata
            WHERE kg_id = %s
        """
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (str(kg_id),))
            result = cur.fetchone()
            return dict(result) if result else None
        
    def _load_tables(self, kg_id: UUID) -> List[Dict]:
        """Load all tables for a KG"""
        query = """
            SELECT table_id, kg_id, table_name, schema_name, qualified_name,
                   table_type, row_count_estimate, description, business_domain,
                   typical_use_cases
            FROM kg_tables
            WHERE kg_id = %s
            ORDER BY table_name
        """
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (str(kg_id),))
            results = cur.fetchall()
            
            # Parse JSON fields
            for row in results:
                if row['typical_use_cases']:
                    if isinstance(row['typical_use_cases'], str):
                        row['typical_use_cases'] = json.loads(row['typical_use_cases'])
            
            return [dict(row) for row in results]

    def _load_columns(self, kg_id: UUID) -> List[Dict]:
        """Load all columns for a KG"""
        query = """
            SELECT c.column_id, c.table_id, c.column_name, c.qualified_name,
                   c.data_type, c.is_nullable, c.is_primary_key, c.is_unique,
                   c.is_foreign_key, c.column_position, c.description,
                   c.business_meaning, c.sample_values, c.enum_values,
                   c.cardinality, c.null_percentage, c.is_pii
            FROM kg_columns c
            JOIN kg_tables t ON c.table_id = t.table_id
            WHERE t.kg_id = %s
            ORDER BY c.table_id, c.column_position
        """
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (str(kg_id),))
            results = cur.fetchall()
            
            # Parse JSON fields
            for row in results:
                if row['sample_values']:
                    if isinstance(row['sample_values'], str):
                        row['sample_values'] = json.loads(row['sample_values'])
                        
                if row['enum_values']:
                    if isinstance(row['enum_values'], str):
                        row['enum_values'] = json.loads(row['enum_values'])
            
            return [dict(row) for row in results]
    
    def _load_relationships(self, kg_id: UUID) -> List[Dict]:
        """Load all relationships for a KG"""
        query = """
            SELECT r.relationship_id, r.kg_id, r.from_table_id, r.to_table_id,
                   r.from_column, r.to_column, r.relationship_type, r.constraint_name,
                   r.join_condition, r.business_meaning, r.is_self_reference,
                   t1.table_name as from_table_name,
                   t2.table_name as to_table_name
            FROM kg_relationships r
            JOIN kg_tables t1 ON r.from_table_id = t1.table_id
            JOIN kg_tables t2 ON r.to_table_id = t2.table_id
            WHERE r.kg_id = %s
        """
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (str(kg_id),))
            results = cur.fetchall()
            return [dict(row) for row in results]
        