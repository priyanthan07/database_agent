import logging
from typing import List, Dict
from uuid import UUID
from psycopg2.extras import RealDictCursor

from ..models import Relationship

logger = logging.getLogger(__name__)

class RelationshipExtractor:
    """
        Extracts foreign key relationships from PostgreSQL.
    """
    
    def __init__(self, connection):
        self.conn = connection
        
    def extract_relationships(self, kg_id: UUID, table_id_map: Dict[str, UUID], schema_name: str = "public") -> List[Relationship]:
        """Extract all foreign key relationships."""
        
        logger.info(f"Extracting relationships from schema '{schema_name}'")
        
        query = """
            SELECT
                tc.constraint_name,
                kcu.table_name AS from_table,
                kcu.column_name AS from_column,
                ccu.table_name AS to_table,
                ccu.column_name AS to_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu 
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = %s
            ORDER BY tc.constraint_name
        """
        
        relationships = []
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (schema_name,))
            rows = cur.fetchall()
            
            for row in rows:
                from_table = row['from_table']
                to_table = row['to_table']
                from_column = row['from_column']
                to_column = row['to_column']
                
                # Get table IDs
                from_table_id = table_id_map.get(from_table)
                to_table_id = table_id_map.get(to_table)
                
                if not from_table_id or not to_table_id:
                    continue
                
                # Determine relationship type
                relationship_type = self._determine_relationship_type(
                    from_table, from_column, to_table, to_column, schema_name
                )
                
                # Build join condition
                join_condition = f"{from_table}.{from_column} = {to_table}.{to_column}"
                
                # Check if self-reference
                is_self_reference = from_table == to_table
                
                relationship = Relationship(
                    kg_id=kg_id,
                    from_table_id=from_table_id,
                    to_table_id=to_table_id,
                    from_table_name=from_table,
                    to_table_name=to_table,
                    from_column=from_column,
                    to_column=to_column,
                    relationship_type=relationship_type,
                    constraint_name=row['constraint_name'],
                    join_condition=join_condition,
                    is_self_reference=is_self_reference
                )
                
                relationships.append(relationship)
                
        logger.info(f"Extracted {len(relationships)} relationships")
        return relationships
    
    def _determine_relationship_type(self, from_table: str, from_column: str, to_table: str, to_column: str, schema_name: str) -> str:
        """
            Determine relationship type from the FK table's perspective.
        
            A foreign key always creates a many-to-one or one-to-one relationship
            from the table that contains the FK to the referenced table.
        """
        
        is_unique_query = """
            SELECT COUNT(*) 
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = %s 
                AND tc.table_name = %s
                AND kcu.column_name = %s
                AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
        """
        
        with self.conn.cursor() as cur:
            cur.execute(is_unique_query, (schema_name, from_table, from_column))
            is_unique = cur.fetchone()[0] > 0
        
        if is_unique:
            return "one-to-one"
        else:
            return "many-to-one"
        