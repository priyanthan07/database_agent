import sys
import os
import psycopg2
import hashlib
import json 
from datetime import datetime
from uuid import UUID

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.kg.models import KnowledgeGraph
from config.settings import Settings
from src.kg.manager.kg_manager import KGManager
from src.openai_client import OpenAIClient

def kg_to_json(kg: KnowledgeGraph, output_path: str = "kg_export.json"):
    """Export KG to JSON file"""
    
    # Helper to convert UUID/datetime to string
    def serialize(obj):
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")
    
    # Build JSON structure
    kg_dict = {
        "kg_id": str(kg.kg_id),
        "source_db": {
            "host": kg.source_db_host,
            "port": kg.source_db_port,
            "name": kg.source_db_name,
            "hash": kg.source_db_hash
        },
        "status": kg.status,
        "created_at": kg.created_at.isoformat(),
        "last_updated": kg.last_updated.isoformat(),
        "tables": {},
        "relationships": []
    }
    
    # Add tables with columns
    for table_name, table in kg.tables.items():
        kg_dict["tables"][table_name] = {
            "table_id": str(table.table_id),
            "schema_name": table.schema_name,
            "qualified_name": table.qualified_name,
            "table_type": table.table_type,
            "row_count_estimate": table.row_count_estimate,
            "description": table.description,
            "business_domain": table.business_domain,
            "typical_use_cases": table.typical_use_cases,
            "columns": {}
        }
        
        # Add columns
        for col_name, col in table.columns.items():
            kg_dict["tables"][table_name]["columns"][col_name] = {
                "column_id": str(col.column_id),
                "qualified_name": col.qualified_name,
                "data_type": col.data_type,
                "is_nullable": col.is_nullable,
                "is_primary_key": col.is_primary_key,
                "is_unique": col.is_unique,
                "is_foreign_key": col.is_foreign_key,
                "column_position": col.column_position,
                "description": col.description,
                "business_meaning": col.business_meaning,
                "sample_values": col.sample_values,
                "enum_values": col.enum_values,
                "cardinality": col.cardinality,
                "null_percentage": col.null_percentage,
                "is_pii": col.is_pii
            }
    
    # Add relationships
    for rel in kg.relationships:
        kg_dict["relationships"].append({
            "relationship_id": str(rel.relationship_id),
            "from_table": rel.from_table_name,
            "from_column": rel.from_column,
            "to_table": rel.to_table_name,
            "to_column": rel.to_column,
            "relationship_type": rel.relationship_type,
            "constraint_name": rel.constraint_name,
            "join_condition": rel.join_condition,
            "is_self_reference": rel.is_self_reference
        })
    
    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(kg_dict, f, indent=2, ensure_ascii=False)
    
    print(f"✅ KG exported to: {output_path}")
    print(f"   Tables: {len(kg_dict['tables'])}")
    print(f"   Relationships: {len(kg_dict['relationships'])}")

def main():
    # Load settings
    settings = Settings()
    
    # Connect to KG storage
    kg_conn = psycopg2.connect(
        host=settings.KG_HOST,
        port=settings.KG_PORT,
        database=settings.KG_DATABASE,
        user=settings.KG_USER,
        password=settings.KG_PASSWORD
    )
    
    # Initialize manager
    manager = KGManager(kg_conn, settings.CHROMA_PERSIST_DIR)
    
    # Create source DB hash
    hash_str = f"{settings.ECOMMERCE_HOST}:{settings.ECOMMERCE_PORT}:{settings.ECOMMERCE_DATABASE}"
    source_db_hash = hashlib.sha256(hash_str.encode()).hexdigest()
    
    # Load KG
    print("Loading Knowledge Graph...")
    kg = manager.get_kg_by_source(source_db_hash)
    
    if not kg:
        print("KG not found! Run build_kg.py first.")
        return
    
    # if kg:
    #     kg_to_json(kg, "kg_export.json")
    
    print(f"\nLoaded KG: {kg.kg_id}")
    print(f"Status: {kg.status}")
    print(f"Tables: {len(kg.tables)}")
    print(f"Relationships: {len(kg.relationships)}")
    
    # Example 1: Access table information
    print("\n" + "=" * 60)
    print("EXAMPLE 1: Accessing Table Information")
    print("=" * 60)
    
    customers_table = kg.get_table("customers")
    if customers_table:
        print(f"\nTable: {customers_table.table_name}")
        print(f"Description: {customers_table.description}")
        print(f"Business Domain: {customers_table.business_domain}")
        print(f"Columns: {len(customers_table.columns)}")
        print("\nFirst 5 columns:")
        for col_name, col in list(customers_table.columns.items())[:5]:
            print(f"  - {col_name} ({col.data_type}): {col.description}")
    
    # Example 2: Semantic search with embeddings
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Semantic Search with Embeddings")
    print("=" * 60)
    
    openai_client = OpenAIClient(settings.OPENAI_API_KEY, enable_langfuse=False)
    collection = manager.get_vector_collection(kg.kg_id)
    
    # Search for tables related to "customer purchases"
    query = "customer purchases"
    print(f"\nSearching for tables related to: '{query}'")
    
    query_embedding = openai_client.generate_embeddings([query])[0]
    results = manager.vector_store.search_tables(collection, query_embedding, n_results=3)
    
    print(f"\nTop {len(results)} results:")
    for i, result in enumerate(results, 1):
        print(f"\n{i}. {result['table_name']}")
        print(f"   Domain: {result['business_domain']}")
        print(f"   Distance: {result['distance']:.4f}")
    
    # Example 3: Get relationships
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Exploring Relationships")
    print("=" * 60)
    
    orders_relationships = kg.get_relationships_for_table("orders")
    print(f"\nRelationships for 'orders' table: {len(orders_relationships)}")
    for rel in orders_relationships:
        print(f"\n  {rel.from_table_name}.{rel.from_column} -> {rel.to_table_name}.{rel.to_column}")
        print(f"  Type: {rel.relationship_type}")
        print(f"  Join: {rel.join_condition}")
    
    kg_conn.close()
    print("\n" + "=" * 60)
    print("Examples complete!")
    print("=" * 60)


if __name__ == '__main__':
    main()