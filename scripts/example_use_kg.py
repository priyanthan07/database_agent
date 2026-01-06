import sys
import os
import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.settings import Settings
from src.kg.manager.kg_manager import KGManager
from src.kg.generators.openai_client import OpenAIClient

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
    import hashlib
    hash_str = f"{settings.ECOMMERCE_HOST}:{settings.ECOMMERCE_PORT}:{settings.ECOMMERCE_DATABASE}"
    source_db_hash = hashlib.sha256(hash_str.encode()).hexdigest()
    
    # Load KG
    print("Loading Knowledge Graph...")
    kg = manager.get_kg_by_source(source_db_hash)
    
    if not kg:
        print("KG not found! Run build_kg.py first.")
        return
    
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