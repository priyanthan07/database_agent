import sys
import os
import argparse
import logging
import psycopg2

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.settings import Settings
from src.kg.generators.openai_client import OpenAIClient
from src.kg.builders.kg_builder import KGBuilder

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Build Knowledge Graph')
    parser.add_argument('--no-descriptions', action='store_true', help='Skip AI description generation')
    parser.add_argument('--no-embeddings', action='store_true', help='Skip embedding generation')
    args = parser.parse_args()
    
    # Load settings
    logger.info("Loading settings...")
    settings = Settings()
    
    # Validate OpenAI API key
    if not settings.OPENAI_API_KEY:
        logger.error("OPENAI_API_KEY not found in environment!")
        sys.exit(1)
    
    # Connect to source database
    logger.info(f"Connecting to source database: {settings.ECOMMERCE_DATABASE}")
    source_conn = psycopg2.connect(
        host=settings.ECOMMERCE_HOST,
        port=settings.ECOMMERCE_PORT,
        database=settings.ECOMMERCE_DATABASE,
        user=settings.ECOMMERCE_USER,
        password=settings.ECOMMERCE_PASSWORD
    )
    
    # Connect to KG storage database
    logger.info(f"Connecting to KG storage database: {settings.KG_DATABASE}")
    kg_conn = psycopg2.connect(
        host=settings.KG_HOST,
        port=settings.KG_PORT,
        database=settings.KG_DATABASE,
        user=settings.KG_USER,
        password=settings.KG_PASSWORD
    )
    
    # Initialize OpenAI client
    logger.info("Initializing OpenAI client...")
    openai_client = OpenAIClient(
        api_key=settings.OPENAI_API_KEY,
        enable_langfuse=settings.enable_langfuse
    )
    
    if settings.enable_langfuse:
        logger.info("Langfuse monitoring: ENABLED")
    else:
        logger.info("Langfuse monitoring: DISABLED")
    
    # Build KG
    logger.info("Starting KG build process...")
    builder = KGBuilder(
        source_conn=source_conn,
        kg_conn=kg_conn,
        openai_client=openai_client,
        settings=settings
    )
    
    try:
        kg = builder.build_kg(
            source_db_name=settings.ECOMMERCE_DATABASE,
            source_db_host=settings.ECOMMERCE_HOST,
            source_db_port=settings.ECOMMERCE_PORT,
            schema_name="public",
            generate_descriptions=not args.no_descriptions,
            generate_embeddings=not args.no_embeddings
        )
        
        if kg:
            logger.info("=" * 60)
            logger.info("SUCCESS! Knowledge Graph built successfully")
            logger.info("=" * 60)
            logger.info(f"KG ID: {kg.kg_id}")
            logger.info(f"Tables: {len(kg.tables)}")
            logger.info(f"Relationships: {len(kg.relationships)}")
        
    except Exception as e:
        logger.error(f"KG build failed: {e}", exc_info=True)
        sys.exit(1)
    
    finally:
        source_conn.close()
        kg_conn.close()
        logger.info("Connections closed")


if __name__ == '__main__':
    main()