import sys
import os
import psycopg2
import hashlib
import logging
from uuid import UUID
from typing import Dict, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.settings import Settings
from src.openai_client import OpenAIClient
from src.kg.manager.kg_manager import KGManager
from src.api.agent_service import AgentService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress verbose logs from libraries
logging.getLogger('chromadb').setLevel(logging.WARNING)
logging.getLogger('openai').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)


class Colors:
    """ANSI color codes for terminal output"""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


def print_header(text: str):
    """Print formatted header"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text.center(80)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.END}\n")


def print_section(text: str):
    """Print formatted section"""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{text}{Colors.END}")
    print(f"{Colors.CYAN}{'-'*len(text)}{Colors.END}")


def print_success(text: str):
    """Print success message"""
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")


def print_error(text: str):
    """Print error message"""
    print(f"{Colors.RED}✗ {text}{Colors.END}")


def print_warning(text: str):
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.END}")


def print_info(text: str):
    """Print info message"""
    print(f"{Colors.CYAN}ℹ {text}{Colors.END}")


def format_sql(sql: str) -> str:
    """Format SQL for display"""
    return f"{Colors.BOLD}{sql}{Colors.END}"


def format_table(data: list, max_rows: int = 10) -> str:
    """Format query results as a table"""
    if not data:
        return "No results"
    
    # Get column names
    columns = list(data[0].keys())
    
    # Calculate column widths
    widths = {col: len(col) for col in columns}
    for row in data[:max_rows]:
        for col in columns:
            widths[col] = max(widths[col], len(str(row[col])))
    
    # Create header
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    separator = "-+-".join("-" * widths[col] for col in columns)
    
    # Create rows
    rows = []
    for row in data[:max_rows]:
        rows.append(" | ".join(str(row[col]).ljust(widths[col]) for col in columns))
    
    result = f"\n{header}\n{separator}\n" + "\n".join(rows)
    
    if len(data) > max_rows:
        result += f"\n\n... and {len(data) - max_rows} more rows"
    
    return result


def handle_clarification(clarification_request: Dict[str, Any]) -> str:
    """Handle clarification request interactively"""
    print_section("🤔 Clarification Required")
    
    print(f"\n{Colors.BOLD}Question:{Colors.END} {clarification_request['question']}")
    print(f"\n{Colors.BOLD}Detected Ambiguity:{Colors.END} {clarification_request['ambiguity']}")
    print(f"\n{Colors.BOLD}Reasoning:{Colors.END} {clarification_request['reasoning']}")
    
    print(f"\n{Colors.BOLD}Please select an option:{Colors.END}")
    options = clarification_request['options']
    
    for i, option in enumerate(options, 1):
        print(f"  {Colors.CYAN}{i}.{Colors.END} {option}")
    
    # Get user selection
    while True:
        try:
            choice = input(f"\n{Colors.BOLD}Enter option number (1-{len(options)}): {Colors.END}")
            choice_idx = int(choice) - 1
            
            if 0 <= choice_idx < len(options):
                selected = options[choice_idx]
                print_success(f"Selected: {selected}")
                return selected
            else:
                print_error(f"Invalid choice. Please enter a number between 1 and {len(options)}")
        except ValueError:
            print_error("Invalid input. Please enter a number.")
        except KeyboardInterrupt:
            print("\n")
            print_warning("Clarification cancelled")
            sys.exit(0)


def display_result(response: Dict[str, Any]):
    """Display query result"""
    
    if response.get("success"):
        print_section("✓ Query Executed Successfully")
        
        # Display SQL
        print(f"\n{Colors.BOLD}Generated SQL:{Colors.END}")
        print(format_sql(response['sql']))
        
        # Display explanation
        if response.get('explanation'):
            print(f"\n{Colors.BOLD}Explanation:{Colors.END}")
            print(response['explanation'])
        
        # Display results
        print_section("📊 Query Results")
        data = response.get('data', [])
        
        if isinstance(data, list) and len(data) > 0:
            print(format_table(data))
            print(f"\n{Colors.BOLD}Total rows:{Colors.END} {len(data)}")
        else:
            print("No data returned")
        
        # Display metadata
        metadata = response.get('metadata', {})
        print_section("ℹ Metadata")
        
        print(f"{Colors.BOLD}Tables Used:{Colors.END} {', '.join(metadata.get('tables_used', []))}")
        print(f"{Colors.BOLD}Confidence Score:{Colors.END} {metadata.get('confidence_score', 'N/A')}")
        print(f"{Colors.BOLD}Iterations:{Colors.END} {metadata.get('iterations', 1)}")
        
        timing = metadata.get('timing', {})
        print(f"\n{Colors.BOLD}Performance:{Colors.END}")
        print(f"  Schema Retrieval: {timing.get('schema_retrieval_ms', 0)}ms")
        print(f"  SQL Generation:   {timing.get('sql_generation_ms', 0)}ms")
        print(f"  Execution:        {timing.get('execution_ms', 0)}ms")
        print(f"  {Colors.BOLD}Total:            {timing.get('total_ms', 0)}ms{Colors.END}")
        
    else:
        print_section("✗ Query Failed")
        
        error = response.get('error', 'Unknown error')
        error_category = response.get('error_category', 'Unknown')
        
        print_error(f"Error: {error}")
        print(f"{Colors.BOLD}Category:{Colors.END} {error_category}")
        
        # Display attempted SQL if available
        sql_attempted = response.get('sql_attempted')
        if sql_attempted:
            print(f"\n{Colors.BOLD}Attempted SQL:{Colors.END}")
            print(format_sql(sql_attempted))
        
        # Display metadata
        metadata = response.get('metadata', {})
        if metadata.get('error_history'):
            print_section("Error History")
            for i, error_record in enumerate(metadata['error_history'], 1):
                print(f"{i}. {error_record.get('agent', 'Unknown')}: {error_record.get('error_category', 'Unknown')}")


def run_interactive_mode(agent_service: AgentService, kg_id: UUID):
    """Run interactive query mode"""
    
    print_header("🤖 Text-to-SQL Agent System - Interactive Mode")
    
    print(f"{Colors.BOLD}KG ID:{Colors.END} {kg_id}")
    print(f"{Colors.BOLD}Mode:{Colors.END} Interactive")
    print(f"\nType your natural language queries. Type 'exit' or 'quit' to stop.\n")
    
    query_count = 0
    
    while True:
        try:
            # Get user query
            user_query = input(f"{Colors.BOLD}{Colors.GREEN}Query> {Colors.END}").strip()
            
            if not user_query:
                continue
            
            if user_query.lower() in ['exit', 'quit', 'q']:
                print_success("Goodbye!")
                break
            
            if user_query.lower() in ['help', 'h']:
                print_section("Help")
                print("Enter natural language queries like:")
                print("  • Show me all customers")
                print("  • What are the top 5 products by revenue?")
                print("  • Find orders from last month")
                print("  • List customers with more than 10 orders")
                print("\nCommands:")
                print("  • exit/quit/q - Exit the program")
                print("  • help/h - Show this help message")
                continue
            
            query_count += 1
            print(f"\n{Colors.CYAN}[Query #{query_count}]{Colors.END}")
            
            # Process query (initial attempt)
            print_info("Processing query...")
            response = agent_service.query(
                user_query=user_query,
                kg_id=kg_id
            )
            
            # Handle clarification if needed
            clarifications = {}
            while response.get('needs_clarification'):
                clarification_request = response['clarification_request']
                selected_answer = handle_clarification(clarification_request)
                
                # Store clarification
                question = clarification_request['question']
                clarifications[question] = selected_answer
                
                # Re-run query with clarifications
                print_info("Re-processing query with clarifications...")
                response = agent_service.query(
                    user_query=user_query,
                    kg_id=kg_id,
                    clarifications=clarifications
                )
            
            # Display final result
            display_result(response)
            print()
            
        except KeyboardInterrupt:
            print("\n")
            print_warning("Interrupted. Type 'exit' to quit.")
            continue
        except Exception as e:
            print_error(f"Unexpected error: {e}")
            logger.error(f"Error in interactive mode: {e}", exc_info=True)


def run_single_query(agent_service: AgentService, kg_id: UUID, query: str):
    """Run a single query"""
    
    print_header("🤖 Text-to-SQL Agent System - Single Query")
    
    print(f"{Colors.BOLD}KG ID:{Colors.END} {kg_id}")
    print(f"{Colors.BOLD}Query:{Colors.END} {query}\n")
    
    # Process query
    print_info("Processing query...")
    response = agent_service.query(
        user_query=query,
        kg_id=kg_id
    )
    
    # Handle clarification if needed
    clarifications = {}
    while response.get('needs_clarification'):
        clarification_request = response['clarification_request']
        selected_answer = handle_clarification(clarification_request)
        
        # Store clarification
        question = clarification_request['question']
        clarifications[question] = selected_answer
        
        # Re-run query with clarifications
        print_info("Re-processing query with clarifications...")
        response = agent_service.query(
            user_query=query,
            kg_id=kg_id,
            clarifications=clarifications
        )
    
    # Display result
    display_result(response)


def main():
    """Main entry point"""
    
    # Load settings
    logger.info("Loading settings...")
    settings = Settings()
    
    # Validate OpenAI API key
    if not settings.OPENAI_API_KEY:
        print_error("OPENAI_API_KEY not found in environment!")
        print_info("Please set OPENAI_API_KEY in your .env file")
        sys.exit(1)
    
    try:
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
        
        # Initialize KG Manager
        logger.info("Initializing KG Manager...")
        kg_manager = KGManager(kg_conn, settings.CHROMA_PERSIST_DIR)
        
        # Get KG by source database hash
        hash_str = f"{settings.ECOMMERCE_HOST}:{settings.ECOMMERCE_PORT}:{settings.ECOMMERCE_DATABASE}"
        source_db_hash = hashlib.sha256(hash_str.encode()).hexdigest()
        
        logger.info("Loading Knowledge Graph...")
        kg = kg_manager.get_kg_by_source(source_db_hash)
        
        if not kg:
            print_error("Knowledge Graph not found!")
            print_info("Please run 'python scripts/build_kg.py' first to build the KG")
            sys.exit(1)
        
        print_success(f"Knowledge Graph loaded: {kg.kg_id}")
        print_info(f"Tables: {len(kg.tables)}, Relationships: {len(kg.relationships)}")
        
        # Initialize Agent Service
        logger.info("Initializing Agent Service...")
        agent_service = AgentService(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_conn,
            kg_conn=kg_conn
        )
        
        # Check for command-line arguments
        if len(sys.argv) > 1:
            # Single query mode
            query = " ".join(sys.argv[1:])
            run_single_query(agent_service, kg.kg_id, query)
        else:
            # Interactive mode
            run_interactive_mode(agent_service, kg.kg_id)
        
    except psycopg2.Error as e:
        print_error(f"Database connection error: {e}")
        logger.error(f"Database error: {e}", exc_info=True)
        sys.exit(1)
    
    except Exception as e:
        print_error(f"Error: {e}")
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)
    
    finally:
        # Close connections
        try:
            if 'source_conn' in locals():
                source_conn.close()
            if 'kg_conn' in locals():
                kg_conn.close()
            logger.info("Connections closed")
        except:
            pass


if __name__ == '__main__':
    main()