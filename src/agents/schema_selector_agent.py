import logging
import time
from typing import Dict, Any

from .base_agent import BaseAgent
from .tools.vector_search_tool import VectorSearchTool
from .tools.llm_filter_tool import LLMFilterTool
from .tools.graph_traversal_tool import GraphTraversalTool
from ..orchestration.agent_state import AgentState

logger = logging.getLogger(__name__)


class SchemaSelectorAgent(BaseAgent):
    """
        Agent 1: Query Understanding & Table Retrieval
    """
    
    def __init__(self, kg_manager, openai_client, source_db_conn):
        super().__init__(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_db_conn,
            agent_name="Schema Selector Agent"
        )
        
        # Initialize tools
        self.vector_search = VectorSearchTool(kg_manager, openai_client)
        self.llm_filter = LLMFilterTool(openai_client)
        self.graph_traversal = GraphTraversalTool()
        
    def process(self, state: AgentState) -> AgentState:
        """
            Main processing logic for schema selection
        """
        
        self.log_start(state)
        start_time = time.time()
        
        try:
            # Load Knowledge Graph
            kg = self.get_kg(state.kg_id)
            
            # Use refined query if available, otherwise use original
            query = state.refined_query if state.refined_query else state.user_query
            
            # Generate embedding for query
            query_embedding = self.openai_client.generate_embeddings([query])[0]
            
            # Step 1: Vector search for candidate tables
            self.logger.info("Step 1: Performing vector search for tables")
            vector_results = self.vector_search.search_tables(
                kg_id=str(state.kg_id),
                query_embedding=query_embedding,
                k=10
            )
            
            state.vector_search_results = vector_results
            state.candidate_tables = [r["table_name"] for r in vector_results]
            
            if not state.candidate_tables:
                raise ValueError("No candidate tables found in vector search")
            
            # Step 2: LLM filtering to select best tables
            self.logger.info("Step 2: LLM filtering to select relevant tables")
            
            # Prepare KG context for LLM
            kg_context = self._prepare_kg_context(kg, state.candidate_tables)
            
            llm_result = self.llm_filter.filter_tables(
                user_query=query,
                candidate_tables=vector_results,
                kg_context=kg_context,
                max_tables=5
            )
            
            state.selected_tables = llm_result["selected_tables"]
            state.confidence_score = llm_result["confidence"]
            
            if not state.selected_tables:
                raise ValueError("LLM did not select any tables")
            
            self.logger.info(f"Selected tables: {state.selected_tables}")
            
            # Step 3: Graph traversal to find bridging tables
            self.logger.info("Step 3: Finding bridging tables via graph traversal")
            bridging_tables = self.graph_traversal.find_bridging_tables(
                kg=kg,
                selected_tables=state.selected_tables
            )
            state.bridging_tables = bridging_tables
            
            # Combine selected and bridging tables
            state.final_tables = state.selected_tables + bridging_tables
            
            self.logger.info(f"Final tables (including bridging): {state.final_tables}")
            
            # Step 4: Load full KG context for all final tables
            self.logger.info("Step 4: Loading full KG context for selected tables")
            state.table_contexts = self._load_full_table_contexts(kg, state.final_tables)
            
            # Validate that all tables are connected
            is_connected = self.graph_traversal.validate_connections(kg, state.final_tables)
            if not is_connected:
                self.logger.warning("Warning: Not all tables are connected via relationships")
            
            # Record timing
            state.schema_retrieval_time_ms = int((time.time() - start_time) * 1000)
            
            # Set next agent
            state.route_to_agent = "agent_2"
            
            self.log_end(state, success=True)
            return state
            
            
        except Exception as e:
            self.logger.error(f"Schema selection failed: {e}", exc_info=True)
            self.record_error(state, "schema_selection_error", str(e))
            state.route_to_agent = "complete"
            self.log_end(state, success=False)
            return state
        
    def _prepare_kg_context(self, kg, table_names: list) -> Dict[str, Any]:
        """Prepare KG context summary for LLM"""
        context = {}
        
        for table_name in table_names:
            table = kg.get_table(table_name)
            if table:
                context[table_name] = {
                    "description": table.description,
                    "business_domain": table.business_domain,
                    "column_count": len(table.columns),
                    "row_count": table.row_count_estimate
                }
        
        return context
    
    def _load_full_table_contexts(self, kg, table_names: list) -> Dict[str, Dict]:
        """Load complete table context including all columns and relationships"""
        contexts = {}
        
        for table_name in table_names:
            table = kg.get_table(table_name)
            if not table:
                self.logger.warning(f"Table not found in KG: {table_name}")
                continue
            
            # Get all columns with metadata
            columns = {}
            for col_name, col in table.columns.items():
                columns[col_name] = {
                    "qualified_name": col.qualified_name,
                    "data_type": col.data_type,
                    "is_nullable": col.is_nullable,
                    "is_primary_key": col.is_primary_key,
                    "is_foreign_key": col.is_foreign_key,
                    "description": col.description,
                    "business_meaning": col.business_meaning,
                    "sample_values": col.sample_values,
                    "enum_values": col.enum_values,
                    "is_pii": col.is_pii
                }
            
            # Get relationships
            relationships = kg.get_relationships_for_table(table_name)
            rel_info = []
            for rel in relationships:
                rel_info.append({
                    "from_table": rel.from_table_name,
                    "from_column": rel.from_column,
                    "to_table": rel.to_table_name,
                    "to_column": rel.to_column,
                    "type": rel.relationship_type,
                    "join_condition": rel.join_condition
                })
            
            contexts[table_name] = {
                "table_name": table.table_name,
                "qualified_name": table.qualified_name,
                "description": table.description,
                "business_domain": table.business_domain,
                "typical_use_cases": table.typical_use_cases,
                "row_count": table.row_count_estimate,
                "columns": columns,
                "relationships": rel_info
            }
        
        return contexts