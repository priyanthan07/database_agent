import logging
from typing import Dict, Any
from langgraph.graph import StateGraph, END
from langfuse import observe
from langfuse import Langfuse

from .agent_state import AgentState
from ..agents.schema_selector_agent import SchemaSelectorAgent
from ..agents.sql_generator_agent import SQLGeneratorAgent
from ..agents.executor_validator_agent import ExecutorValidatorAgent
from config.settings import Settings


logger = logging.getLogger(__name__)


class AgentWorkflow:
    """
        LangGraph workflow orchestrating the 3-agent system.
    """
    def __init__(
        self,
        kg_manager,
        openai_client,
        source_db_conn,
        memory_repository,
        error_summary_manager=None
    ):
        self.kg_manager = kg_manager
        self.openai_client = openai_client
        self.source_db_conn = source_db_conn
        self.memory_repository = memory_repository
        self.error_summary_manager = error_summary_manager
        
        self.setting = Settings()
        
        self.langfuse = Langfuse(
            public_key=self.setting.LANGFUSE_PUBLIC_KEY,
            secret_key=self.setting.LANGFUSE_SECRET_KEY,
            host=self.setting.LANGFUSE_HOST
        )
        
        # Initialize agents
        self.agent_1 = SchemaSelectorAgent(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_db_conn
        )
        
        self.agent_2 = SQLGeneratorAgent(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_db_conn,
            memory_repository=memory_repository
        )
        
        self.agent_3 = ExecutorValidatorAgent(
            kg_manager=kg_manager,
            openai_client=openai_client,
            source_db_conn=source_db_conn,
            memory_repository=memory_repository,
            error_summary_manager=error_summary_manager
        )
        
        # Build workflow graph
        self.graph = self._build_graph()
        
    def _build_graph(self) -> StateGraph:
        """Build LangGraph workflow"""
        
        # Create graph
        workflow = StateGraph(AgentState)
        
        # Add nodes (agents)
        workflow.add_node("agent_1", self._run_agent_1)
        workflow.add_node("agent_2", self._run_agent_2)
        workflow.add_node("agent_3", self._run_agent_3)
        
        # Add edges
        # Start always goes to Agent 1
        workflow.set_entry_point("agent_1")
        
        # Agent 1 → Agent 2 (always)
        workflow.add_edge("agent_1", "agent_2")
        
        # Agent 2 → Agent 3 (always)
        workflow.add_edge("agent_2", "agent_3")
        
        # Agent 3 → Decision (retry or complete)
        workflow.add_conditional_edges(
            "agent_3",
            self._routing_decision,
            {
                "agent_1": "agent_1",
                "agent_2": "agent_2",
                "complete": END
            }
        )
        
        return workflow.compile()
    
    @observe(name="workflow_node_agent_1", as_type="span")
    def _run_agent_1(self, state: AgentState) -> AgentState:
        """Execute Agent 1 (Schema Selector)"""
        logger.info("Executing Agent 1: Schema Selector")
        return self.agent_1.process(state)
    
    @observe(name="workflow_node_agent_2", as_type="span")
    def _run_agent_2(self, state: AgentState) -> AgentState:
        """Execute Agent 2 (SQL Generator)"""
        logger.info("Executing Agent 2: SQL Generator")
        return self.agent_2.process(state)
    
    @observe(name="workflow_node_agent_3", as_type="span")
    def _run_agent_3(self, state: AgentState) -> AgentState:
        """Execute Agent 3 (Executor & Validator)"""
        logger.info("Executing Agent 3: Executor & Validator")
        return self.agent_3.process(state)
    
    def _routing_decision(self, state: AgentState) -> str:
        """
            Determine next step after Agent 3.
        """
        if isinstance(state, dict):
            route = state.get("route_to_agent")
        else:
            route = state.route_to_agent
        
        logger.info(f"Routing decision: {route}")
        
        if route == "agent_1":
            logger.info("Routing back to Agent 1 for schema re-selection")
            return "agent_1"
        elif route == "agent_2":
            logger.info("Routing back to Agent 2 for SQL re-generation")
            return "agent_2"
        else:
            logger.info("Workflow complete")
            return "complete"
    
    @observe(
        name="langgraph_workflow_execute",
        as_type="span"
    )
    def execute(self, initial_state: AgentState) -> AgentState:
        """
            Execute the complete workflow.
        """
        
        self.langfuse.update_current_span(
            input={
                "user_query": initial_state.user_query,
                "kg_id": str(initial_state.kg_id),
                "has_clarifications": bool(initial_state.clarifications_provided)
            }
        )
        
        logger.info("STARTING AGENT WORKFLOW")
        logger.info(f"User Query: {initial_state.user_query}")
        logger.info(f"KG ID: {initial_state.kg_id}")
        
        try:
            result = self.graph.invoke(initial_state)
            
            if isinstance(result, dict):
                logger.info("Converting dict result to AgentState")
                final_state = AgentState(**result)
            else:
                final_state = result
            
            logger.info("WORKFLOW COMPLETED")
            logger.info(f"Success: {final_state.execution_success}")
            logger.info(f"Iterations: {final_state.retry_count + 1}")
            
            if final_state.is_retry_success:
                logger.info("Note: Success was achieved after retry (lesson extracted)")
                
            logger.info(f"Total Time: {final_state.total_time_ms}ms")
            
            self.langfuse.update_current_span(
                output={
                    "execution_success": final_state.execution_success,
                    "retry_count": final_state.retry_count,
                    "final_route": final_state.route_to_agent
                },
                metadata={
                    "total_time_ms": final_state.total_time_ms,
                    "iterations": final_state.retry_count + 1,
                    "is_retry_success": final_state.is_retry_success
                }
            )
            
            return final_state
            
        except Exception as e:
            logger.error(f"Workflow execution failed: {e}", exc_info=True)
            initial_state.error_message = str(e)
            initial_state.execution_success = False
            return initial_state
        